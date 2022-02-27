use futures::channel::mpsc::{self, Sender};
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use ntex::{rt, util::Bytes};
use std::collections::HashMap;

use crate::encoding::*;
use crate::hub_proto::*;
use crate::metric;
use crate::pilot_proto::*;
use crate::session_proto::*;
use crate::utils::*;
use bati_lib as lib;
use bati_lib::chanmsg::*;
use bati_lib::{get_now_milli, ChannelConf, ChannelData, ChannelMsg, Postman};

#[derive(Clone)]
struct ChannelPostman {
    conf: ChannelConf,
    msg_sender: Sender<lib::PostmanMsg>,
}

pub struct Pilot {
    ix: usize,
    hubs: Vec<Option<HubSender>>,
    postmen: HashMap<String, ChannelPostman>,
    encoders: Vec<Encoder>,
    msg_sender: PilotSender,
    msg_receiver: PilotReceiver,
}

impl Pilot {
    pub fn new(ix: usize, hubs_num: usize) -> Self {
        let mut hubs: Vec<Option<HubSender>> = vec![];
        let (msg_sender, msg_receiver) = new_pilot_channel(1024);
        for _ in 0..hubs_num {
            hubs.push(None);
        }
        Pilot {
            ix,
            hubs,
            msg_sender,
            msg_receiver,
            postmen: HashMap::with_capacity(32),
            encoders: vec![],
        }
    }

    pub fn start(self) -> PilotSender {
        let msg_sender = self.msg_sender.clone();
        let mut pilot = self;
        rt::Arbiter::new().exec_fn(move || {
            rt::spawn(async move {
                loop {
                    let msg = pilot.msg_receiver.next().await;
                    if msg.is_none() {
                        rt::Arbiter::current().stop();
                        panic!("pilot-{} failed to recv msg", pilot.ix,);
                    }
                    pilot.handle_message(msg.unwrap()).await;
                }
            });
        });

        msg_sender
    }

    async fn handle_message(&mut self, msg: PilotMessage) {
        match msg {
            PilotMessage::FromSession(msg) => {
                debug!("recv PilotSessionMsg msg: {:?}", msg);
                let Session2PilotMsg { channel, data } = msg;
                self.send_postman_msg(
                    channel,
                    lib::PostmanMsg {
                        data,
                        channel: None,
                    },
                )
                .await;
            }

            PilotMessage::FromHub(msg) => {
                debug!("recv PilotHubMsg: {:?}", msg);
                match msg {
                    Hub2PilotMsg::ChannelMsg(msg) => {
                        let PilotChannelMsg { channel, data } = msg;
                        self.send_postman_msg(
                            channel,
                            lib::PostmanMsg {
                                data,
                                channel: None,
                            },
                        )
                        .await;
                    }
                    Hub2PilotMsg::EncodingMsg(encoding) => {
                        let encoder = Encoder::new(encoding);
                        if !self.encoders.contains(&encoder) {
                            self.encoders.push(encoder);
                            debug!("============ pilot encoders: {:?}", self.encoders);
                        }
                    }
                }
            }

            PilotMessage::FromMaster(msg) => {
                info!("pilot-{} add hub-{}", self.ix, msg.ix);
                self.hubs[msg.ix] = Some(msg.hub);
            }

            PilotMessage::FromChanFinder(conf) => {
                if self.postmen.get(&conf.name).is_some() {
                    return;
                }

                let (sender1, mut receiver1) = mpsc::channel(1024);
                let (sender2, receiver2) = mpsc::channel(1024);

                let postman = Postman::new_upper(conf.clone(), sender1, receiver2);
                if let Err(e) = postman {
                    error!("failed to start postman, {} - {}", conf.name, e);
                    return;
                }

                let mut postman = postman.unwrap();
                if let Err(e) = postman.run() {
                    error!(
                        "pilot-{} failed to run postman: {} - {}",
                        self.ix, conf.name, e
                    );
                    return;
                }

                warn!(
                    "pilot-{} start postman for with channel conf: {}",
                    self.ix, conf
                );

                for hub in self.hubs.iter() {
                    if hub.is_some() {
                        hub.as_ref()
                            .unwrap()
                            .clone()
                            .send_pilot_msg(Pilot2HubMsg::ChannelConf(conf.clone()))
                            .await
                            .unwrap_or_else(|e| {
                                error!("failed to send channel conf to hub: {}", e);
                            });
                    }
                }

                let channel = conf.name.clone();
                self.postmen.insert(
                    conf.name.clone(),
                    ChannelPostman {
                        conf,
                        msg_sender: sender2,
                    },
                );

                let mut sender = self.msg_sender.clone();
                rt::spawn(async move {
                    loop {
                        if let Some(msg) = receiver1.next().await {
                            sender.send_postman_msg(msg).await.unwrap_or_else(|e| {
                                error!("failed to send pilot msg to hub: {}", e)
                            });
                        } else {
                            error!("pilot-{} recv pipe broken: ", channel);
                            break;
                        }
                    }
                });
            }

            PilotMessage::FromPostman(mut postman_msg) => {
                let s = std::str::from_utf8(postman_msg.data.as_ref());
                if s.is_err() {
                    error!("bad postman msg recved: {}", s.err().unwrap());
                    return;
                }

                let msg: serde_json::Result<ChannelMsg> = serde_json::from_str(s.unwrap());
                if msg.is_err() {
                    error!("bad postman msg recved: {}", msg.err().unwrap());
                    return;
                }

                let mut msg = msg.unwrap();
                if msg.channel.is_none() {
                    msg.channel = postman_msg.channel.take();
                }
                debug!(
                    "recv channel msg in pilot: {} - {}",
                    msg.channel.as_ref().unwrap_or(&"x".to_string()),
                    msg.id
                );

                // update channel msg latency metric
                if msg.ts > 0 {
                    let channel = match msg.channel.as_ref() {
                        Some(s) => s.as_str(),
                        _ => "x",
                    };
                    metric::update_channel_msg_latency(channel, get_now_milli() - msg.ts);
                }

                match msg.typ {
                    CHAN_MSG_TYPE_REG_CHANNEL => self.handle_join_channel_msg(&mut msg).await,
                    CHAN_MSG_TYPE_UNREG_ROOM | CHAN_MSG_TYPE_UNREG_CHANNEL => {
                        self.handle_leave_room_msg(&mut msg).await
                    }
                    CHAN_MSG_TYPE_SESSION
                    | CHAN_MSG_TYPE_BROADCAST
                    | CHAN_MSG_TYPE_CHANNEL
                    | CHAN_MSG_TYPE_ROOM_USERS => self.handle_biz_msg(&mut msg).await,
                    _ => {
                        warn!("bad channel msg type: {}", msg.typ);
                    }
                }
            }
        }
    }

    fn get_specified_hub_by_session_id(&self, sid: &str) -> Option<HubSender> {
        if let Some(ix) = get_worker_index_from_session_id(sid) {
            if let Some(hub) = self.hubs.get(ix) {
                if hub.is_some() {
                    return Some(hub.as_ref().unwrap().clone());
                }
            }
        }

        None
    }

    async fn send_hub_msgs(&self, sid: Option<String>, msg: Pilot2HubMsg) {
        match sid {
            Some(ref sid) => {
                if let Some(mut hub) = self.get_specified_hub_by_session_id(sid) {
                    debug!("send single-hub HubPilotMsg, sid: {}", sid);
                    hub.send_pilot_msg(msg).await.unwrap_or_else(|e| {
                        error!("failed to send HubPilotMsg: {}", e);
                    });
                }
            }
            _ => {
                self.broadcast_hub_msg(msg).await;
            }
        }
    }

    async fn broadcast_hub_msg(&self, msg: Pilot2HubMsg) {
        debug!("send broadcast-hub HubPilotMsg");

        for hub in self.hubs.iter() {
            if hub.is_some() {
                hub.as_ref()
                    .unwrap()
                    .clone()
                    .send_pilot_msg(msg.clone())
                    .await
                    .unwrap_or_else(|e| {
                        error!("failed to send HubPilotMsg: {}", e);
                    });
            }
        }
    }

    async fn handle_join_channel_msg(&self, msg: &mut ChannelMsg) {
        debug!("handle join channel msg: {:?}", msg);
        if msg.channel.is_none() || msg.sid.is_none() || msg.data.is_none() {
            return;
        }
        let cid = msg.channel.take().unwrap();
        let sid = msg.sid.take().unwrap();

        let channel = self.postmen.get(&cid);
        if channel.is_none() {
            return;
        }
        let channel = channel.unwrap();

        let channel_data: serde_json::Result<ChannelData> =
            serde_json::from_str(msg.data.take().unwrap().get());
        if channel_data.is_err() {
            return;
        }
        let channel_data = channel_data.unwrap();

        if !channel.conf.enable_multi_rooms && channel_data.rids.len() > 1 {
            return;
        }

        self.send_hub_msgs(
            Some(sid.clone()),
            Pilot2HubMsg::JoinChannel(HubJoinChannelMsg {
                sid,
                channel: cid.clone(),
                multi_rooms: channel.conf.enable_multi_rooms,
                rooms: channel_data.rids,
            }),
        )
        .await;
    }

    async fn handle_leave_room_msg(&mut self, msg: &mut ChannelMsg) {
        debug!("handle leave room msg: {:?}", msg);
        if msg.channel.is_none() || msg.room.is_none() || (msg.uid.is_none() && msg.sid.is_none()) {
            return;
        }

        let sid = msg.sid.take();
        self.send_hub_msgs(
            sid.clone(),
            Pilot2HubMsg::LeaveRoom(HubLeaveRoomMsg {
                sid,
                uid: msg.uid.take(),
                channel: msg.channel.take().unwrap(),
                room: msg.room.take().unwrap(),
            }),
        )
        .await;
    }

    async fn handle_biz_msg(&mut self, msg: &mut ChannelMsg) {
        debug!("handle channel biz msg: {:?}", msg);
        if msg.data.is_none() {
            warn!("no biz data for msg: {}", msg.id);
            return;
        };

        let mut biz_msg = HubChannelBizMsg::default();
        let mut cmsg = Session2ClientMsg {
            id: msg.id.clone(),
            typ: CMsgType(CMSG_TYPE_BIZ),
            ack: 0,
            channel_id: msg.channel.take(),
            data: msg.data.take(),
        };
        let data = serde_json::to_vec(&cmsg);
        if data.is_err() {
            error!("failed to gen Session2ClientMsg: {}", data.err().unwrap());
            return;
        }

        biz_msg.data = ChannelBizData::new(Bytes::from(data.unwrap()));
        self.encoders.iter().for_each(|e| {
            biz_msg
                .data
                .insert_data_with_encoder(e)
                .unwrap_or_else(|err| {
                    error!("failed to insert data with encoder: {} - {}", e.name(), err);
                });
        });

        let sid = msg.sid.clone();
        match msg.typ {
            CHAN_MSG_TYPE_SESSION => {
                biz_msg.typ = ChannelBizMsgType::Session;
                biz_msg.sid = msg.sid.take();
                biz_msg.channel = cmsg.channel_id.take();
                if biz_msg.sid.is_none() {
                    warn!("session not found for session-bizmsg: {}", msg.id);
                    return;
                }
            }
            CHAN_MSG_TYPE_CHANNEL => {
                biz_msg.typ = ChannelBizMsgType::Channel;
                biz_msg.channel = cmsg.channel_id.take();
                if biz_msg.channel.is_none() {
                    return;
                }
                biz_msg.room = msg.room.take();
                biz_msg.ratio = self.get_broadcast_ratio(&msg);
                biz_msg.blacks = msg.exclude_mids.take();
                biz_msg.whites = msg.include_mids.take();
            }
            CHAN_MSG_TYPE_BROADCAST => {
                biz_msg.typ = ChannelBizMsgType::Broadcast;
                biz_msg.ratio = self.get_broadcast_ratio(&msg);
                biz_msg.blacks = msg.exclude_mids.take();
                biz_msg.whites = msg.include_mids.take();
            }
            CHAN_MSG_TYPE_ROOM_USERS => {
                biz_msg.typ = ChannelBizMsgType::Room;
                biz_msg.channel = cmsg.channel_id.take();
                biz_msg.room = msg.room.take();
                biz_msg.mids = msg.uids.take();
                if biz_msg.channel.is_none()
                    || biz_msg.room.is_none()
                    || biz_msg.mids.is_none()
                    || biz_msg.mids.as_ref().unwrap().is_empty()
                {
                    return;
                }
            }
            _ => {
                return;
            }
        }
        biz_msg.id = msg.id.clone();
        self.send_hub_msgs(sid, Pilot2HubMsg::Biz(biz_msg)).await;
    }

    fn get_broadcast_ratio(&self, msg: &ChannelMsg) -> Option<u8> {
        match msg.broadcast_rate {
            Some(v) if (0..100).contains(&v) => Some(v as u8),
            Some(v) if v < 0 => Some(0),
            _ => None,
        }
    }

    async fn send_postman_msg(&mut self, channel: String, msg: lib::PostmanMsg) {
        debug!("recv PilotHubMsg: {} - {:?}", channel, msg);
        if let Some(postman) = self.postmen.get_mut(&channel) {
            let mut sender = postman.msg_sender.clone();
            sender.send(msg).await.unwrap_or_else(|e| {
                error!(
                    "failed to send msg to postman in channel: {}, err: {}",
                    channel, e
                );
            });
        } else {
            warn!("channel postman not found: {}", channel);
        }
    }
}
