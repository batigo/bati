use futures::channel::mpsc::{self, Sender};
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use ntex::{rt, util::Bytes};
use std::collections::HashMap;

use crate::conn_proto::*;
use crate::encoding::*;
use crate::hub_proto::*;
use crate::metric;
use crate::pilot_proto::*;
use crate::utils::*;
use bati_lib as lib;
use bati_lib::service_msg::*;
use bati_lib::{get_now_milli, Postman, PostmanMsg, ServiceConf, ServiceData, ServiceMsg};

#[derive(Clone)]
struct ServicePostman {
    conf: ServiceConf,
    msg_sender: Sender<lib::PostmanMsg>,
}

pub struct Pilot {
    ix: usize,
    hubs: Vec<Option<HubSender>>,
    postmen: HashMap<String, ServicePostman>,
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
            PilotMessage::FromConn(msg) => {
                debug!("recv PilotMessage::FromConn msg: {:?}", msg);
                let PilotServiceBizMsg {
                    service: channel,
                    data,
                } = msg;
                self.send_postman_msg(
                    channel,
                    lib::PostmanBatiMsg {
                        data,
                        service: None,
                    },
                )
                .await;
            }

            PilotMessage::FromHub(msg) => {
                debug!("recv PilotMessage::FromHub: {:?}", msg);
                match msg {
                    Hub2PilotMsg::BizMsg(msg) => {
                        let PilotServiceBizMsg {
                            service: channel,
                            data,
                        } = msg;
                        self.send_postman_msg(
                            channel,
                            lib::PostmanBatiMsg {
                                data,
                                service: None,
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

            PilotMessage::FromServiceFinder(conf) => {
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
                            .send_pilot_msg(Pilot2HubMsg::ServiceConf(conf.clone()))
                            .await
                            .unwrap_or_else(|e| {
                                error!("failed to send channel conf to hub: {}", e);
                            });
                    }
                }

                let channel = conf.name.clone();
                self.postmen.insert(
                    conf.name.clone(),
                    ServicePostman {
                        conf,
                        msg_sender: sender2,
                    },
                );

                let mut sender = self.msg_sender.clone();
                rt::spawn(async move {
                    loop {
                        if let Some(PostmanMsg::Downer(msg)) = receiver1.next().await {
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

            PilotMessage::FromPostman(mut msg) => {
                debug!("recv service msg in pilot: {} - {}", msg.service, msg.id);

                if msg.ts > 0 {
                    metric::update_service_msg_latency(&msg.service, get_now_milli() - msg.ts);
                }

                match msg.typ {
                    SERVICE_MSG_TYPE_CONN_JOIN => self.handle_join_service_msg(&mut msg).await,
                    SERVICE_MSG_TYPE_CONN_QUIT => self.handle_leave_room_msg(&mut msg).await,
                    SERVICE_MSG_TYPE_BIZ
                    | SERVICE_MSG_TYPE_BROADCAST
                    | SERVICE_MSG_TYPE_SERVICE
                    | SERVICE_MSG_TYPE_ROOM_USERS => self.handle_biz_msg(&mut msg).await,
                    _ => {
                        warn!("bad service msg type: {}", msg.typ);
                    }
                }
            }
            PilotMessage::FromTester(mut msg) => {
                let mut data = PilotQueryData {
                    hubs: self.hubs.clone(),
                    postmen: HashMap::new(),
                    encoders: self.encoders.clone(),
                };
                for (id, d) in self.postmen.iter() {
                    data.postmen.insert(id.to_string(), d.conf.clone());
                }
                msg.sender.try_send(data).unwrap();
            }
        }
    }

    fn get_specified_hub_by_cid(&self, cid: &str) -> Option<HubSender> {
        if let Some(ix) = get_worker_index_from_cid(cid) {
            if let Some(hub) = self.hubs.get(ix) {
                if hub.is_some() {
                    return Some(hub.as_ref().unwrap().clone());
                }
            }
        }

        None
    }

    async fn send_hub_msgs(&self, cid: Option<String>, msg: Pilot2HubMsg) {
        match cid {
            Some(ref cid) => {
                if let Some(mut hub) = self.get_specified_hub_by_cid(cid) {
                    debug!("send single-hub HubPilotMsg, sid: {}", cid);
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

    async fn handle_join_service_msg(&self, msg: &mut ServiceMsg2) {
        debug!("handle join service msg: {:?}", msg);
        if msg.join_data.is_none() {
            return;
        }

        let mut join_data = msg.join_data.take().unwrap();
        if join_data.cid.is_none() && join_data.uid.is_none() {
            return;
        }

        let service = &msg.service;
        let cid = join_data.cid.take();
        let uid = join_data.uid.take();
        let rids = join_data.rids.take();

        let cp = self.postmen.get(service);
        if cp.is_none() {
            return;
        }

        let cp = cp.unwrap();
        if !cp.conf.enable_multi_rooms && rids.is_some() && rids.as_ref().unwrap().len() > 1 {
            return;
        }

        self.send_hub_msgs(
            cid.clone(),
            Pilot2HubMsg::JoinService(HubJoinServiceMsg {
                cid,
                uid,
                service: service.clone(),
                multi_rooms: cp.conf.enable_multi_rooms,
                rooms: service_data.rids,
                join_service: join_data.join_service,
            }),
        )
        .await;
    }

    async fn handle_leave_room_msg(&mut self, msg: &mut ServiceMsg) {
        debug!("handle leave room msg: {:?}", msg);
        if msg.service.is_none() || msg.room.is_none() || (msg.uid.is_none() && msg.cid.is_none()) {
            return;
        }

        let sid = msg.cid.take();
        self.send_hub_msgs(
            sid.clone(),
            Pilot2HubMsg::LeaveRoom(HubLeaveRoomMsg {
                cid: sid,
                uid: msg.uid.take(),
                service: msg.service.take().unwrap(),
                room: msg.room.take().unwrap(),
            }),
        )
        .await;
    }

    async fn handle_biz_msg(&mut self, msg: &mut ServiceMsg) {
        debug!("handle channel biz msg: {:?}", msg);
        if msg.data.is_none() {
            warn!("no biz data for msg: {}", msg.id);
            return;
        };

        let mut biz_msg = HubServiceBizMsg::default();
        let mut cmsg = ClientMsg {
            id: msg.id.clone(),
            typ: ClientMsgType(CMSG_TYPE_BIZ),
            ack: 0,
            service_id: msg.service.take(),
            data: msg.data.take(),
        };
        let data = serde_json::to_vec(&cmsg);
        if data.is_err() {
            error!("failed to gen Session2ClientMsg: {}", data.err().unwrap());
            return;
        }

        biz_msg.data = ServiceBizData::new(Bytes::from(data.unwrap()));
        self.encoders.iter().for_each(|e| {
            biz_msg
                .data
                .insert_data_with_encoder(e)
                .unwrap_or_else(|err| {
                    error!("failed to insert data with encoder: {} - {}", e.name(), err);
                });
        });

        let sid = msg.cid.clone();
        match msg.typ {
            SERVICE_MSG_TYPE_BIZ => {
                biz_msg.typ = ServiceBizMsgType::Conn;
                biz_msg.cid = msg.cid.take();
                biz_msg.service = cmsg.service_id.take();
                if biz_msg.cid.is_none() {
                    warn!("conn not found for conn-bizmsg: {}", msg.id);
                    return;
                }
            }
            SERVICE_MSG_TYPE_SERVICE => {
                biz_msg.typ = ServiceBizMsgType::Service;
                biz_msg.service = cmsg.service_id.take();
                if biz_msg.service.is_none() {
                    return;
                }
                biz_msg.room = msg.room.take();
                biz_msg.ratio = self.get_broadcast_ratio(&msg);
                biz_msg.blacks = msg.exclude_uids.take();
                biz_msg.whites = msg.include_uids.take();
            }
            SERVICE_MSG_TYPE_BROADCAST => {
                biz_msg.typ = ServiceBizMsgType::Broadcast;
                biz_msg.ratio = self.get_broadcast_ratio(&msg);
                biz_msg.blacks = msg.exclude_uids.take();
                biz_msg.whites = msg.include_uids.take();
            }
            SERVICE_MSG_TYPE_ROOM_USERS => {
                biz_msg.typ = ServiceBizMsgType::Room;
                biz_msg.service = cmsg.service_id.take();
                biz_msg.room = msg.room.take();
                biz_msg.uids = msg.uids.take();
                if biz_msg.service.is_none()
                    || biz_msg.room.is_none()
                    || biz_msg.uids.is_none()
                    || biz_msg.uids.as_ref().unwrap().is_empty()
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

    fn get_broadcast_ratio(&self, msg: &ServiceMsg) -> Option<u8> {
        match msg.broadcast_rate {
            Some(v) if (0..100).contains(&v) => Some(v as u8),
            Some(v) if v < 0 => Some(0),
            _ => None,
        }
    }

    async fn send_postman_msg(&mut self, channel: String, msg: lib::PostmanBatiMsg) {
        debug!("recv PilotHubMsg: {} - {:?}", channel, msg);
        if let Some(postman) = self.postmen.get_mut(&channel) {
            let mut sender = postman.msg_sender.clone();
            sender
                .send(PostmanMsg::Upper(msg))
                .await
                .unwrap_or_else(|e| {
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
