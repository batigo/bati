use crate::cmsg;
use crate::encoding::*;
use crate::hub_proto::*;
use crate::metric;
use crate::pilot_proto::*;
use crate::utils::*;
use bati_lib as lib;
use bati_lib::smsg::*;
use bati_lib::{get_now_milli, Postman, PostmanMsg, ServiceConf, ServiceMsgType};
use futures::channel::mpsc::{self, Sender};
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use ntex::{rt, util::Bytes};
use std::collections::HashMap;

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

            PilotMessage::FromPostman(msg) => {
                warn!("recv service msg in pilot: {} - {}", msg.service, msg.id);

                if msg.ts > 0 {
                    metric::update_service_msg_latency(&msg.service, get_now_milli() - msg.ts);
                }

                match msg.get_type() {
                    ServiceMsgType::ConnJoin => self.handle_join_service_msg(msg).await,
                    ServiceMsgType::ConnQuit => self.handle_quit_service_msg(msg).await,
                    ServiceMsgType::Biz => self.handle_biz_msg(msg).await,
                    _ => {
                        warn!("bad service msg type: {}", msg.get_type() as i32);
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

    async fn handle_join_service_msg(&self, mut msg: ServiceMsg) {
        warn!("handle join service msg: {}", msg);
        if msg.join_data.is_none() {
            warn!("==== join data empty: {}", msg);
            return;
        }

        let mut join_data = msg.join_data.take().unwrap();
        if join_data.cid.is_none() && join_data.uid.is_none() {
            warn!("==== join data abnormal: {}", msg);
            return;
        }

        let service = &msg.service;
        let cid = join_data.cid.take();
        let uid = join_data.uid.take();
        let JoinData {
            rooms,
            join_service,
            ..
        } = join_data;

        let cp = self.postmen.get(service);
        if cp.is_none() {
            warn!("==== service postman not found for {}", service);
            return;
        }

        let cp = cp.unwrap();
        if !cp.conf.enable_multi_rooms && rooms.len() > 1 {
            warn!(
                "service-{} disable multirooms, recv join multi-rooms msg: {}",
                msg.service, msg.id
            );
            return;
        }

        let mut nmsg = HubJoinServiceMsg {
            uid,
            cid: cid.clone(),
            service: service.clone(),
            multi_rooms: cp.conf.enable_multi_rooms,
            rooms: None,
            join_service: join_service.unwrap_or(false),
        };
        if !rooms.is_empty() {
            nmsg.rooms = Some(rooms);
        }
        self.send_hub_msgs(cid, Pilot2HubMsg::JoinService(nmsg))
            .await;
    }

    async fn handle_quit_service_msg(&mut self, mut msg: ServiceMsg) {
        debug!("handle leave room msg: {}", msg);
        if msg.quit_data.is_none() {
            return;
        }

        let mut quit_data = msg.quit_data.take().unwrap();
        let cid = quit_data.cid.take();
        let uid = quit_data.uid.take();

        let QuitData {
            rooms,
            quit_service,
            ..
        } = quit_data;

        let mut nmsg = HubLeaveRoomMsg {
            cid: cid.clone(),
            uid,
            service: msg.service,
            rooms: None,
            quit_service: quit_service.unwrap_or(false),
        };
        if !rooms.is_empty() {
            nmsg.rooms = Some(rooms);
        }
        self.send_hub_msgs(cid, Pilot2HubMsg::LeaveRoom(nmsg)).await;
    }

    async fn handle_biz_msg(&mut self, mut msg: ServiceMsg) {
        debug!("handle channel biz msg: {}", msg);
        if msg.biz_data.is_none() || msg.biz_data.as_ref().unwrap().data.is_none() {
            warn!("no biz data for msg: {}", msg.id);
            return;
        };

        let mut biz_data = msg.biz_data.take().unwrap();
        let data = biz_data.data.take().unwrap();

        let typ = biz_data.get_type();
        let ServiceMsg { id, service, .. } = msg;
        let BizData {
            cids,
            uids,
            black_uids,
            white_uids,
            broadcast_ratio,
            ..
        } = biz_data;

        let service_biz_data = self.gen_service_biz_data(id.clone(), service.clone(), data);
        let mut biz_msg = HubServiceBizMsg {
            id,
            typ,
            ratio: broadcast_ratio,
            service: Some(service),
            room: biz_data.room.take(),
            data: service_biz_data,
            ..Default::default()
        };
        if !cids.is_empty() {
            biz_msg.cids = Some(cids);
        }
        if !uids.is_empty() {
            biz_msg.uids = Some(uids);
        }
        if !black_uids.is_empty() {
            biz_msg.blacks = Some(black_uids);
        }
        if !white_uids.is_empty() {
            biz_msg.whites = Some(white_uids);
        }

        if biz_msg.cids.is_some() && biz_msg.cids.as_ref().unwrap().len() == 1 {
            self.send_hub_msgs(
                biz_msg.cids.as_ref().unwrap().get(0).cloned(),
                Pilot2HubMsg::Biz(biz_msg),
            )
            .await
        } else {
            self.send_hub_msgs(None, Pilot2HubMsg::Biz(biz_msg)).await;
        }
    }

    fn gen_service_biz_data(&self, id: String, service: String, data: Vec<u8>) -> ServiceBizData {
        let mut need_comressor = false;
        for encode in self.encoders.iter() {
            if encode.name() == DEFLATE_NAME {
                need_comressor = true;
                break;
            }
        }
        need_comressor = need_comressor && data.len() > 200;

        if need_comressor {
            if let Ok(bs) = Encoder::new(DEFLATE_NAME).encode(&data) {
                let cmsg = cmsg::ClientMsg {
                    id: id.clone(),
                    r#type: cmsg::ClientMsgType::Biz as i32,
                    ack: 0,
                    service_id: Some(service.clone()),
                    compressor: None,
                    biz_data: Some(data),
                    init_data: None,
                };
                let rbs = cmsg::serialize_cmsg(&cmsg);

                let cmsg = cmsg::ClientMsg {
                    id: id.clone(),
                    r#type: cmsg::ClientMsgType::Biz as i32,
                    ack: 0,
                    service_id: Some(service),
                    compressor: Some(cmsg::CompressorType::Deflate as i32),
                    biz_data: Some(bs),
                    init_data: None,
                };
                let cbs = cmsg::serialize_cmsg(&cmsg);
                return ServiceBizData::new(Bytes::from(rbs), Some(Bytes::from(cbs)));
            }
        }

        let cmsg = cmsg::ClientMsg {
            id: id.clone(),
            r#type: cmsg::ClientMsgType::Biz as i32,
            ack: 0,
            service_id: Some(service.clone()),
            compressor: None,
            biz_data: Some(data),
            init_data: None,
        };
        let bs = cmsg::serialize_cmsg(&cmsg);
        return ServiceBizData::new(Bytes::from(bs), None);
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
