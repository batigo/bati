use crate::conn_proto::*;
use crate::const_proto::*;
use crate::encoding::*;
use crate::hub_proto::*;
use crate::pilot_proto::*;
use crate::utils::*;
use crate::{cmsg, metric};
use bati_lib as lib;
use log::{debug, error, info, warn};
use ntex::ws::{Frame as WsFrame, Message as WsMessage, WsSink};
use ntex::{rt, util::Bytes};
use std::fmt;
use std::time::{Duration, Instant};
use zstd::zstd_safe::WriteBuf;
use bati_lib::serialize_bati_msg;

const HEARTBEAT_INTERVAL_SEC: u32 = 60;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(180);

pub struct Conn {
    id: String,
    uid: String,
    did: String,
    ip: String,
    dt: DeviceType,
    hb: Instant,
    encoder: Option<Encoder>,
    hub: HubSender,
    join_hub: bool,
    pilot: PilotSender,
    msg_sender: ConnSender,
    msg_receiver: ConnReceiver,
    ws_sink: Option<WsSink>,
}

impl Conn {
    pub fn new(
        uid: String,
        did: String,
        ip: String,
        dt: u8,
        hub: HubSender,
        pilot: PilotSender,
        worker_index: usize,
    ) -> Conn {
        let id = gen_conn_id(&did, &uid, worker_index);
        let (msg_sender, msg_receiver) = new_conn_channel(&id, 32);

        Conn {
            id,
            uid,
            ip,
            hub,
            pilot,
            msg_receiver,
            msg_sender,
            did: did.clone(),
            dt: trans_dt(dt),
            encoder: None,
            hb: Instant::now(),
            join_hub: false,
            ws_sink: None,
        }
    }

    pub fn start(self, ws_sink: WsSink) -> ConnSender {
        info!("conn[{}] starting... :", self.id);
        use crate::timer::start_conn_hearbeat_cron;
        let conn_sender = self.msg_sender.clone();
        rt::spawn(async move {
            let mut conn = self;
            conn.ws_sink = Some(ws_sink);
            loop {
                let msg = conn.msg_receiver.next().await;
                if msg.is_none() {
                    conn.quit().await;
                    return;
                }
                if !conn.handle_message(msg.unwrap()).await {
                    conn.quit().await;
                    return;
                }
            }
        });

        start_conn_hearbeat_cron(conn_sender.clone());
        conn_sender
    }

    async fn handle_message(&mut self, msg: ConnMsg) -> bool {
        match msg {
            ConnMsg::FromMaster(msg) => {
                self.hb = Instant::now();
                match msg {
                    Master2ConnMsg::Shutdown => false,
                    Master2ConnMsg::Frame(frame) => match frame {
                        WsFrame::Ping(msg) => {
                            debug!("recv ping from conn: {}", self.id);
                            self.send_client_msg(WsMessage::Pong(msg)).await
                        }
                        WsFrame::Pong(bs) => {
                            debug!("recv pong from conn: {}", self.id);
                            self.send_client_msg(WsMessage::Pong(bs)).await
                        }
                        WsFrame::Text(bs) => {
                            info!("recv text msg from conn: {}", self.id,);
                            self.proc_client_msg(bs.as_slice()).await
                        }
                        WsFrame::Binary(bs) => {
                            info!("recv bin msg from conn: {}", self.id,);
                            self.proc_client_msg(bs.as_slice()).await
                        }
                        WsFrame::Close(reason) => {
                            warn!(
                                "recv close msg from conn: {}, reason: {}",
                                self.id,
                                match reason {
                                    Some(r) => r.code.into(),
                                    None => 0_u16,
                                }
                            );
                            false
                        }
                        WsFrame::Continuation(_data) => {
                            // TODO：
                            true
                        }
                    },
                }
            }
            ConnMsg::FromHub(msg) => {
                debug!("conn recv SessionHubMsg: {} - {:?}", self.id, msg);
                return match msg {
                    Hub2ConnMsg::QUIT => {
                        warn!("recv quit msg from hub: {}", self.id);
                        false
                    }
                    Hub2ConnMsg::BIZ(bs) => {
                        debug!(
                            "send biz msg to conn: {}, msg: {}",
                            self.id,
                            String::from_utf8(bs.to_vec()).unwrap_or_else(|_| "".to_string())
                        );
                        self.send_client_msg(WsMessage::Binary(bs)).await
                    }
                };
            }
            ConnMsg::FromTimer(msg) => match msg {
                Timer2ConnMsg::HearBeatCheck => {
                    if Instant::now().duration_since(self.hb) > CLIENT_TIMEOUT {
                        warn!("conn timeout: {}", self.id);
                        return false;
                    }
                    if !self.send_client_msg(WsMessage::Ping(Bytes::new())).await {
                        return false;
                    }
                    true
                }
            },
        }
    }

    async fn send_client_msg(&self, msg: WsMessage) -> bool {
        let mut ok = true;
        if let Some(ws) = self.ws_sink.as_ref() {
            ws.send(msg).await.unwrap_or_else(|e| {
                error!("failed to send msg to client: {} - {:?}", self.id, e);
                ok = false;
            });
        }
        ok
    }

    async fn proc_client_msg(&mut self, bs: &[u8]) -> bool {
        let cmsg = cmsg::deserialize_cmsg(bs);
        if let Err(e) = cmsg {
            error!("recv bad msg from conn: {}, err: {}", self.id, e);
            return false;
        }

        let cmsg = cmsg.unwrap();
        let id = &cmsg.id;
        if let Err(e) = cmsg.validate() {
            warn!(
                "msg validate failed: {}, msg-id: {}, err: {}",
                self.id, cmsg.id, e
            );
            return false;
        }

        let cmsg_type = cmsg::ClientMsgType::from_must(cmsg.r#type);
        metric::inc_recv_msg(cmsg_type.str(), 1);

        if cmsg_type.is_type(cmsg::ClientMsgType::Ack) {
            return true;
        }

        if cmsg.ack == 1 {
            let rmsg = cmsg::ClientMsg::new_ack_msg(&cmsg.id);
            let r = self.send_cmsg(&rmsg).await;
            if r.is_err() {
                return false;
            }
        }

        match cmsg_type {
            cmsg::ClientMsgType::Echo => {
                self.send_cmsg(&cmsg)
                    .await
                    .unwrap_or_else(|e| error!("failed to send cmsg: {}", e));
            }
            cmsg::ClientMsgType::Init => self.proc_init_cmsg(cmsg).await,
            cmsg::ClientMsgType::Biz => self.proc_biz_msg(cmsg).await,
            _ => {}
        }

        true
    }

    async fn proc_init_cmsg(&mut self, mut msg: cmsg::ClientMsg) {
        let init_data = msg.init_data.take().unwrap();
        let init_data = self.gen_init_resp_data(init_data);
        self.encoder = Some(cmsg::CompressorType::new_compressor(
            init_data.accept_compressor,
        ));
        let rmsg = cmsg::ClientMsg {
            id: lib::gen_msg_id(),
            r#type: cmsg::ClientMsgType::InitResp as i32,
            ack: 1,
            init_data: Some(init_data),
            ..Default::default()
        };
        match self.send_cmsg(&rmsg).await {
            Err(e) => {
                error!("failed to send cmsg: {} - {}", self.id, e);
                self.quit().await;
            }
            Ok(_) => {
                info!("conn inited, cid: {}, encoder: {}", self.id, self.encoder.as_ref().unwrap());
                if !self.join_hub {
                    self.join_hub().await;
                }
            }
        }
    }

    async fn proc_biz_msg(&mut self, mut msg: cmsg::ClientMsg) {
        let service_id = msg.service_id.take().unwrap();

        debug!("proc biz msg: {}", msg.id);

        let biz_data = msg.take_biz_data();
        if let Err(e) = biz_data {
            error!("recv bad biz data: {} - {}", self.id, e);
            return;
        }

        let mut biz_data = biz_data.unwrap();
        if let Some(encoder) = msg.compressor {
            if encoder != cmsg::CompressorType::Null as i32 {
                let encoder = cmsg::CompressorType::new_compressor(encoder);
                let r = encoder.decode(biz_data.as_ref());
                if r.is_err() {
                    error!("failed to uncompress bizdata: {} - {}", msg.id, r.err().unwrap());
                    return
                }
                biz_data = r.unwrap();
            }
        }

        let bati_msg = lib::BatiMsg::new(
            Some(msg.id.clone()),
            lib::BATI_MSG_TYPE_BIZ,
            self.id.clone(),
            self.uid.clone(),
            Some(self.ip.clone()),
            Some(biz_data),
        );

        let bs = serialize_bati_msg(&bati_msg);
        self.pilot
            .send_conn_msg(PilotServiceBizMsg {
                service: service_id,
                data: Bytes::from(bs),
            })
            .await
            .unwrap_or_else(|e| {
                error!("failed to send pilot msg: {}", e);
            });
    }

    async fn send_cmsg(&self, msg: &cmsg::ClientMsg) -> Result<(), Box<dyn std::error::Error>> {
        debug!(
            "send msg to conn: {}, msg, id: {}, type: {}",
            self.id, msg.id, msg.r#type
        );

        let bs = cmsg::serialize_cmsg(msg);
        let bs = Bytes::from(bs);
        self.send_client_msg(WsMessage::Binary(bs)).await;
        Ok(())

        // let encoder = match msg.typ.0 {
        //     CMSG_TYPE_INIT_RESP => None,
        //     _ => self.encoder.clone(),
        // };
        //
        // match msg.gen_bytes_with_encoder(encoder) {
        //     Err(e) => Err(e),
        //     Ok(v) => {
        //         self.send_client_msg(WsMessage::Binary(v)).await;
        //         Ok(())
        //     }
        // }
    }

    async fn quit(&mut self) {
        info!(
            "conn stopping, sid: {}, mid: {}, did: {}, dt: {}, ip: {}",
            self.id, self.uid, self.did, self.dt, self.ip
        );
        if self.join_hub {
            self.hub
                .send_conn_msg(Conn2HubMsg::Unregister(ConnUnregMsg {
                    cid: self.id.clone(),
                    uid: self.uid.clone(),
                    did: self.did.clone(),
                    dt: self.dt,
                    ip: Some(self.ip.clone()),
                }))
                .await
                .unwrap_or_else(|e| {
                    warn!("failed to send HubSessionUnregMsg: {} - {}", self.id, e);
                });
        }
        if let Some(ws) = self.ws_sink.take() {
            ws.io().close();
        }
    }

    fn gen_init_resp_data(&self, data: cmsg::InitData) -> cmsg::InitData {
        cmsg::InitData {
            ping_interval: HEARTBEAT_INTERVAL_SEC,
            accept_compressor: data.accept_compressor,
        }

        // if data.accept_encoding.ne("") {
        //     let es: Vec<_> = data.accept_encoding.split(',').collect();
        //     let mut gzip_enable = false;
        //     let mut deflate_enable = false;
        //     for s in es.into_iter() {
        //         match s {
        //             ZSTD_NAME => gzip_enable = true,
        //             DEFLATE_NAME => deflate_enable = true,
        //             _ => {}
        //         }
        //     }
        //     if deflate_enable {
        //         resp_msg.accept_encoding = DEFLATE_NAME.to_string();
        //     } else if gzip_enable {
        //         resp_msg.accept_encoding = ZSTD_NAME.to_string();
        //     }
        // }

        // if data.conn_id.is_some() {
        //     resp_msg.conn_id = Some(data.conn_id.as_ref().unwrap().to_string());
        // } else if data.connid.is_some() {
        //     resp_msg.conn_id = Some(data.connid.as_ref().unwrap().to_string());
        // }

        // Ok(resp_msg)
    }

    async fn join_hub(&mut self) {
        let msg = Conn2HubMsg::Register(ConnRegMsg {
            cid: self.id.clone(),
            uid: self.uid.clone(),
            dt: self.dt,
            encoder: self.encoder.clone().unwrap(),
            addr: self.msg_sender.clone(),
        });
        match self.hub.send_conn_msg(msg).await {
            Ok(_) => {
                self.join_hub = true;
                debug!("success to send HubSessionRegMsg: {}", self.id);
            }
            Err(e) => {
                error!("failed to send HubSessionRegMsg: {} - {}", self.id, e);
            }
        }
    }
}

impl fmt::Display for Conn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "conn - id:{}, uid:{}, did:{}, ip:{}, encoder: {:?}",
            self.id, self.uid, self.did, self.ip, self.encoder,
        )
    }
}
