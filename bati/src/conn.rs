use crate::const_proto::*;
use crate::encoding::*;
use crate::hub_proto::*;
use crate::metric;
use crate::pilot_proto::*;
use crate::conn_proto::*;
use crate::utils::*;
use bati_lib as lib;
use log::{debug, error, info, warn};
use ntex::ws::{Frame as WsFrame, Message as WsMessage, WsSink};
use ntex::{rt, util::Bytes};
use serde_json::value::RawValue;
use std::fmt;
use std::time::{Duration, Instant};
use zstd::zstd_safe::WriteBuf;

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
                            debug!("recv ping from session: {}", self.id);
                            self.send_client_msg(WsMessage::Pong(msg)).await
                        }
                        WsFrame::Pong(bs) => {
                            debug!("recv pong from session: {}", self.id);
                            self.send_client_msg(WsMessage::Pong(bs)).await
                        }
                        WsFrame::Text(bs) => {
                            debug!("recv text msg from session: {}, msg: {:?}", self.id, bs);
                            self.proc_client_msg(bs.as_slice()).await
                        }
                        WsFrame::Binary(bs) => {
                            debug!("recv bin msg from session: {}, msg: {:?}", self.id, bs);
                            self.proc_client_msg(bs.as_slice()).await
                        }
                        WsFrame::Close(reason) => {
                            warn!(
                                "recv close msg from session: {}, reason: {}",
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
                debug!("session recv SessionHubMsg: {} - {:?}", self.id, msg);
                return match msg {
                    Hub2ConnMsg::QUIT => {
                        warn!("recv quit msg from hub: {}", self.id);
                        false
                    }
                    Hub2ConnMsg::BIZ(bs) => {
                        debug!(
                            "send biz msg to session: {}, msg: {}",
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
                        warn!("session timeout: {}", self.id);
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
        self.ws_sink
            .as_ref()
            .unwrap()
            .send(msg)
            .await
            .unwrap_or_else(|e| {
                error!("failed to send msg to client: {} - {:?}", self.id, e);
                ok = false;
            });
        ok
    }

    async fn proc_client_msg(&mut self, bs: &[u8]) -> bool {
        let msg: serde_json::Result<ClientMsg>;
        let mut unzdata: Option<Vec<u8>> = None;
        match self.encoder {
            Some(ref encoder) if encoder.name() != NULLENCODER_NAME => {
                let data = encoder.decode(&bs);
                if data.is_err() {
                    error!(
                        "recv bad msg from session: {}, err: {}, msg: {:?}",
                        self.id,
                        data.err().unwrap(),
                        bs,
                    );
                    return false;
                }
                unzdata = Some(data.unwrap());
            }
            _ => {}
        }

        if unzdata.is_some() {
            msg = serde_json::from_slice(unzdata.as_ref().unwrap());
        } else {
            msg = serde_json::from_slice(bs);
        }

        if msg.is_err() {
            error!(
                "recv bad msg from session: {}, err: {}",
                self.id,
                msg.err().unwrap()
            );
            return false;
        }

        let mut msg = msg.unwrap();
        let ok = msg.validate();
        if ok.is_err() {
            warn!(
                "msg validate failed: {}, msg-id: {}, err: {}",
                self.id,
                msg.id,
                ok.err().unwrap()
            );
            return false;
        }

        metric::inc_recv_msg(msg.typ.str(), 1);

        if msg.typ.is_type(CMSG_TYPE_ACK) {
            return true;
        }

        if msg.ack == 1 {
            let rmsg = ClientMsg::new_ack_msg(&msg.id);
            let r = self.send_cmsg(&rmsg).await;
            if r.is_err() {
                return false;
            }
        }

        match msg.typ.0 {
            CMSG_TYPE_ECHO => {
                self.send_cmsg(&msg)
                    .await
                    .unwrap_or_else(|e| error!("failed to send cmsg: {}", e));
            }
            CMSG_TYPE_INIT => self.proc_init_cmsg(&msg).await,
            CMSG_TYPE_BIZ => self.proc_biz_msg(&mut msg).await,
            _ => {}
        }

        true
    }

    async fn proc_init_cmsg(&mut self, msg: &ClientMsg) {
        if self.encoder.is_some() {
            warn!("recv init msg from inited session: {}", self.id);
            return;
        }

        let r: serde_json::Result<ConnInitMsgData> =
            serde_json::from_str(msg.data.as_ref().unwrap().get());
        if r.is_err() {
            warn!("recv init msg from inited session: {}", self.id);
            return self.quit().await;
        }

        let data = r.unwrap();
        let data = self.gen_init_resp_data(data);
        if data.is_err() {
            warn!(
                "failed to gen session init resp data: {}, err: {}",
                self.id,
                data.err().unwrap()
            );
            return self.quit().await;
        }

        let data = data.unwrap();

        self.encoder = Some(Encoder::new(&data.accept_encoding));

        let data = serde_json::to_string(&data).unwrap();
        let data = RawValue::from_string(data).unwrap();
        let rmsg = ClientMsg {
            id: lib::gen_msg_id(),
            typ: ClientMsgType(CMSG_TYPE_INIT_RESP),
            ack: 1,
            service_id: None,
            data: Some(data),
        };
        match self.send_cmsg(&rmsg).await {
            Err(e) => {
                error!("failed to send cmsg: {} - {}", self.id, e);
                self.quit().await;
            }
            Ok(_) => {
                if !self.join_hub {
                    self.join_hub().await;
                }
            }
        }
    }

    async fn proc_biz_msg(&mut self, msg: &mut ClientMsg) {
        debug!("proc biz msg: {}", msg.id);
        let mut channel_msg = lib::ServiceMsg::new();
        channel_msg.data = msg.data.take();
        channel_msg.cid = Some(self.id.clone());
        channel_msg.ip = Some(self.ip.clone());
        channel_msg.uid = Some(self.uid.clone());
        match serde_json::to_vec(&channel_msg) {
            Ok(bs) => {
                self.pilot
                    .send_conn_msg( PilotServiceBizMsg {
                        service: msg.service_id.take().unwrap(),
                        data: bytes::Bytes::from(bs),
                    })
                    .await
                    .unwrap_or_else(|e| {
                        error!("failed to send pilot msg: {}", e);
                    });
            }
            Err(e) => {
                error!("failed to gen channel msg: {}, err: {}", msg.id, e);
            }
        }
    }

    async fn send_cmsg(&self, msg: &ClientMsg) -> Result<(), Box<dyn std::error::Error>> {
        debug!(
            "send msg to session: {}, msg, id: {}, type: {}, data: {:?}",
            self.id, msg.id, msg.typ, msg.data
        );

        let encoder = match msg.typ.0 {
            CMSG_TYPE_INIT_RESP => None,
            _ => self.encoder.clone(),
        };
        match msg.gen_bytes_with_encoder(encoder) {
            Err(e) => Err(e),
            Ok(v) => {
                self.send_client_msg(WsMessage::Binary(v)).await;
                Ok(())
            }
        }
    }

    async fn quit(&mut self) {
        info!(
            "session stopping, sid: {}, mid: {}, did: {}, dt: {}, ip: {}",
            self.id, self.uid, self.did, self.dt, self.ip
        );
        if self.join_hub {
            self.hub
                .send_session_msg(Conn2HubMsg::Unregister(ConnUnregMsg {
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

    fn gen_init_resp_data(&self, data: ConnInitMsgData) -> Result<ConnInitMsgData, String> {
        let mut resp_msg = ConnInitMsgData {
            ping_interval: HEARTBEAT_INTERVAL_SEC,
            ..Default::default()
        };

        if data.accept_encoding.ne("") {
            let es: Vec<_> = data.accept_encoding.split(',').collect();
            let mut gzip_enable = false;
            let mut deflate_enable = false;
            for s in es.into_iter() {
                match s {
                    ZSTD_NAME => gzip_enable = true,
                    DEFLATE_NAME => deflate_enable = true,
                    _ => {}
                }
            }
            if deflate_enable {
                resp_msg.accept_encoding = DEFLATE_NAME.to_string();
            } else if gzip_enable {
                resp_msg.accept_encoding = ZSTD_NAME.to_string();
            }
        }

        // if data.session_id.is_some() {
        //     resp_msg.session_id = Some(data.session_id.as_ref().unwrap().to_string());
        // } else if data.sessionid.is_some() {
        //     resp_msg.session_id = Some(data.sessionid.as_ref().unwrap().to_string());
        // }

        Ok(resp_msg)
    }

    async fn join_hub(&mut self) {
        let msg = Conn2HubMsg::Register(ConnRegMsg {
            cid: self.id.clone(),
            uid: self.uid.clone(),
            dt: self.dt,
            encoder: self.encoder.clone().unwrap(),
            addr: self.msg_sender.clone(),
        });
        match self.hub.send_session_msg(msg).await {
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
            "session - id:{}, uid:{}, did:{}, ip:{}, encoder: {:?}",
            self.id, self.uid, self.did, self.ip, self.encoder,
        )
    }
}
