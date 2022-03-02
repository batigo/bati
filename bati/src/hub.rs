use crate::const_proto::*;
use crate::encoding::*;
use crate::hub_proto::*;
use crate::metric;
use crate::metric_proto::*;
use crate::pilot_proto::*;
use crate::conn_proto::*;
use bati_lib as lib;
use log::{debug, error, warn};
use ntex::rt;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub struct Hub {
    ix: usize,
    conns: HashMap<String, Rc<Conn>>,
    rooms: HashMap<String, HashMap<String, Rc<Conn>>>,
    conn_rooms: HashMap<String, HashSet<String>>,
    services: HashMap<String, HashMap<String, Rc<Conn>>>,
    conn_services: HashMap<String, HashSet<String>>,
    uid_conns: HashMap<String, HashSet<String>>,
    dt_conns: HashMap<DeviceType, u64>,
    rand: rand::rngs::ThreadRng,
    pilot: PilotSender,
    metric_collector: MetricSender,
    service_confs: HashMap<String, lib::ServiceConf>,
    encoders: Vec<Encoder>,
    msg_receiver: HubReceiver,
    msg_sender: HubSender,
}

#[derive(Clone)]
struct Conn {
    uid: String,
    encoder: Encoder,
    addr: ConnSender,
}

impl Conn {
    async fn send_conn_msg(&self, sid: &str, msg: Hub2ConnMsg) {
        self.addr.send_hub_msg(msg).await.unwrap_or_else(|e| {
            warn!("failed to send conn biz msg: {} - {}", sid, e);
        });
    }
}

impl std::fmt::Display for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "conn, id: {}, endcode: {}",
            self.addr.id, self.encoder
        )
    }
}

#[derive(PartialEq)]
struct ServiceRoom {
    service: String,
    room: String,
}

impl ServiceRoom {
    fn validate_room_id(room_id: &str) -> bool {
        let v: Vec<&str> = room_id.split("@_@").collect();
        v.len() == 2
    }

    fn gen_room_id(service: &str, room: &str) -> Result<String, &'static str> {
        let room_id = format!("{}@_@{}", service, room);
        if !Self::validate_room_id(&room_id) {
            return Err("bad service & room value");
        }

        Ok(room_id)
    }
}

impl Hub {
    pub fn new(ix: usize, pilot: PilotSender, metric_collector: MetricSender) -> Self {
        let rand = rand::thread_rng();
        let (sender, receiver) = new_hub_channel(1024);
        let mut hub = Hub {
            ix,
            rand,
            pilot,
            metric_collector,
            conns: HashMap::with_capacity(10240),
            rooms: HashMap::with_capacity(1024),
            conn_rooms: HashMap::with_capacity(10240),
            services: HashMap::with_capacity(32),
            conn_services: HashMap::with_capacity(10240),
            uid_conns: HashMap::with_capacity(10240),
            dt_conns: HashMap::with_capacity(4),
            service_confs: HashMap::with_capacity(32),
            encoders: vec![],
            msg_receiver: receiver,
            msg_sender: sender,
        };

        hub.dt_conns.insert(DEVICE_TYPE_ANDROID, 0);
        hub.dt_conns.insert(DEVICE_TYPE_IOS, 0);
        hub.dt_conns.insert(DEVICE_TYPE_WEB, 0);
        hub.dt_conns.insert(DEVICE_TYPE_X, 0);
        hub
    }

    pub fn start(self) -> HubSender {
        use crate::timer::start_hub_stat_cron;
        let mut hub = self;
        let hub_sender = hub.msg_sender.clone();
        rt::spawn(async move {
            loop {
                let msg = hub.msg_receiver.next().await;
                if msg.is_none() {
                    panic!("hub-{} failed to recv msg", hub.ix,);
                }
                hub.handle_message(msg.unwrap()).await;
            }
        });

        start_hub_stat_cron(hub_sender.clone());
        hub_sender
    }

    async fn handle_message(&mut self, msg: HubMessage) {
        match msg {
            HubMessage::FromConn(msg) => {
                debug!("recv Session2HubMsg in hub-{}: {}", self.ix, msg);
                match msg {
                    Conn2HubMsg::Register(msg) => self.handle_conn_register_msg(msg).await,
                    Conn2HubMsg::Unregister(msg) => {
                        self.handle_conn_unregister_msg(msg).await
                    }
                }
            }
            HubMessage::FromPilot(msg) => {
                debug!("recv Pilot2HubMsg in hub-{}: {:?}", self.ix, msg);
                match msg {
                    Pilot2HubMsg::Biz(msg) => match msg.typ {
                        ServiceBizMsgType::Conn => self.handle_conn_biz_msg(msg).await,
                        ServiceBizMsgType::Room => self.handle_room_biz_msg(msg).await,
                        ServiceBizMsgType::Service => self.handle_service_biz_msg(msg).await,
                        ServiceBizMsgType::Broadcast => self.broadcast_bizmsg(msg).await,
                    },
                    Pilot2HubMsg::JoinService(msg) => self.handle_join_service_msg(msg).await,
                    Pilot2HubMsg::LeaveRoom(msg) => self.handle_leave_room_msg(msg).await,
                    Pilot2HubMsg::ServiceConf(conf) => {
                        self.service_confs.insert(conf.name.clone(), conf);
                    }
                }
            }

            HubMessage::FromTimer(msg) => match msg {
                Timer2HubMsg::MetricStat => self.do_stats().await,
            },
            HubMessage::FromTester(mut msg) => {
                let mut data = HubDataQueryData{
                    conns: Default::default(),
                    rooms: Default::default(),
                    conn_rooms: self.conn_rooms.clone(),
                    services: Default::default(),
                    conn_services: self.conn_services.clone(),
                    uid_conns: self.uid_conns.clone(),
                    dt_conns: self.dt_conns.clone(),
                    service_confs: self.service_confs.clone(),
                    encoders: self.encoders.clone(),
                };

                for (id, conn) in self.conns.iter() {
                    data.conns.insert(id.to_string(), conn.addr.clone());
                }
                for (id, conn) in self.rooms.iter() {
                    let mut conns = HashMap::new();
                    for (id, conn) in conn.iter() {
                        conns.insert(id.to_string(), conn.addr.clone());
                    }
                    data.rooms.insert(id.to_string(), conns);
                }
                for (id, conn) in self.services.iter() {
                    let mut conns = HashMap::new();
                    for (id, conn) in conn.iter() {
                        conns.insert(id.to_string(), conn.addr.clone());
                    }
                    data.services.insert(id.to_string(), conns);
                }
                msg.sender.try_send(data).unwrap();
            }
        }
    }

    fn get_conn(&self, cid: &str) -> Option<&Rc<Conn>> {
        self.conns.get(cid)
    }

    fn get_conn_clone(&self, sid: &str) -> Option<Rc<Conn>> {
        self.conns.get(sid).cloned()
    }

    fn get_uid_sids_clone(&self, uid: &str) -> Option<HashSet<String>> {
        self.uid_conns.get(uid).cloned()
    }

    fn get_room(&self, room: &str) -> Option<&HashMap<String, Rc<Conn>>> {
        self.rooms.get(room)
    }

    fn get_service(&self, name: &str) -> Option<&HashMap<String, Rc<Conn>>> {
        self.services.get(name)
    }

    fn get_conn_rooms(&self, cid: &str) -> Option<&HashSet<String>> {
        self.conn_rooms.get(cid)
    }

    fn add_conn_room(&mut self, cid: &str, room: &str) {
        if let Some(rooms) = self.conn_rooms.get_mut(cid) {
            rooms.insert(room.to_string());
            return;
        }

        let mut s = HashSet::new();
        s.insert(room.to_string());
        self.conn_rooms.insert(cid.to_string(), s);
    }

    fn del_conn_room(&mut self, cid: &str, room: &str) {
        if let Some(rooms) = self.conn_rooms.get_mut(cid) {
            rooms.remove(room);
            if rooms.is_empty() {
                self.conn_rooms.remove(cid);
            }
        }
    }

    fn get_conn_services(&self, sid: &str) -> Option<&HashSet<String>> {
        self.conn_services.get(sid)
    }

    fn add_conn_service(&mut self, cid: &str, service: &str) {
        if let Some(services) = self.conn_services.get_mut(cid) {
            services.insert(service.to_string());
            return;
        }

        let mut s = HashSet::new();
        s.insert(service.to_string());
        self.conn_services.insert(cid.to_string(), s);
    }

    fn del_conn_service(&mut self, cid: &str, service: &str) {
        if let Some(services) = self.conn_services.get_mut(cid) {
            services.remove(service);
            if services.is_empty() {
                self.conn_services.remove(cid);
            }
        }
    }

    fn add_uid(&mut self, uid: &str, cid: &str) {
        self.uid_conns
            .entry(uid.to_string())
            .or_insert_with(HashSet::new)
            .insert(cid.to_string());
    }

    fn remove_uid(&mut self, uid: &str, cid: &str) -> bool {
        match self.uid_conns.get_mut(uid) {
            Some(sessions) => {
                if sessions.len() <= 1 {
                    self.uid_conns.remove(uid);
                } else {
                    sessions.remove(cid);
                }
                true
            }
            _ => false,
        }
    }

    fn incr_dt_count(&mut self, dt: DeviceType) {
        *self.dt_conns.get_mut(dt).unwrap() += 1;
    }

    fn decr_dt_count(&mut self, dt: DeviceType) {
        *self.dt_conns.get_mut(dt).unwrap() -= 1;
    }

    fn join_room(&mut self, cid: &str, service: &str, room: &str, multi_rooms: bool) {
        let room_id = ServiceRoom::gen_room_id(service, room);
        if room_id.is_err() {
            error!(
                "recv bad join room data, sid: {}, channel: {}, room: {}, err: {}",
                cid,
                service,
                room,
                room_id.err().unwrap()
            );
            return;
        }

        let room_id = room_id.unwrap();

        let conn = self.get_conn_clone(cid);
        if conn.is_none() {
            return;
        }
        let conn = conn.unwrap();

        debug!(
            "conn join room, uid: {}, sid: {}, room: {}",
            conn.uid, cid, room_id
        );

        if !multi_rooms && self.get_conn_rooms(cid).is_some() {
            let quit_rooms: Vec<_> = self
                .get_conn_rooms(cid)
                .unwrap()
                .iter()
                .map(|x| x.clone())
                .collect();
            quit_rooms.iter().for_each(|room| self.quit_room(cid, room));
        }

        self.add_conn_room(cid, &room_id);
        self.rooms
            .entry(room_id)
            .or_insert_with(HashMap::new)
            .insert(cid.to_owned().to_string(), conn.clone());
    }

    fn quit_room(&mut self, cid: &str, room: &str) {
        if let Some(conns) = self.rooms.get_mut(room) {
            debug!("conn quit room, sid: {}, room: {}", cid, &room);
            conns.remove(cid);
            if conns.is_empty() {
                self.rooms.remove(room);
            }
        }

        self.del_conn_room(cid, room);
    }

    fn join_service(&mut self, cid: &str, service: &str) -> bool {
        let conn = self.get_conn_clone(cid);
        if conn.is_none() {
            return false;
        }

        let conn = conn.unwrap();
        if let Some(services) = self.services.get_mut(service) {
            if !services.contains_key(cid) {
                services.insert(cid.to_string(), conn.clone());
                self.add_conn_service(cid, service);
            }
        } else {
            self.services
                .entry(service.to_string())
                .or_insert_with(HashMap::new)
                .insert(cid.to_string(), conn.clone());
            self.add_conn_service(cid, service);
        }

        true
    }

    fn quit_service(&mut self, cid: &str, service: &str) -> bool {
        let quited = match self.services.get_mut(service) {
            Some(conns) => {
                debug!("conn quit service, sid: {}, channel: {}", cid, service);
                conns.remove(cid);
                if conns.len() == 0 {
                    self.services.remove(service);
                }
                true
            }
            _ => false,
        };

        if quited {
            self.del_conn_service(cid, service);
        }

        quited
    }

    async fn handle_conn_register_msg(&mut self, msg: ConnRegMsg) {
        if self.conns.contains_key(&msg.cid) {
            warn!("conn re-register: {}", msg.cid);
            return
        }

        let ConnRegMsg{
            cid,   uid, encoder, dt, addr
        } = msg;

        debug!("new conn registered in hub-{}: {}", self.ix, cid);
        self.incr_dt_count(dt);
        self.add_uid(&uid, &cid);

        let conn = Rc::new(Conn {
            uid, addr,
            encoder: encoder.clone(),
        });
        self.conns.insert(cid, conn);
        if !self.encoders.contains(&encoder) {
            self.encoders.push(encoder.clone());
            self.pilot
                .send_hub_msg(Hub2PilotMsg::EncodingMsg(encoder.name()))
                .await
                .unwrap_or_else(|e| error!("failed to send pilot encoding msg: {}", e));
        }
    }

    async fn handle_conn_unregister_msg(&mut self, msg: ConnUnregMsg) {
        debug!("conn unregistered in hub-{}: {}", self.ix, msg.cid);
        let conn = self.conns.remove(&msg.cid);
        if conn.is_none() {
            return;
        }

        self.decr_dt_count(msg.dt);
        self.remove_uid(&msg.uid, &msg.cid);

        let session = conn.unwrap();
        debug!("conn unreg data: {} - {}", msg.cid, session);

        let mut quit_rooms = HashSet::new();
        if let Some(rooms) = self.get_conn_rooms(&msg.cid) {
            quit_rooms = rooms.clone();
        }
        quit_rooms
            .iter()
            .for_each(|room| self.quit_room(&msg.cid, room));

        let mut quit_services = HashSet::new();
        if let Some(services) = self.get_conn_services(&msg.cid) {
            quit_services = services.clone();
        }

        for service in quit_services {
            if !self.quit_service(&msg.cid, &service) {
                continue;
            }
            if let Some(conf) = self.service_confs.get(&service) {
                if !conf.enable_close_notify {
                    continue;
                }
                let mut service_msg = lib::ServiceMsg::new();
                service_msg.cid = Some(msg.cid.clone());
                service_msg.typ = lib::CHAN_MSG_TYPE_CONN_QUIT;
                service_msg.uid = Some(msg.uid.clone());
                service_msg.cid = Some(msg.cid.clone());
                service_msg.ip = msg.ip.clone();
                debug!(
                    "send client quit msg to channel: {} - {:?}",
                    service, service_msg
                );
                self.pilot
                    .send_hub_msg(Hub2PilotMsg::BizMsg(PilotServiceBizMsg {
                        service: service,
                        data: bytes::Bytes::from(serde_json::to_vec(&service_msg).unwrap()),
                    }))
                    .await
                    .unwrap_or_else(|e| {
                        error!("failed to send pilot msg: {}", e);
                    });
            }
        }
    }

    async fn handle_conn_biz_msg(&mut self, mut msg: HubServiceBizMsg) {
        if msg.cid.is_none() {
            warn!("empty sid for session biz msg: {}", msg.id);
            return;
        }

        let cid = msg.cid.take().unwrap();
        debug!("handle session biz msg, sid: {:?}, msg: {}", cid, msg.id);

        if let Some(session) = self.get_conn(&cid) {
            match msg.data.get_data_with_encoder(&session.encoder) {
                Ok(data) => {
                    session
                        .send_conn_msg(&cid, Hub2ConnMsg::BIZ(data))
                        .await;
                    metric::inc_send_msg(msg.service.as_ref().unwrap_or(&"x".to_string()), 1);
                }
                Err(e) => {
                    error!(
                        "failed to encode session client msg, msgid: {}, err: {}",
                        msg.id, e
                    );
                }
            }
        } else {
            debug!(
                "conn not found for HubChannelBizMsg, sid: {}, msgid: {}",
                cid, msg.id
            );
        }
    }

    async fn handle_room_biz_msg(&mut self, msg: HubServiceBizMsg) {
        debug!("handle room biz msg: {}", msg.id);
        if msg.service.is_none() || msg.room.is_none() {
            warn!("bad msg: channel or room is empty - {}", msg.id);
            return;
        }

        if let Ok(ref rid) =
            ServiceRoom::gen_room_id(msg.service.as_ref().unwrap(), msg.room.as_ref().unwrap())
        {
            return self
                .broadcast_service_bizmsg(msg, self.get_room(rid))
                .await;
        }

        warn!(
            "bad service room name, msgid: {}, channel: {}, room: {}",
            msg.id,
            msg.service.unwrap_or("x".to_string()),
            msg.room.unwrap_or("x".to_string())
        );
    }

    async fn handle_service_biz_msg(&mut self, msg: HubServiceBizMsg) {
        debug!("handle service biz msg: {}", msg.id);
        if msg.room.is_some() {
            return self.handle_room_biz_msg(msg).await;
        }

        let conns = self.get_service(msg.service.as_ref().unwrap());
        self.broadcast_service_bizmsg(msg, conns).await
    }

    async fn broadcast_service_bizmsg(
        &self,
        mut msg: HubServiceBizMsg,
        conns: Option<&HashMap<String, Rc<Conn>>>,
    ) {
        if conns.is_none() {
            return;
        }

        let mut send_count: u64 = 0;
        let whites = msg.whites.take();
        let blacks = msg.blacks.take();
        let ratio = msg.ratio.take();

        for (sid, conn) in conns.unwrap().iter() {
            if self.filter_conn(&conn.uid, &whites, &blacks, &ratio) {
                match msg.data.get_data_with_encoder(&conn.encoder) {
                    Err(e) => {
                        error!(
                            "failed to encode msg, encoder: {}, err: {}",
                            conn.encoder.name(),
                            e
                        );
                    }
                    Ok(data) => {
                        conn
                            .send_conn_msg(sid, Hub2ConnMsg::BIZ(data))
                            .await;
                        send_count += 1;
                    }
                }
            }
        }

        metric::inc_send_msg(
            match msg.service.as_ref() {
                Some(s) => s.as_str(),
                None => "x",
            },
            send_count,
        );
    }

    async fn broadcast_bizmsg(&mut self, mut msg: HubServiceBizMsg) {
        if self.conns.is_empty() {
            return;
        }

        let mut send_count: u64 = 0;
        let whites = msg.whites.take();
        let blacks = msg.blacks.take();
        let ratio = msg.ratio.take();
        for (sid, conn) in self.conns.iter() {
            if self.filter_conn(&conn.uid, &whites, &blacks, &ratio) {
                continue;
            }
            match msg.data.get_data_with_encoder(&conn.encoder) {
                Err(e) => {
                    error!(
                        "failed to encode msg, encoder: {}, err: {}",
                        conn.encoder.name(),
                        e
                    );
                }
                Ok(data) => {
                    conn
                        .send_conn_msg(sid, Hub2ConnMsg::BIZ(data))
                        .await;
                    send_count += 1;
                }
            }
        }
        metric::inc_send_msg(
            match msg.service.as_ref() {
                Some(s) => s.as_str(),
                None => "x",
            },
            send_count,
        );
    }

    async fn handle_join_service_msg(&mut self, msg: HubJoinServiceMsg) {
        if self.join_service(&msg.cid, &msg.service) {
            msg.rooms
                .iter()
                .for_each(|room| self.join_room(&msg.cid, &msg.service, room, msg.multi_rooms));
        }
    }

    fn proc_session_leave_room(&mut self, cid: &str, service: &str, room: &str) {
        if let Ok(ref rid) = ServiceRoom::gen_room_id(service, room) {
            self.quit_room(cid, rid);
        }
    }

    async fn handle_leave_room_msg(&mut self, msg: HubLeaveRoomMsg) {
        if let Some(ref cid) = msg.cid {
            return self.proc_session_leave_room(cid, &msg.service, &msg.room);
        }

        if msg.uid.is_some() {
            if let Some(sids) = self.get_uid_sids_clone(msg.uid.as_ref().unwrap()) {
                sids.iter().for_each(|sid| {
                    self.proc_session_leave_room(sid, &msg.service, &msg.room);
                });
            }
        }
    }

    fn filter_conn(
        &self,
        uid: &String,
        whites: &Option<Vec<String>>,
        blacks: &Option<Vec<String>>,
        ratio: &Option<u8>,
    ) -> bool {
        if let Some(v) = whites {
            if v.contains(uid) {
                return true;
            }
        }

        if let Some(v) = blacks {
            if v.contains(uid) {
                return false;
            }
        }

        if let Some(ratio) = ratio {
            return self.rand.clone().gen_range(0, 100) < *ratio;
        }

        true
    }

    async fn do_stats(&self) {
        debug!(
            "hub-{} stats, totoal-conns: {}, android: {}, ios: {}, x: {}, services: {}, rooms: {}, uids:{}",
            self.ix,
            self.conns.len(),
            self.dt_conns.get(DEVICE_TYPE_ANDROID).unwrap(),
            self.dt_conns.get(DEVICE_TYPE_IOS).unwrap(),
            self.dt_conns.get(DEVICE_TYPE_X).unwrap(),
            self.services.len(),
            self.rooms.len(),
            self.uid_conns.len(),
        );

        let service_conns: HashMap<String, u64> = self
            .services
            .iter()
            .map(|(k, v)| (k.to_owned(), v.len() as u64))
            .collect();
        self.metric_collector
            .clone()
            .send_hub_msg(HubMetricMsg {
                service_conns,
                ix: self.ix,
                dt_conns: self.dt_conns.clone(),
            })
            .await
            .unwrap_or_else(|e| error!("failed to send metric stat msg: {}", e));
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct AllSenders {
        hub_sender: HubSender,
        pilot_sender: PilotSender,
    }

    fn async_env_prepare()  {

    }

}
