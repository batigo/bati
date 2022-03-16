use crate::conn_proto::*;
use crate::const_proto::*;
use crate::encoding::*;
use crate::hub_proto::*;
use crate::metric;
use crate::metric_proto::*;
use crate::pilot_proto::*;
use bati_lib as lib;
use log::{debug, error, warn};
use ntex::rt;
use ntex::util::Bytes;
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
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
    async fn send_conn_msg(&self, cid: &str, msg: Hub2ConnMsg) {
        warn!("send msg to conn: {} - {:?}", cid, msg);
        if let Err(e) = self.addr.send_hub_msg(msg).await {
            warn!("failed to send conn biz msg: {} - {}", cid, e);
            self.addr.send_hub_msg(Hub2ConnMsg::QUIT).await;
        }
    }
}

impl std::fmt::Display for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "conn, id: {}, endcode: {}", self.addr.id, self.encoder)
    }
}

struct ServiceRoom;

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
        let (sender, receiver) = new_hub_channel(1024);
        let mut hub = Hub {
            ix,
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
                    Conn2HubMsg::Unregister(msg) => self.handle_conn_unregister_msg(msg).await,
                }
            }
            HubMessage::FromPilot(msg) => {
                debug!("recv Pilot2HubMsg in hub-{}: {:?}", self.ix, msg);
                match msg {
                    Pilot2HubMsg::Biz(msg) => self.handle_biz_msg(msg).await,
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
                let mut data = HubDataQueryData {
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

    fn get_conn_clone(&self, cid: &str) -> Option<Rc<Conn>> {
        self.conns.get(cid).cloned()
    }

    fn get_uid_cids_clone(&self, uid: &str) -> Option<HashSet<String>> {
        self.uid_conns.get(uid).cloned()
    }

    fn get_uid_cids(&self, uid: &str) -> Option<&HashSet<String>> {
        self.uid_conns.get(uid)
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
        debug!("handle join room: {}, {}, {}", cid, service, room);
        let room_id = ServiceRoom::gen_room_id(service, room);
        if room_id.is_err() {
            error!(
                "recv bad join room data, sid: {}, : {}, room: {}, err: {}",
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
            "======= conn join room, uid: {}, sid: {}, room: {}",
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
                debug!("conn quit service, sid: {}, : {}", cid, service);
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
            return;
        }

        let ConnRegMsg {
            cid,
            uid,
            encoder,
            dt,
            addr,
        } = msg;

        debug!("new conn registered in hub-{}: {}", self.ix, cid);
        self.incr_dt_count(dt);
        self.add_uid(&uid, &cid);

        let conn = Rc::new(Conn {
            uid,
            addr,
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

        let conn = conn.unwrap();
        debug!("conn unreg data: {} - {}", msg.cid, conn);

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
                let bati_msg = lib::BatiMsg::new(
                    None,
                    lib::BATI_MSG_TYPE_CONN_QUIT,
                    msg.cid.clone(),
                    msg.uid.clone(),
                    msg.ip.clone(),
                    None,
                );
                debug!(
                    "send client quit msg to service: {} - {:?}",
                    service, bati_msg
                );
                self.pilot
                    .send_hub_msg(Hub2PilotMsg::BizMsg(PilotServiceBizMsg {
                        service,
                        data: Bytes::from(serde_json::to_vec(&bati_msg).unwrap()),
                    }))
                    .await
                    .unwrap_or_else(|e| {
                        error!("failed to send pilot msg: {}", e);
                    });
            }
        }
    }

    async fn handle_biz_msg(&mut self, msg: HubServiceBizMsg) {
        match msg.typ {
            lib::BIZ_MSG_TYPE_USERS => self.handle_users_biz_msg(msg).await,
            lib::BIZ_MSG_TYPE_ROOM => self.handle_room_biz_msg(msg).await,
            lib::BIZ_MSG_TYPE_SERVICE => self.handle_service_biz_msg(msg).await,
            lib::BIZ_MSG_TYPE_ALL => self.broadcast_bizmsg(msg).await,
            _ => {}
        }
        return;
    }

    async fn handle_users_biz_msg(&mut self, mut msg: HubServiceBizMsg) {
        let cids = msg.cids.take();
        let uids = msg.uids.take();
        if cids.is_none() && uids.is_none() {
            return;
        }

        let mut send_count = 0;
        if uids.is_some() {
            for uid in uids.as_ref().unwrap() {
                send_count += self.send_uid_biz_msg(uid, &mut msg, true).await;
            }
        } else {
            for cid in cids.as_ref().unwrap() {
                send_count += self.send_conn_biz_msg(cid, &mut msg, true).await;
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

    async fn send_conn_biz_msg(
        &self,
        cid: &str,
        msg: &mut HubServiceBizMsg,
        fiter_service_room: bool,
    ) -> u64 {
        let conn = self.get_conn(cid);
        if conn.is_none() {
            debug!(
                "conn not found for HubChannelBizMsg, sid: {}, msgid: {}",
                cid, msg.id
            );
            return 0;
        }

        if fiter_service_room {
            let conns;
            match msg.room.as_ref() {
                Some(rid) => conns = self.get_room(rid),
                None => conns = self.get_service(msg.service.as_ref().unwrap()),
            }
            if conns.is_none() || !conns.as_ref().unwrap().contains_key(cid) {
                return 0;
            }
        }

        let conn = conn.unwrap();
        match msg.data.get_data_with_encoder(&conn.encoder) {
            Ok(data) => {
                conn.send_conn_msg(&cid, Hub2ConnMsg::BIZ(data)).await;
                1
            }
            Err(e) => {
                error!(
                    "failed to encode conn client msg, msgid: {}, err: {}",
                    msg.id, e
                );
                0
            }
        }
    }

    async fn send_uid_biz_msg(
        &mut self,
        uid: &str,
        msg: &mut HubServiceBizMsg,
        filter_service_room: bool,
    ) -> u64 {
        let mut n: u64 = 0;
        let cids = self.get_uid_cids_clone(uid);
        if cids.is_none() {
            return 0;
        }

        let cids = cids.unwrap();
        if !filter_service_room || msg.service.is_none() {
            for cid in &cids {
                n += self.send_conn_biz_msg(cid, msg, false).await;
            }
            return n;
        }

        let conns;
        match msg.room.as_ref() {
            Some(rid) => conns = self.get_room(rid),
            None => conns = self.get_service(msg.service.as_ref().unwrap()),
        }
        if conns.is_none() {
            return 0;
        }

        let conns = conns.unwrap();
        for cid in &cids {
            if conns.contains_key(cid) {
                n += self.send_conn_biz_msg(cid, msg, false).await;
            }
        }
        return n;
    }

    async fn handle_room_biz_msg(&mut self, msg: HubServiceBizMsg) {
        debug!("handle room biz msg: {}", msg.id);
        if msg.service.is_none() || msg.room.is_none() {
            warn!("bad msg: service or room missing - {}", msg.id);
            return;
        }

        if let Ok(ref rid) =
            ServiceRoom::gen_room_id(msg.service.as_ref().unwrap(), msg.room.as_ref().unwrap())
        {
            return self.broadcast_service_bizmsg(msg, self.get_room(rid)).await;
        }

        warn!(
            "bad service room name, msgid: {}, service: {}, room: {}",
            msg.id,
            msg.service.unwrap_or("x".to_string()),
            msg.room.unwrap_or("x".to_string())
        );
    }

    async fn handle_service_biz_msg(&mut self, msg: HubServiceBizMsg) {
        debug!("handle service biz msg: {}", msg.id);
        if msg.service.is_none() {
            warn!("bad msg: service missing - {}", msg.id);
            return;
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

        let conns = conns.unwrap();
        let mut send_count: u64 = 0;
        let whites = msg.whites.take();
        let blacks = msg.blacks.take();
        let ratio: u8;
        if let Some(n) = msg.ratio.take() {
            ratio = n;
        } else {
            ratio = 100;
        }

        let need_filter = ratio < 100 || blacks.is_some();

        let mut randd: Option<ThreadRng> = None;
        if need_filter {
            randd = Some(rand::thread_rng());
        }

        let mut rand = rand::thread_rng();
        for (sid, conn) in conns.iter() {
            if need_filter
                && !self.filter_conn(&conn.uid, &whites, &blacks, ratio, randd.as_mut().unwrap())
            {
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
                    conn.send_conn_msg(sid, Hub2ConnMsg::BIZ(data)).await;
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

    async fn broadcast_bizmsg(&mut self, mut msg: HubServiceBizMsg) {
        return self.broadcast_service_bizmsg(msg, Some(&self.conns)).await;
    }

    async fn handle_join_service_msg(&mut self, msg: HubJoinServiceMsg) {
        warn!("recv join service msg: {:?}", msg);
        if let Some(cid) = msg.cid.as_ref() {
            self.handle_conn_join_service(cid, &msg);
        } else if let Some(uid) = msg.uid.as_ref() {
            self.handle_uid_join_service(uid, &msg);
        }
    }

    fn handle_conn_join_service(&mut self, cid: &str, msg: &HubJoinServiceMsg) {
        if !self.conns.contains_key(cid) {
            warn!("++++ conn not found in service join msg: {:?}", msg);
            return;
        }
        if msg.join_service {
            self.join_service(cid, &msg.service);
        }
        if let Some(rooms) = msg.rooms.as_ref() {
            rooms
                .iter()
                .for_each(|room| self.join_room(cid, &msg.service, room, msg.multi_rooms));
        }
    }

    fn handle_uid_join_service(&mut self, uid: &str, msg: &HubJoinServiceMsg) {
        let cids = self.get_uid_cids_clone(uid);
        if cids.is_none() {
            return;
        }

        let cids = cids.unwrap();
        for cid in &cids {
            self.handle_conn_join_service(cid, msg);
        }
    }

    fn handle_conn_leave_room(&mut self, cid: &str, service: &str, room: &str) {
        if let Ok(ref rid) = ServiceRoom::gen_room_id(service, room) {
            self.quit_room(cid, rid);
        }
    }

    async fn handle_leave_room_msg(&mut self, msg: HubLeaveRoomMsg) {
        if let Some(cid) = msg.cid.as_ref() {
            if let Some(rooms) = msg.rooms.as_ref() {
                for room in rooms {
                    self.handle_conn_leave_room(cid, &msg.service, room);
                }
            }
            if msg.quit_service {
                self.quit_service(cid, &msg.service);
            }
        }

        if let Some(uid) = msg.uid.as_ref() {
            if let Some(cids) = self.get_uid_cids_clone(uid) {
                for cid in &cids {
                    if let Some(rooms) = msg.rooms.as_ref() {
                        for room in rooms {
                            self.handle_conn_leave_room(cid, &msg.service, room);
                        }
                    }
                    if msg.quit_service {
                        self.quit_service(cid, &msg.service);
                    }
                }
            }
        }
    }

    fn filter_conn(
        &self,
        uid: &String,
        whites: &Option<Vec<String>>,
        blacks: &Option<Vec<String>>,
        ratio: u8,
        rand: &mut rand::rngs::ThreadRng,
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

        return ratio >= 100 || rand.gen_range(0, 100) < ratio;
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
}
