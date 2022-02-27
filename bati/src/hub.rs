use crate::const_proto::*;
use crate::encoding::*;
use crate::hub_proto::*;
use crate::metric;
use crate::metric_proto::*;
use crate::pilot_proto::*;
use crate::session_proto::*;
use bati_lib as lib;
use log::{debug, error, warn};
use ntex::rt;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub struct Hub {
    ix: usize,
    sessions: HashMap<String, Rc<Session>>,
    rooms: HashMap<String, HashMap<String, Rc<Session>>>,
    session_rooms: HashMap<String, HashSet<String>>,
    channels: HashMap<String, HashMap<String, Rc<Session>>>,
    session_channels: HashMap<String, HashSet<String>>,
    uid_sessions: HashMap<String, HashSet<String>>,
    dt_sessions: HashMap<DeviceType, u64>,
    rand: rand::rngs::ThreadRng,
    pilot: PilotSender,
    metric_collector: MetricSender,
    channel_confs: HashMap<String, lib::ChannelConf>,
    encoders: Vec<Encoder>,
    msg_receiver: HubReceiver,
    msg_sender: HubSender,
}

#[derive(Clone)]
struct Session {
    uid: String,
    encoder: Encoder,
    addr: SessionSender,
}

impl Session {
    async fn send_session_msg(&self, sid: &str, msg: Hub2SessionMsg) {
        self.addr.send_hub_msg(msg).await.unwrap_or_else(|e| {
            warn!("failed to send session biz msg: {} - {}", sid, e);
        });
    }
}

impl std::fmt::Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "session, id: {}, endcode: {}",
            self.addr.id, self.encoder
        )
    }
}

#[derive(PartialEq)]
struct ChannelRoom {
    channel: String,
    room: String,
}

impl ChannelRoom {
    // fn parse_room_id(room_id: &str) -> Result<Self, &'static str> {
    //     let v: Vec<&str> = room_id.split("@_@").collect();
    //     if v.len() != 2 {
    //         return Err("bad room id format");
    //     }
    //
    //     Ok(ChannelRoom {
    //         channel: v[0].to_string(),
    //         room: v[1].to_string(),
    //     })
    // }

    fn validate_room_id(room_id: &str) -> bool {
        let v: Vec<&str> = room_id.split("@_@").collect();
        v.len() == 2
    }

    fn gen_room_id(channel: &str, room: &str) -> Result<String, &'static str> {
        let room_id = format!("{}@_@{}", channel, room);
        if !Self::validate_room_id(&room_id) {
            return Err("bad channel & room value");
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
            sessions: HashMap::with_capacity(10240),
            rooms: HashMap::with_capacity(1024),
            session_rooms: HashMap::with_capacity(10240),
            channels: HashMap::with_capacity(32),
            session_channels: HashMap::with_capacity(10240),
            uid_sessions: HashMap::with_capacity(10240),
            dt_sessions: HashMap::with_capacity(3),
            channel_confs: HashMap::with_capacity(32),
            encoders: vec![],
            msg_receiver: receiver,
            msg_sender: sender,
        };

        hub.dt_sessions.insert(DEVICE_TYPE_ANDROID, 0);
        hub.dt_sessions.insert(DEVICE_TYPE_IOS, 0);
        hub.dt_sessions.insert(DEVICE_TYPE_X, 0);
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
            HubMessage::FromSession(msg) => {
                debug!("recv Session2HubMsg in hub-{}: {}", self.ix, msg);
                match msg {
                    Session2HubMsg::Register(msg) => self.handle_session_register_msg(msg).await,
                    Session2HubMsg::Unregister(msg) => {
                        self.handle_session_unregister_msg(msg).await
                    }
                }
            }
            HubMessage::FromPilot(msg) => {
                debug!("recv Pilot2HubMsg in hub-{}: {:?}", self.ix, msg);
                match msg {
                    Pilot2HubMsg::Biz(msg) => match msg.typ {
                        ChannelBizMsgType::Session => self.handle_session_biz_msg(msg).await,
                        ChannelBizMsgType::Room => self.handle_room_biz_msg(msg).await,
                        ChannelBizMsgType::Channel => self.handle_channel_biz_msg(msg).await,
                        ChannelBizMsgType::Broadcast => self.broadcast_bizmsg(msg).await,
                    },
                    Pilot2HubMsg::JoinChannel(msg) => self.handle_join_channel_msg(msg).await,
                    Pilot2HubMsg::LeaveRoom(msg) => self.handle_leave_room_msg(msg).await,
                    Pilot2HubMsg::ChannelConf(conf) => {
                        self.channel_confs.insert(conf.name.clone(), conf);
                    }
                }
            }

            HubMessage::FromTimer(msg) => match msg {
                Timer2HubMsg::MetricStat => self.do_stats().await,
            },
        }
    }

    fn get_session(&self, sid: &str) -> Option<&Rc<Session>> {
        self.sessions.get(sid)
    }

    fn get_session_clone(&self, sid: &str) -> Option<Rc<Session>> {
        self.sessions.get(sid).cloned()
    }

    fn get_uid_sids_clone(&self, uid: &str) -> Option<HashSet<String>> {
        self.uid_sessions.get(uid).cloned()
    }

    fn get_room(&self, room: &str) -> Option<&HashMap<String, Rc<Session>>> {
        self.rooms.get(room)
    }

    fn get_channel(&self, channel: &str) -> Option<&HashMap<String, Rc<Session>>> {
        self.channels.get(channel)
    }

    fn get_session_rooms(&self, sid: &str) -> Option<&HashSet<String>> {
        self.session_rooms.get(sid)
    }

    fn add_session_room(&mut self, sid: &str, room: &str) {
        if let Some(rooms) = self.session_rooms.get_mut(sid) {
            rooms.insert(room.to_string());
            return;
        }

        let mut s = HashSet::new();
        s.insert(room.to_string());
        self.session_rooms.insert(sid.to_string(), s);
    }

    fn del_session_room(&mut self, sid: &str, room: &str) {
        if let Some(rooms) = self.session_rooms.get_mut(sid) {
            rooms.remove(room);
            if rooms.is_empty() {
                self.session_rooms.remove(sid);
            }
        }
    }

    fn get_session_channels(&self, sid: &str) -> Option<&HashSet<String>> {
        self.session_channels.get(sid)
    }

    fn add_session_channel(&mut self, sid: &str, channel: &str) {
        if let Some(channels) = self.session_channels.get_mut(sid) {
            channels.insert(channel.to_string());
            return;
        }

        let mut s = HashSet::new();
        s.insert(channel.to_string());
        self.session_channels.insert(sid.to_string(), s);
    }

    fn del_session_channel(&mut self, sid: &str, channel: &str) {
        if let Some(channels) = self.session_channels.get_mut(sid) {
            channels.remove(channel);
            if channels.is_empty() {
                self.session_channels.remove(sid);
            }
        }
    }

    fn add_uid(&mut self, uid: &str, sid: &str) {
        self.uid_sessions
            .entry(uid.to_string())
            .or_insert_with(HashSet::new)
            .insert(sid.to_string());
    }

    fn remove_uid(&mut self, uid: &str, sid: &str) -> bool {
        match self.uid_sessions.get_mut(uid) {
            Some(sessions) => {
                if sessions.len() <= 1 {
                    self.uid_sessions.remove(uid);
                } else {
                    sessions.remove(sid);
                }
                true
            }
            _ => false,
        }
    }

    fn incr_dt_count(&mut self, dt: DeviceType) {
        *self.dt_sessions.get_mut(dt).unwrap() += 1;
    }

    fn decr_dt_count(&mut self, dt: DeviceType) {
        *self.dt_sessions.get_mut(dt).unwrap() -= 1;
    }

    fn join_room(&mut self, sid: &str, channel: &str, room: &str, multi_rooms: bool) {
        let room_id = ChannelRoom::gen_room_id(channel, room);
        if room_id.is_err() {
            error!(
                "recv bad join room data, sid: {}, channel: {}, room: {}, err: {}",
                sid,
                channel,
                room,
                room_id.err().unwrap()
            );
            return;
        }

        let room_id = room_id.unwrap();

        let session = self.get_session_clone(sid);
        if session.is_none() {
            return;
        }
        let session = session.unwrap();

        debug!(
            "session join room, uid: {}, sid: {}, room: {}",
            session.uid, sid, room_id
        );

        if !multi_rooms && self.get_session_rooms(sid).is_some() {
            let quit_rooms: Vec<_> = self
                .get_session_rooms(sid)
                .unwrap()
                .iter()
                .map(|x| x.clone())
                .collect();
            quit_rooms.iter().for_each(|room| self.quit_room(sid, room));
        }

        self.add_session_room(sid, &room_id);
        self.rooms
            .entry(room_id)
            .or_insert_with(HashMap::new)
            .insert(sid.to_owned().to_string(), session.clone());
    }

    fn quit_room(&mut self, sid: &str, room: &str) {
        if let Some(sessions) = self.rooms.get_mut(room) {
            debug!("session quit room, sid: {}, room: {}", sid, &room);
            sessions.remove(sid);
            if sessions.is_empty() {
                self.rooms.remove(room);
            }
        }

        self.del_session_room(sid, room);
    }

    fn join_channel(&mut self, sid: &str, channel: &str) -> bool {
        let session = self.get_session_clone(sid);
        if session.is_none() {
            return false;
        }

        let session = session.unwrap();
        if let Some(channels) = self.channels.get_mut(channel) {
            if !channels.contains_key(sid) {
                channels.insert(sid.to_string(), session.clone());
                self.add_session_channel(sid, channel);
            }
        } else {
            self.channels
                .entry(channel.to_string())
                .or_insert_with(HashMap::new)
                .insert(sid.to_string(), session.clone());
            self.add_session_channel(sid, channel);
        }

        true
    }

    fn quit_channel(&mut self, sid: &str, channel: &str) -> bool {
        let quited = match self.channels.get_mut(channel) {
            Some(sessions) => {
                debug!("session quit channel, sid: {}, channel: {}", sid, channel);
                sessions.remove(sid);
                if sessions.len() == 0 {
                    self.channels.remove(channel);
                }
                true
            }
            _ => false,
        };

        if quited {
            self.del_session_channel(sid, channel);
        }

        quited
    }

    async fn handle_session_register_msg(&mut self, msg: SessionRegMsg) {
        debug!("new session registered in hub-{}: {}", self.ix, &msg.sid);
        self.incr_dt_count(msg.dt);
        self.add_uid(&msg.uid, &msg.sid);

        let session = Rc::new(Session {
            uid: msg.uid,
            addr: msg.addr,
            encoder: msg.encoder.clone(),
        });
        self.sessions.insert(msg.sid, session);
        if !self.encoders.contains(&msg.encoder) {
            self.encoders.push(msg.encoder.clone());
            self.pilot
                .send_hub_msg(Hub2PilotMsg::EncodingMsg(msg.encoder.name()))
                .await
                .unwrap_or_else(|e| error!("failed to send pilot encoding msg: {}", e));
        }
    }

    async fn handle_session_unregister_msg(&mut self, msg: SessionUnregMsg) {
        debug!("session unregistered in hub-{}: {}", self.ix, msg.sid);
        let session = self.sessions.remove(&msg.sid);
        if session.is_none() {
            return;
        }

        self.decr_dt_count(msg.dt);
        self.remove_uid(&msg.uid, &msg.sid);

        let session = session.unwrap();
        debug!("session unreg data: {} - {}", msg.sid, session);

        let mut quit_rooms = HashSet::new();
        if let Some(rooms) = self.get_session_rooms(&msg.sid) {
            quit_rooms = rooms.clone();
        }
        quit_rooms
            .iter()
            .for_each(|room| self.quit_room(&msg.sid, room));

        let mut quit_channels = HashSet::new();
        if let Some(channels) = self.get_session_channels(&msg.sid) {
            quit_channels = channels.clone();
        }

        for channel in quit_channels {
            if !self.quit_channel(&msg.sid, &channel) {
                continue;
            }
            if let Some(conf) = self.channel_confs.get(&channel) {
                if !conf.enable_close_notify {
                    continue;
                }
                let mut channel_msg = lib::ChannelMsg::new();
                channel_msg.sid = Some(msg.sid.clone());
                channel_msg.typ = lib::CHAN_MSG_TYPE_CLIENT_QUIT;
                channel_msg.uid = Some(msg.uid.clone());
                channel_msg.sid = Some(msg.sid.clone());
                channel_msg.ip = msg.ip.clone();
                debug!(
                    "send client quit msg to channel: {} - {:?}",
                    channel, channel_msg
                );
                self.pilot
                    .send_hub_msg(Hub2PilotMsg::ChannelMsg(PilotChannelMsg {
                        channel,
                        data: bytes::Bytes::from(serde_json::to_vec(&channel_msg).unwrap()),
                    }))
                    .await
                    .unwrap_or_else(|e| {
                        error!("failed to send pilot msg: {}", e);
                    });
            }
        }
    }

    async fn handle_session_biz_msg(&mut self, mut msg: HubChannelBizMsg) {
        if msg.sid.is_none() {
            warn!("empty sid for session biz msg: {}", msg.id);
            return;
        }

        let sid = msg.sid.take().unwrap();
        debug!("handle session biz msg, sid: {:?}, msg: {}", sid, msg.id);

        if let Some(session) = self.get_session(&sid) {
            match msg.data.get_data_with_encoder(&session.encoder) {
                Ok(data) => {
                    session
                        .send_session_msg(&sid, Hub2SessionMsg::BIZ(data))
                        .await;
                    metric::inc_send_msg(msg.channel.as_ref().unwrap_or(&"x".to_string()), 1);
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
                "session not found for HubChannelBizMsg, sid: {}, msgid: {}",
                sid, msg.id
            );
        }
    }

    async fn handle_room_biz_msg(&mut self, msg: HubChannelBizMsg) {
        debug!("handle room biz msg: {}", msg.id);
        if msg.channel.is_none() || msg.room.is_none() {
            warn!("bad msg: channel or room is empty - {}", msg.id);
            return;
        }

        if let Ok(ref room_id) =
            ChannelRoom::gen_room_id(msg.channel.as_ref().unwrap(), msg.room.as_ref().unwrap())
        {
            return self
                .broadcast_channel_bizmsg(msg, self.get_room(room_id))
                .await;
        }

        warn!(
            "bad channel room name, msgid: {}, channel: {}, room: {}",
            msg.id,
            msg.channel.unwrap_or("x".to_string()),
            msg.room.unwrap_or("x".to_string())
        );
    }

    async fn handle_channel_biz_msg(&mut self, msg: HubChannelBizMsg) {
        debug!("handle channel biz msg: {}", msg.id);
        if msg.room.is_some() {
            return self.handle_room_biz_msg(msg).await;
        }

        let sessions = self.get_channel(msg.channel.as_ref().unwrap());
        self.broadcast_channel_bizmsg(msg, sessions).await
    }

    async fn broadcast_channel_bizmsg(
        &self,
        mut msg: HubChannelBizMsg,
        sessions: Option<&HashMap<String, Rc<Session>>>,
    ) {
        if sessions.is_none() {
            return;
        }

        let mut send_count: u64 = 0;
        let whites = msg.whites.take();
        let blacks = msg.blacks.take();
        let ratio = msg.ratio.take();

        for (sid, session) in sessions.unwrap().iter() {
            if self.filter_session(&session.uid, &whites, &blacks, &ratio) {
                match msg.data.get_data_with_encoder(&session.encoder) {
                    Err(e) => {
                        error!(
                            "failed to encode msg, encoder: {}, err: {}",
                            session.encoder.name(),
                            e
                        );
                    }
                    Ok(data) => {
                        session
                            .send_session_msg(sid, Hub2SessionMsg::BIZ(data))
                            .await;
                        send_count += 1;
                    }
                }
            }
        }

        metric::inc_send_msg(
            match msg.channel.as_ref() {
                Some(s) => s.as_str(),
                None => "x",
            },
            send_count,
        );
    }

    async fn broadcast_bizmsg(&mut self, mut msg: HubChannelBizMsg) {
        if self.sessions.is_empty() {
            return;
        }

        let mut send_count: u64 = 0;
        let whites = msg.whites.take();
        let blacks = msg.blacks.take();
        let ratio = msg.ratio.take();
        for (sid, session) in self.sessions.iter() {
            if self.filter_session(&session.uid, &whites, &blacks, &ratio) {
                continue;
            }
            match msg.data.get_data_with_encoder(&session.encoder) {
                Err(e) => {
                    error!(
                        "failed to encode msg, encoder: {}, err: {}",
                        session.encoder.name(),
                        e
                    );
                }
                Ok(data) => {
                    session
                        .send_session_msg(sid, Hub2SessionMsg::BIZ(data))
                        .await;
                    send_count += 1;
                }
            }
        }
        metric::inc_send_msg(
            match msg.channel.as_ref() {
                Some(s) => s.as_str(),
                None => "x",
            },
            send_count,
        );
    }

    async fn handle_join_channel_msg(&mut self, msg: HubJoinChannelMsg) {
        if self.join_channel(&msg.sid, &msg.channel) {
            msg.rooms
                .iter()
                .for_each(|room| self.join_room(&msg.sid, &msg.channel, room, msg.multi_rooms));
        }
    }

    fn proc_session_leave_room(&mut self, sid: &str, channel: String, room: String) {
        if let Ok(ref room_id) = ChannelRoom::gen_room_id(&channel, &room) {
            self.quit_room(sid, room_id);
        }
    }

    async fn handle_leave_room_msg(&mut self, msg: HubLeaveRoomMsg) {
        if let Some(ref sid) = msg.sid {
            return self.proc_session_leave_room(sid, msg.channel, msg.room);
        }

        if msg.uid.is_some() {
            if let Some(sids) = self.get_uid_sids_clone(msg.uid.as_ref().unwrap()) {
                sids.iter().for_each(|sid| {
                    self.proc_session_leave_room(sid, msg.channel.clone(), msg.room.clone());
                });
            }
        }
    }

    fn filter_session(
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
            "hub-{} stats, totoal-sessions: {}, android: {}, ios: {}, x: {}, channels: {}, rooms: {}, uids:{}",
            self.ix,
            self.sessions.len(),
            self.dt_sessions.get(DEVICE_TYPE_ANDROID).unwrap(),
            self.dt_sessions.get(DEVICE_TYPE_IOS).unwrap(),
            self.dt_sessions.get(DEVICE_TYPE_X).unwrap(),
            self.channels.len(),
            self.rooms.len(),
            self.uid_sessions.len(),
        );

        let channel_sessions: HashMap<String, u64> = self
            .channels
            .iter()
            .map(|(k, v)| (k.to_owned(), v.len() as u64))
            .collect();
        self.metric_collector
            .clone()
            .send_hub_msg(HubMetricMsg {
                channel_sessions,
                ix: self.ix,
                dt_sessions: self.dt_sessions.clone(),
            })
            .await
            .unwrap_or_else(|e| error!("failed to send metric stat msg: {}", e));
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_reg_session() {
        let mut hub = Hub::new(1);
        let uid = 1;
        let sid = "abcd";
        let msg = SessionRegMsg {
            uid,
            sid: sid.to_string(),
            did: "".to_string(),
            ip: None,
            encoder: Encoder::new(NULLENCODER_NAME),
            dt: DEVICE_TYPE_IOS,
            addr: None,
        };

        hub.handle_session_register_msg(msg);
        assert_eq!(hub.sessions.len(), 1);
        assert_eq!(hub.sessions.get(sid).is_some(), true);
        assert_eq!(hub.uid_sessions.get(&uid).unwrap().len(), 1);
        assert_eq!(*hub.dt_sessions.get(DEVICE_TYPE_IOS).unwrap(), 1);
        assert_eq!(*hub.dt_sessions.get(DEVICE_TYPE_ANDROID).unwrap(), 0);
        assert_eq!(hub.encoders.contains(&Encoder::new(NULLENCODER_NAME)), true);
        assert_eq!(hub.encoders.contains(&Encoder::new(ZSTD_NAME)), false);

        let uid = 1;
        let sid = "abcde";
        let msg = SessionRegMsg {
            uid,
            sid: sid.to_string(),
            did: "".to_string(),
            ip: None,
            encoder: Encoder::new(ZSTD_NAME),
            dt: DEVICE_TYPE_ANDROID,
            addr: None,
        };
        hub.handle_session_register_msg(msg);
        assert_eq!(hub.sessions.len(), 2);
        assert_eq!(hub.sessions.get(sid).is_some(), true);
        assert_eq!(hub.uid_sessions.get(&uid).unwrap().len(), 2);
        assert_eq!(*hub.dt_sessions.get(DEVICE_TYPE_ANDROID).unwrap(), 1);
        assert_eq!(hub.encoders.contains(&Encoder::new(ZSTD_NAME)), true);

        let uid = 2;
        let sid = "abcdef";
        let msg = SessionRegMsg {
            uid,
            sid: sid.to_string(),
            did: "".to_string(),
            ip: None,
            encoder: Encoder::new(ZSTD_NAME),
            dt: DEVICE_TYPE_ANDROID,
            addr: None,
        };
        hub.handle_session_register_msg(msg);
        assert_eq!(hub.sessions.len(), 3);
        assert_eq!(hub.sessions.get(sid).is_some(), true);
        assert_eq!(hub.uid_sessions.get(&uid).unwrap().len(), 1);
        assert_eq!(hub.uid_sessions.len(), 2);
        assert_eq!(*hub.dt_sessions.get(DEVICE_TYPE_ANDROID).unwrap(), 2);
        // encoders 不会重复膨胀
        assert_eq!(hub.encoders.len(), 2);

        let msg = SessionUnregMsg {
            uid,
            sid: sid.to_string(),
            did: "".to_string(),
            ip: None,
            dt: DEVICE_TYPE_ANDROID,
        };
        hub.handle_session_unregister_msg(msg);
        assert_eq!(hub.sessions.len(), 2);
        assert_eq!(hub.sessions.get(sid).is_some(), false);
        assert_eq!(hub.uid_sessions.get(&uid).is_some(), false);
        assert_eq!(hub.uid_sessions.len(), 1);
        assert_eq!(*hub.dt_sessions.get(DEVICE_TYPE_ANDROID).unwrap(), 1);
        assert_eq!(hub.encoders.len(), 2);

        let uid = 1;
        let sid = "abcde";
        let msg = SessionUnregMsg {
            sid: sid.to_string(),
            did: "".to_string(),
            ip: None,
            dt: DEVICE_TYPE_ANDROID,
            uid,
        };
        hub.handle_session_unregister_msg(msg);
        assert_eq!(hub.sessions.len(), 1);
        assert_eq!(hub.sessions.get(sid).is_some(), false);
        assert_eq!(hub.uid_sessions.get(&uid).unwrap().len(), 1);
        assert_eq!(*hub.dt_sessions.get(DEVICE_TYPE_ANDROID).unwrap(), 0);
    }

    #[test]
    fn test_reg_room_with_nonreg_session() {
        let mut hub = Hub::new(1);
        let sid = "abc";
        let channel = "channel_1";
        let msg = HubJoinChannelMsg {
            sid: sid.to_string(),
            channel: channel.to_string(),
            rooms: vec!["room_1".to_string()],
            multi_rooms: false,
        };
        hub.handle_join_channel_msg(msg);
        // room not register
        assert_eq!(hub.rooms.len(), 0);
        assert_eq!(hub.channels.len(), 0);
    }

    #[test]
    fn test_reg_room_with_reg_session() {
        // construct data
        let mut hub = Hub::new(1);
        let sid = "abc";
        let uid = 1;
        let reg_session_msg = SessionRegMsg {
            uid,
            sid: sid.to_string(),
            did: "".to_string(),
            ip: None,
            encoder: Encoder::new(NULLENCODER_NAME),
            dt: DEVICE_TYPE_IOS,
            addr: None,
        };

        let unreg_session_msg = SessionUnregMsg {
            uid,
            sid: sid.to_string(),
            did: "".to_string(),
            ip: None,
            dt: DEVICE_TYPE_IOS,
        };

        let channel = "channel_1";
        let room = "room_1";
        let join_channel_msg = HubJoinChannelMsg {
            sid: sid.to_string(),
            channel: channel.to_string(),
            rooms: vec![room.to_string()],
            multi_rooms: false,
        };

        let leave_room_msg = HubLeaveRoomMsg {
            uid: None,
            sid: Some(sid.to_string()),
            channel: channel.to_string(),
            room: room.to_string(),
        };

        hub.handle_session_register_msg(reg_session_msg.clone());
        hub.handle_join_channel_msg(join_channel_msg.clone());
        assert_eq!(hub.rooms.len(), 1);
        assert_eq!(hub.channels.len(), 1);
        assert_eq!(
            hub.channels.get(channel).unwrap().get(sid).unwrap().uid,
            uid
        );
        let room_id = ChannelRoom::gen_room_id(channel, room).unwrap();
        assert_eq!(hub.rooms.get(&room_id).unwrap().get(sid).unwrap().uid, uid);
        assert_eq!(hub.get_session_rooms(sid).unwrap().len(), 1);
        assert!(hub.get_session_rooms(sid).unwrap().contains(&room_id));
        assert_eq!(hub.get_session_channels(sid).unwrap().len(), 1);
        assert!(hub.get_session_channels(sid).unwrap().contains(channel));

        // session unreg, clear room & channel resource
        hub.handle_session_unregister_msg(unreg_session_msg.clone());
        assert_eq!(hub.rooms.len(), 0);
        assert_eq!(hub.channels.len(), 0);
        assert_eq!(hub.get_session_rooms(sid), None);
        assert_eq!(hub.get_session_channels(sid), None);

        // session leave room
        hub.handle_session_register_msg(reg_session_msg.clone());
        hub.handle_join_channel_msg(join_channel_msg.clone());
        hub.handle_leave_room_msg(leave_room_msg.clone());
        assert_eq!(hub.rooms.len(), 0);
        assert_eq!(hub.channels.len(), 1);
        assert_eq!(hub.get_session_rooms(sid), None);
        // already in channel
        assert_eq!(hub.channels.get(channel).unwrap().get(sid).is_some(), true);
        // unreg session
        hub.handle_session_unregister_msg(unreg_session_msg.clone());
        assert_eq!(hub.channels.len(), 0);
    }

    #[test]
    fn test_multi_rooms() {
        // no multiroom, auto-leave room
    }
}
