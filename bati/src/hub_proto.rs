use crate::conn_proto::ConnSender;
use crate::const_proto::*;
use crate::encoding::*;
use bati_lib as lib;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use ntex::util::Bytes;
use std::collections::*;
use std::fmt;

pub fn new_hub_channel(buffer: usize) -> (HubSender, HubReceiver) {
    let (tx, rx) = mpsc::channel(buffer);
    (HubSender(tx), HubReceiver(rx))
}

#[derive(Clone)]
pub struct HubSender(mpsc::Sender<HubMessage>);

impl HubSender {
    async fn send(&mut self, msg: HubMessage) -> SendResult {
        self.0.send(msg).await
    }

    pub async fn send_conn_msg(&mut self, msg: Conn2HubMsg) -> SendResult {
        self.send(HubMessage::FromConn(msg)).await
    }

    pub async fn send_pilot_msg(&mut self, msg: Pilot2HubMsg) -> SendResult {
        self.send(HubMessage::FromPilot(msg)).await
    }

    pub async fn send_timer_msg(&mut self, msg: Timer2HubMsg) -> SendResult {
        self.send(HubMessage::FromTimer(msg)).await
    }
}

pub struct HubReceiver(mpsc::Receiver<HubMessage>);

impl HubReceiver {
    pub async fn next(&mut self) -> Option<HubMessage> {
        self.0.next().await
    }
}

pub enum HubMessage {
    FromConn(Conn2HubMsg),
    FromPilot(Pilot2HubMsg),
    FromTimer(Timer2HubMsg),
    FromTester(HubDataQueryMsg),
}

// recved from pilot
#[derive(Clone, Debug)]
pub enum Pilot2HubMsg {
    Biz(HubServiceBizMsg),
    JoinService(HubJoinServiceMsg),
    LeaveRoom(HubLeaveRoomMsg),
    ServiceConf(lib::ServiceConf),
}

#[derive(Clone, Default, Debug)]
pub struct HubServiceBizMsg {
    pub id: String,
    pub typ: lib::BatiMsgType,
    pub cids: Option<Vec<String>>,
    pub uids: Option<Vec<String>>,
    pub service: Option<String>,
    pub room: Option<String>,
    pub ratio: Option<u8>,
    pub whites: Option<Vec<String>>,
    pub blacks: Option<Vec<String>>,
    pub data: ServiceBizData,
}

#[derive(Clone, Debug)]
pub struct ServiceBizData {
    raw_data: Bytes,
    encoding_data: Vec<Option<Bytes>>,
}

impl Default for ServiceBizData {
    fn default() -> Self {
        ServiceBizData {
            raw_data: Bytes::from(""),
            encoding_data: vec![None, None],
        }
    }
}

impl ServiceBizData {
    pub fn new(raw_data: Bytes) -> Self {
        ServiceBizData {
            raw_data,
            encoding_data: vec![None, None],
        }
    }

    pub fn insert_data_with_encoder(&mut self, encoder: &Encoder) -> Result<(), String> {
        if encoder.name() == NULLENCODER_NAME {
            return Ok(());
        }

        match encoder.encode(self.raw_data.as_ref()) {
            Ok(data) => {
                let encoder_index = Self::encoder_index(encoder.name());
                self.encoding_data[encoder_index] = Some(Bytes::from(data));
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn get_data_with_encoder(&mut self, encoder: &Encoder) -> Result<Bytes, String> {
        match encoder.name() {
            NULLENCODER_NAME => Ok(self.raw_data.clone()),
            encoding => match self.encoding_data.get(Self::encoder_index(encoding)) {
                Some(Some(data)) => Ok(data.clone()),
                _ => match self.insert_data_with_encoder(encoder) {
                    Err(e) => Err(e),
                    Ok(_) => self.get_data_with_encoder(encoder),
                },
            },
        }
    }

    fn encoder_index(encoding: &'static str) -> usize {
        match encoding {
            ZSTD_NAME => 0,
            _ => 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HubJoinServiceMsg {
    pub cid: Option<String>,
    pub uid: Option<String>,
    pub service: String,
    pub rooms: Option<Vec<String>>,
    pub multi_rooms: bool,
    pub join_service: bool,
}

impl fmt::Display for HubJoinServiceMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HubJoinServiceMsg: cid:{:?}, service:{}, rooms{:?}, multi_rooms:{}",
            self.cid, self.service, self.rooms, self.multi_rooms
        )
    }
}

#[derive(Clone, Debug)]
pub struct HubLeaveRoomMsg {
    pub uid: Option<String>,
    pub cid: Option<String>,
    pub service: String,
    pub rooms: Option<Vec<String>>,
    pub quit_service: bool,
}

impl fmt::Display for HubLeaveRoomMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HubLeaveRoomMsg: sid:{:?}, uid:{:?}, service:{}, rooms: {:?}",
            self.cid, self.uid, self.service, self.rooms
        )
    }
}

// recved from session
#[derive(Clone)]
pub enum Conn2HubMsg {
    Register(ConnRegMsg),
    Unregister(ConnUnregMsg),
}

impl fmt::Display for Conn2HubMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Conn2HubMsg::Register(msg) => {
                write!(f, "Conn2HubMsg::Rgister::{}", msg)
            }
            Conn2HubMsg::Unregister(msg) => {
                write!(f, "Conn2HubMsg::Unregister::{}", msg)
            }
        }
    }
}

#[derive(Clone)]
pub struct ConnRegMsg {
    pub cid: String,
    pub uid: String,
    pub encoder: Encoder,
    pub dt: DeviceType,
    pub addr: ConnSender,
}

impl fmt::Display for ConnRegMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConnRegMsg: sid:{}, ", self.cid)
    }
}

#[derive(Debug, Clone)]
pub struct ConnUnregMsg {
    pub cid: String,
    pub did: String,
    pub uid: String,
    pub dt: DeviceType,
    pub ip: Option<String>,
}

impl fmt::Display for ConnUnregMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConnUnregMsg: sid:{}, ", self.cid)
    }
}

#[derive(Debug)]
pub enum Timer2HubMsg {
    MetricStat,
}

// only for test

pub struct HubDataQueryMsg {
    pub sender: mpsc::Sender<HubDataQueryData>,
}

pub struct HubDataQueryData {
    pub conns: HashMap<String, ConnSender>,
    pub rooms: HashMap<String, HashMap<String, ConnSender>>,
    pub conn_rooms: HashMap<String, HashSet<String>>,
    pub services: HashMap<String, HashMap<String, ConnSender>>,
    pub conn_services: HashMap<String, HashSet<String>>,
    pub uid_conns: HashMap<String, HashSet<String>>,
    pub dt_conns: HashMap<DeviceType, u64>,
    pub service_confs: HashMap<String, lib::ServiceConf>,
    pub encoders: Vec<Encoder>,
}
