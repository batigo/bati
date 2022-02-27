use crate::const_proto::*;
use crate::encoding::*;
use crate::session_proto::SessionSender;
use bati_lib as lib;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use ntex::util::Bytes;
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

    pub async fn send_session_msg(&mut self, msg: Session2HubMsg) -> SendResult {
        self.send(HubMessage::FromSession(msg)).await
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
    FromSession(Session2HubMsg),
    FromPilot(Pilot2HubMsg),
    FromTimer(Timer2HubMsg),
}

// recved from pilot
#[derive(Clone, Debug)]
pub enum Pilot2HubMsg {
    Biz(HubChannelBizMsg),
    JoinChannel(HubJoinChannelMsg),
    LeaveRoom(HubLeaveRoomMsg),
    ChannelConf(lib::ChannelConf),
}

#[derive(Clone, Default, Debug)]
pub struct HubChannelBizMsg {
    pub id: String,
    pub typ: ChannelBizMsgType,
    pub sid: Option<String>,
    pub channel: Option<String>,
    pub room: Option<String>,
    pub mids: Option<Vec<u64>>,
    pub ratio: Option<u8>,
    pub whites: Option<Vec<String>>,
    pub blacks: Option<Vec<String>>,
    pub data: ChannelBizData,
}

#[derive(Copy, Clone, Debug)]
pub enum ChannelBizMsgType {
    Session,
    Room,
    Channel,
    Broadcast,
}

impl Default for ChannelBizMsgType {
    fn default() -> Self {
        ChannelBizMsgType::Session
    }
}

#[derive(Clone, Debug)]
pub struct ChannelBizData {
    raw_data: Bytes,
    encoding_data: Vec<Option<Bytes>>,
}

impl Default for ChannelBizData {
    fn default() -> Self {
        ChannelBizData {
            raw_data: Bytes::from(""),
            encoding_data: vec![None, None],
        }
    }
}

impl ChannelBizData {
    pub fn new(raw_data: Bytes) -> Self {
        ChannelBizData {
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
pub struct HubJoinChannelMsg {
    pub sid: String,
    pub channel: String,
    pub rooms: Vec<String>,
    pub multi_rooms: bool,
}

impl fmt::Display for HubJoinChannelMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HubJoinChannelMsg: sid:{:?}, channel:{}, rooms{:?}, multi_rooms:{}",
            self.sid, self.channel, self.rooms, self.multi_rooms
        )
    }
}

#[derive(Clone, Debug)]
pub struct HubLeaveRoomMsg {
    pub uid: Option<String>,
    pub sid: Option<String>,
    pub channel: String,
    pub room: String,
}

impl fmt::Display for HubLeaveRoomMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HubLeaveRoomMsg: sid:{:?}, uid:{:?}, channel:{}, room{}",
            self.sid, self.uid, self.channel, self.room
        )
    }
}

// recved from session
#[derive(Clone)]
pub enum Session2HubMsg {
    Register(SessionRegMsg),
    Unregister(SessionUnregMsg),
}

impl fmt::Display for Session2HubMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Session2HubMsg::Register(msg) => {
                write!(f, "Session2HubMsg::Rgister::{}", msg)
            }
            Session2HubMsg::Unregister(msg) => {
                write!(f, "Session2HubMsg::Unregister::{}", msg)
            }
        }
    }
}

#[derive(Clone)]
pub struct SessionRegMsg {
    pub sid: String,
    pub did: String,
    pub ip: Option<String>,
    pub uid: String,
    pub encoder: Encoder,
    pub dt: DeviceType,
    pub addr: SessionSender,
}

impl fmt::Display for SessionRegMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SessionRegMsg: sid:{}, ", self.sid)
    }
}

#[derive(Debug, Clone)]
pub struct SessionUnregMsg {
    pub sid: String,
    pub did: String,
    pub uid: String,
    pub dt: DeviceType,
    pub ip: Option<String>,
}

impl fmt::Display for SessionUnregMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SessionUnregMsg: sid:{}, ", self.sid)
    }
}

#[derive(Debug)]
pub enum Timer2HubMsg {
    MetricStat,
}
