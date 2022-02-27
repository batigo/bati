use crate::const_proto::*;
use crate::encoding::*;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use ntex::util::Bytes;
use ntex::web::ws::Frame;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fmt;

// 长连接初始化消息
pub const CMSG_TYPE_INIT: u8 = 1;
// 长连接初始化消息响应
pub const CMSG_TYPE_INIT_RESP: u8 = 2;
// 业务消息
pub const CMSG_TYPE_BIZ: u8 = 3;
// ack消息
pub const CMSG_TYPE_ACK: u8 = 4;
// echo消息，用于测试
pub const CMSG_TYPE_ECHO: u8 = 102;

#[derive(Deserialize, Serialize, Clone, Copy, Default)]
pub struct CMsgType(pub u8);

impl fmt::Display for CMsgType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.str())
    }
}

impl CMsgType {
    pub fn str(&self) -> &'static str {
        match self.0 {
            CMSG_TYPE_INIT => "init",
            CMSG_TYPE_INIT_RESP => "init_resp",
            CMSG_TYPE_BIZ => "biz",
            CMSG_TYPE_ACK => "ack",
            CMSG_TYPE_ECHO => "echo",
            _ => "unknown",
        }
    }

    fn is_valid(&self) -> bool {
        matches!(
            self.0,
            CMSG_TYPE_INIT | CMSG_TYPE_INIT_RESP | CMSG_TYPE_BIZ | CMSG_TYPE_ACK | CMSG_TYPE_ECHO
        )
    }

    pub fn is_type(&self, t: u8) -> bool {
        self.0 == t
    }

    fn datamust(&self) -> bool {
        matches!(self.0, CMSG_TYPE_INIT | CMSG_TYPE_BIZ)
    }
}

#[derive(Serialize, Default)]
#[serde(default)]
pub struct Conn2ClientMsg {
    pub id: String,
    #[serde(rename = "t")]
    pub typ: CMsgType,
    pub ack: u8,
    #[serde(rename = "cid", skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<RawValue>>,
}

pub enum ConnMsg {
    FromMaster(Master2ConnMsg),
    FromHub(Hub2ConnMsg),
    FromTimer(Timer2ConnMsg),
}

#[derive(Debug)]
pub enum Master2ConnMsg {
    Frame(Frame),
    Shutdown,
}

#[derive(Clone)]
pub struct ConnSender {
    pub id: String,
    sender: mpsc::Sender<ConnMsg>,
}

impl fmt::Display for ConnSender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "conn-sender, id: {}", self.id,)
    }
}

impl ConnSender {
    async fn send(&self, msg: ConnMsg) -> SendResult {
        self.sender.clone().send(msg).await
    }

    pub async fn send_master_msg(&self, msg: Master2ConnMsg) -> SendResult {
        self.send(ConnMsg::FromMaster(msg)).await
    }

    pub async fn send_hub_msg(&self, msg: Hub2ConnMsg) -> SendResult {
        self.send(ConnMsg::FromHub(msg)).await
    }

    pub async fn send_timer_msg(&self, msg: Timer2ConnMsg) -> SendResult {
        self.send(ConnMsg::FromTimer(msg)).await
    }
}

pub struct ConnReceiver {
    id: String,
    receiver: mpsc::Receiver<ConnMsg>,
}

impl ConnReceiver {
    pub async fn next(&mut self) -> Option<ConnMsg> {
        self.receiver.next().await
    }
}

impl fmt::Display for ConnReceiver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "conn-receiver, id: {}", self.id,)
    }
}

pub fn new_conn_channel(id: &str, buffer: usize) -> (ConnSender, ConnReceiver) {
    let (tx, rx) = mpsc::channel(buffer);
    (
        ConnSender {
            sender: tx,
            id: id.to_string(),
        },
        ConnReceiver {
            receiver: rx,
            id: id.to_string(),
        },
    )
}

// msg client -> conn
#[derive(Deserialize, Serialize, Default)]
#[serde(default)]
pub struct ClientMsg {
    pub id: String,
    #[serde(rename = "t")]
    pub typ: CMsgType,
    pub ack: u8,
    #[serde(rename = "cid", skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<RawValue>>,
}

impl ClientMsg {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !self.typ.is_valid() {
            return Err("unknown msg type");
        }

        if self.typ.datamust() && self.data.is_none() {
            return Err("msg data misss");
        }

        if self.typ.is_type(CMSG_TYPE_BIZ) && self.channel_id.is_none() {
            return Err("empty cid for bizmsg");
        }

        Ok(())
    }

    pub fn gen_bytes_with_encoder(
        &self,
        encoder: Option<Encoder>,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        match serde_json::to_vec(self) {
            Err(e) => Err(Box::new(e)),
            Ok(v) => match encoder {
                Some(ref e) if e.name() != NULLENCODER_NAME => match e.encode(v.as_slice()) {
                    Err(e) => Err(Box::new(e)),
                    Ok(v) => Ok(Bytes::from(v)),
                },
                _ => Ok(Bytes::from(v)),
            },
        }
    }

    pub fn new_ack_msg(id: &str) -> Self {
        ClientMsg {
            id: id.to_string(),
            typ: CMsgType(CMSG_TYPE_ACK),
            ..Default::default()
        }
    }
}

impl fmt::Display for ClientMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "id: {}, t: {}, ack: {}, cid: {:?}",
            self.id, self.typ, self.ack, self.channel_id
        )
    }
}

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ConnInitMsgData {
    pub accept_encoding: String,
    pub ping_interval: u32,
}

// msg recv from Hub
#[derive(Clone, Debug)]
pub enum Hub2ConnMsg {
    QUIT,
    BIZ(Bytes),
}

#[derive(Debug, Clone)]
pub enum Timer2ConnMsg {
    HearBeatCheck,
}