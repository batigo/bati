use crate::{gen_msg_id, get_now_milli};
use serde::{Deserialize, Serialize};
use std::fmt;

pub type ServiceMsgType = u8;

pub const CHAN_MSG_TYPE_CONN: ServiceMsgType = 1;
pub const CHAN_MSG_TYPE_SERVICE: ServiceMsgType = 2;
pub const CHAN_MSG_TYPE_BROADCAST: ServiceMsgType = 3;
pub const CHAN_MSG_TYPE_ROOM_USERS: ServiceMsgType = 4;
pub const CHAN_MSG_TYPE_REG_CHANNEL: ServiceMsgType = 5;
pub const CHAN_MSG_TYPE_UNREG_ROOM: ServiceMsgType = 6;
pub const CHAN_MSG_TYPE_UNREG_CHANNEL: ServiceMsgType = 7;
pub const CHAN_MSG_TYPE_CONN_QUIT: ServiceMsgType = 8;
pub const CHAN_MSG_TYPE_CONN_JOIN: ServiceMsgType = 9;

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct ServiceMsg {
    pub id: String,
    #[serde(rename = "t")]
    pub typ: ServiceMsgType,
    #[serde(rename = "d")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<serde_json::value::RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uids: Option<Vec<u64>>,
    #[serde(rename = "cid", skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(rename = "rid", skip_serializing_if = "Option::is_none")]
    pub room: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broadcast_rate: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_uids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_uids: Option<Vec<String>>,
    pub ts: u64,
}

impl ServiceMsg {
    pub fn new() -> Self {
        ServiceMsg {
            id: gen_msg_id(),
            ts: get_now_milli(),
            ..Default::default()
        }
    }
}

impl fmt::Display for ServiceMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "id: {}, type: {}, cid: {:?}, uid: {:?}, ip: {:?}, uids: {:?}, cid: {:?}, rid: {:?}, \
            broadcast_rate: {:?}, exclude_mids: {:?}, include_mids: {:?}, ts:{:?}, data: {:?}",
            self.id,
            self.typ,
            self.cid,
            self.uid,
            self.ip,
            self.uids,
            self.service,
            self.room,
            self.broadcast_rate,
            self.exclude_uids,
            self.include_uids,
            self.ts,
            self.data
        )
    }
}

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ServiceData {
    pub rids: Vec<String>,
}