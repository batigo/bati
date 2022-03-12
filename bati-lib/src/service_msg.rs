use crate::{gen_msg_id, get_now_milli};
use serde::{Deserialize, Serialize};
use std::fmt;

pub type ServiceMsgType = u8;

pub const SERVICE_MSG_TYPE_CONN: ServiceMsgType = 1;
pub const SERVICE_MSG_TYPE_SERVICE: ServiceMsgType = 2;
pub const SERVICE_MSG_TYPE_BROADCAST: ServiceMsgType = 3;
pub const SERVICE_MSG_TYPE_ROOM_USERS: ServiceMsgType = 4;
pub const SERVICE_MSG_TYPE_REG_SERVICE: ServiceMsgType = 5;
pub const SERVICE_MSG_TYPE_UNREG_ROOM: ServiceMsgType = 6;
pub const SERVICE_MSG_TYPE_UNREG_SERVICE: ServiceMsgType = 7;

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct ServiceMsg {
    pub id: String,
    #[serde(rename = "t")]
    pub typ: ServiceMsgType,
    #[serde(rename = "d")]
    pub data: Option<Box<serde_json::value::RawValue>>,
    pub cid: Option<String>,
    pub uid: Option<String>,
    pub ip: Option<String>,
    pub uids: Option<Vec<u64>>,
    #[serde(rename = "sid")]
    pub service: Option<String>,
    #[serde(rename = "rid")]
    pub room: Option<String>,
    pub broadcast_rate: Option<i8>,
    pub exclude_uids: Option<Vec<String>>,
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
            "id: {}, type: {}, cid: {:?}, uid: {:?}, uids: {:?}, cid: {:?}, rid: {:?}, \
            broadcast_rate: {:?}, exclude_mids: {:?}, include_mids: {:?}, ts:{:?}, data: {:?}",
            self.id,
            self.typ,
            self.cid,
            self.uid,
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

pub type BatiMsgType = u8;
pub const BATI_MSG_TYPE_BIZ: BatiMsgType = 1;
pub const BATI_MSG_TYPE_CONN_QUIT: BatiMsgType = 2;
pub const BATI_MSG_TYPE_CONN_JOIN: BatiMsgType = 3;

#[derive(Serialize, Default, Clone, Debug)]
#[serde(default)]
pub struct BatiMsg {
    pub id: String,
    #[serde(rename = "t")]
    pub typ: BatiMsgType,
    #[serde(rename = "d")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<serde_json::value::RawValue>>,
    pub cid: String,
    pub uid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    pub ts: u64,
}

impl fmt::Display for BatiMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "id: {}, cid:{}, uid: {:?}, ip: {:?}, ts:{:?}, data: {:?}",
            self.id, self.cid, self.uid, self.ip, self.ts, self.data
        )
    }
}

impl BatiMsg {
    pub fn new(
        typ: BatiMsgType,
        cid: String,
        uid: String,
        ip: Option<String>,
        data: Option<Box<serde_json::value::RawValue>>,
    ) -> Self {
        BatiMsg {
            typ,
            cid,
            uid,
            ip,
            data,
            id: gen_msg_id(),
            ts: get_now_milli(),
        }
    }
}
