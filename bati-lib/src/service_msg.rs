use crate::{gen_msg_id, get_now_milli};
use serde::{Deserialize, Serialize};
use std::fmt;

pub type ServiceMsgType = u8;

pub const SERVICE_MSG_TYPE_CONN_JOIN: ServiceMsgType = 1;
pub const SERVICE_MSG_TYPE_CONN_QUIT: ServiceMsgType = 2;
pub const SERVICE_MSG_TYPE_BIZ: ServiceMsgType = 3;

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct ServiceMsg {
    pub id: String,
    pub service: String,
    #[serde(rename = "type")]
    pub typ: ServiceMsgType,
    pub join_data: Option<JoinData>,
    pub quit_data: Option<QuitData>,
    pub biz_data: Option<BizData>,
    pub ts: u64,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct JoinData {
    pub cid: Option<String>,
    pub uid: Option<String>,
    pub join_service: bool,
    pub rids: Option<Vec<String>>,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct QuitData {
    pub cid: Option<String>,
    pub uid: Option<String>,
    pub quit_service: bool,
    pub rids: Option<Vec<String>>,
}

type BizMsgType = u8;

pub const BIZ_MSG_TYPE_USERS: BizMsgType = 1;
pub const BIZ_MSG_TYPE_ROOM: BizMsgType = 2;
pub const BIZ_MSG_TYPE_SERVICE: BizMsgType = 3;
pub const BIZ_MSG_TYPE_ALL: BizMsgType = 4;

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct BizData {
    #[serde(rename = "type")]
    pub typ: BizMsgType,
    pub cids: Option<Vec<String>>,
    pub uids: Option<Vec<String>>,
    pub rid: Option<String>,
    pub broadcast_ratio: Option<u8>,
    pub black_uids: Option<Vec<String>>,
    pub white_uids: Option<Vec<String>>,
    pub data: Option<Box<serde_json::value::RawValue>>,
}

impl fmt::Display for ServiceMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "id: {}", self.id,)
    }
}

impl ServiceMsg {
    pub fn valiate(&self) -> Result<(), &'static str> {
        match self.typ {
            SERVICE_MSG_TYPE_CONN_JOIN => {
                if self.join_data.is_none() {
                    return Err("join_data missing");
                }
                let data = self.join_data.as_ref().unwrap();
                if data.cid.is_none() && data.uid.is_none() {
                    return Err("both cid & uid missing");
                }
            }
            SERVICE_MSG_TYPE_CONN_QUIT => {
                if self.quit_data.is_none() {
                    return Err("join_data missing");
                }
                let data = self.quit_data.as_ref().unwrap();
                if data.cid.is_none() && data.uid.is_none() {
                    return Err("both cid & uid missing");
                }
            }
            SERVICE_MSG_TYPE_BIZ => {
                if self.biz_data.is_none() {
                    return Err("join_data missing");
                }
                let data = self.biz_data.as_ref().unwrap();
                match data.typ {
                    BIZ_MSG_TYPE_USERS => {
                        if data.cids.is_none() || data.uids.is_none() {
                            return Err("both cids && uids missing in users biz msg");
                        }
                    }
                    BIZ_MSG_TYPE_ROOM => {
                        if data.rid.is_none() {
                            return Err("rid missing in room biz msg");
                        }
                    }
                    BIZ_MSG_TYPE_SERVICE | BIZ_MSG_TYPE_ALL => {}
                    _ => {
                        return Err("unknown service type");
                    }
                }
            }
            _ => {
                return Err("unknown service type");
            }
        }
        Ok(())
    }
}

pub type BatiMsgType = u8;
pub const BATI_MSG_TYPE_BIZ: BatiMsgType = 1;
pub const BATI_MSG_TYPE_CONN_QUIT: BatiMsgType = 2;

#[derive(Serialize, Default, Clone, Debug)]
#[serde(default)]
pub struct BatiMsg {
    pub id: String,
    #[serde(rename = "type")]
    pub typ: BatiMsgType,
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
