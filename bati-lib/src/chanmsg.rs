use crate::{gen_msg_id, get_now_milli};
use serde::{Deserialize, Serialize};
use std::fmt;

pub type ChannelMsgType = u8;

pub const CHAN_MSG_TYPE_SESSION: ChannelMsgType = 1;
pub const CHAN_MSG_TYPE_CHANNEL: ChannelMsgType = 2;
pub const CHAN_MSG_TYPE_BROADCAST: ChannelMsgType = 3;
pub const CHAN_MSG_TYPE_ROOM_USERS: ChannelMsgType = 10;
pub const CHAN_MSG_TYPE_REG_CHANNEL: ChannelMsgType = 4;
pub const CHAN_MSG_TYPE_UNREG_ROOM: ChannelMsgType = 5;
pub const CHAN_MSG_TYPE_UNREG_CHANNEL: ChannelMsgType = 6;
pub const CHAN_MSG_TYPE_CLIENT_QUIT: ChannelMsgType = 7;
pub const CHAN_MSG_TYPE_CLIENT_JOIN: ChannelMsgType = 8;

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct ChannelMsg {
    pub id: String,
    #[serde(rename = "t")]
    pub typ: ChannelMsgType,
    #[serde(rename = "d")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<serde_json::value::RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uids: Option<Vec<u64>>,
    #[serde(rename = "cid", skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(rename = "rid", skip_serializing_if = "Option::is_none")]
    pub room: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broadcast_rate: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_mids: Option<Vec<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_mids: Option<Vec<u64>>,
    pub ts: u64,
}

impl ChannelMsg {
    pub fn new() -> Self {
        ChannelMsg {
            id: gen_msg_id(),
            ts: get_now_milli(),
            ..Default::default()
        }
    }
}

impl fmt::Display for ChannelMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "id: {}, type: {}, sid: {:?}, mid: {:?}, ip: {:?}, mids: {:?}, cid: {:?}, rid: {:?}, \
            broadcast_rate: {:?}, exclude_mids: {:?}, include_mids: {:?}, ts:{:?}, data: {:?}",
            self.id,
            self.typ,
            self.sid,
            self.uid,
            self.ip,
            self.uids,
            self.channel,
            self.room,
            self.broadcast_rate,
            self.exclude_mids,
            self.include_mids,
            self.ts,
            self.data
        )
    }
}

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ChannelData {
    pub rids: Vec<String>,
}
