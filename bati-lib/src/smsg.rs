use log::error;
use crate::{gen_msg_id, get_now_milli};
use prost::Message;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatiMsg {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(enumeration="BatiMsgType", tag="2")]
    pub r#type: i32,
    #[prost(bytes="vec", optional, tag="3")]
    pub data: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(string, tag="4")]
    pub cid: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub uid: ::prost::alloc::string::String,
    #[prost(string, optional, tag="6")]
    pub ip: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, tag="7")]
    pub ts: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ServiceMsg {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub service: ::prost::alloc::string::String,
    #[prost(enumeration="ServiceMsgType", tag="3")]
    pub r#type: i32,
    #[prost(message, optional, tag="4")]
    pub biz_data: ::core::option::Option<BizData>,
    #[prost(message, optional, tag="5")]
    pub join_data: ::core::option::Option<JoinData>,
    #[prost(message, optional, tag="6")]
    pub quit_data: ::core::option::Option<QuitData>,
    #[prost(uint64, tag="7")]
    pub ts: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinData {
    #[prost(string, optional, tag="1")]
    pub cid: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="2")]
    pub uid: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(bool, optional, tag="3")]
    pub join_service: ::core::option::Option<bool>,
    #[prost(string, repeated, tag="4")]
    pub rooms: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QuitData {
    #[prost(string, optional, tag="1")]
    pub cid: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="2")]
    pub uid: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(bool, optional, tag="3")]
    pub quit_service: ::core::option::Option<bool>,
    #[prost(string, repeated, tag="4")]
    pub rooms: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BizData {
    #[prost(enumeration="BizMsgType", tag="1")]
    pub r#type: i32,
    #[prost(string, repeated, tag="2")]
    pub cids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="3")]
    pub uids: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag="4")]
    pub room: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint32, optional, tag="5")]
    pub broadcast_ratio: ::core::option::Option<u32>,
    #[prost(uint32, repeated, tag="6")]
    pub black_uids: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint32, repeated, tag="7")]
    pub white_uids: ::prost::alloc::vec::Vec<u32>,
    #[prost(bytes="vec", optional, tag="8")]
    pub data: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BatiMsgType {
    Unused = 0,
    Biz = 1,
    ConnQuit = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ServiceMsgType {
    Unused = 0,
    ConnJoin = 1,
    ConnQuit = 2,
    Biz = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BizMsgType {
    Unused = 0,
    Users = 1,
    Room = 2,
    Service = 3,
    All = 4,
}


impl std::fmt::Display for ServiceMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "id: {}", self.id,)
    }
}

impl ServiceMsg {
    pub fn valiate(&self) -> Result<(), &'static str> {
        let typ = ServiceMsgType::from_i32(self.r#type);
        if typ.is_none() {
            error!("recv unknown service msg type: {} - {}", self.service, self.r#type);
            return Err("unknown service msg type")
        }
        let typ : ServiceMsgType = typ.unwrap();

        match typ {
           ServiceMsgType::ConnJoin  => {
                if self.join_data.is_none() {
                    return Err("join_data missing");
                }
                let data = self.join_data.as_ref().unwrap();
                if data.cid.is_none() && data.uid.is_none() {
                    return Err("both cid & uid missing");
                }
            }
             ServiceMsgType::ConnQuit => {
                if self.quit_data.is_none() {
                    return Err("join_data missing");
                }
                let data = self.quit_data.as_ref().unwrap();
                if data.cid.is_none() && data.uid.is_none() {
                    return Err("both cid & uid missing");
                }
            }
            ServiceMsgType::Biz => {
                if self.biz_data.is_none() {
                    return Err("join_data missing");
                }
                let data = self.biz_data.as_ref().unwrap();

                let typ = BizMsgType::from_i32(data.r#type);
                if typ.is_none() {
                    error!("recv unknown  biz-msg type: {} - {}", self.service, self.r#type);
                    return Err("unknown biz msg type")
                }
                let typ : BizMsgType = typ.unwrap();

                match typ {
                     BizMsgType::Users=> {
                        if data.cids.is_empty() && data.uids.is_empty() {
                            return Err("both cids && uids missing in users biz msg");
                        }
                    }
                    BizMsgType::Room => {
                        if data.room.is_none() {
                            return Err("rid missing in room biz msg");
                        }
                    }
                    BizMsgType::Service |  BizMsgType::All => {}
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

impl std::fmt::Display for BatiMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "id: {}, cid:{}, uid: {:?}, ip: {:?}, ts:{:?}, data: {:?}",
            self.id, self.cid, self.uid, self.ip, self.ts, self.data
        )
    }
}

impl BatiMsg {
    pub fn new(
        id: Option<String>,
        typ: BatiMsgType,
        cid: String,
        uid: String,
        ip: Option<String>,
        data: Option<Vec<u8>>,
    ) -> Self {
        let mid;
        if let Some(id) = id {
            mid = id
        } else {
            mid = gen_msg_id()
        }
        BatiMsg {
            id: mid,
            cid,
            uid,
            ip,
            data,
            ts: get_now_milli(),
            r#type: typ as i32,
        }
    }
}

pub fn serialize_service_msg(msg: &ServiceMsg) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.reserve(msg.encoded_len());
    msg.encode(&mut buf);
    buf
}

pub fn deserialize_service_message(buf: &[u8]) -> Result<ServiceMsg, prost::DecodeError> {
    ServiceMsg::decode(&mut std::io::Cursor::new(buf))
}

pub fn serialize_bati_msg(msg: &BatiMsg) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.reserve(msg.encoded_len());
    msg.encode(&mut buf);
    buf
}

pub fn deserialize_bati_message(buf: &[u8]) -> Result<BatiMsg, prost::DecodeError> {
    BatiMsg::decode(&mut std::io::Cursor::new(buf))
}