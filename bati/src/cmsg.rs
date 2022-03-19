use crate::encoding::*;
use prost::Message;
use std::io::Cursor;

/// client消息
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClientMsg {
    /// 消息id
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// 消息类型
    #[prost(enumeration = "ClientMsgType", tag = "2")]
    pub r#type: i32,
    /// 本条消息是否需要回ack
    #[prost(bool, tag = "3")]
    pub ack: bool,
    /// 业务消息对应的service id
    #[prost(string, optional, tag = "4")]
    pub service_id: ::core::option::Option<::prost::alloc::string::String>,
    /// biz_data压缩类型
    #[prost(enumeration = "CompressorType", optional, tag = "5")]
    pub compressor: ::core::option::Option<i32>,
    /// 业务消息data, 网关作为消息通道透传，  client <-> service
    #[prost(bytes = "vec", optional, tag = "6")]
    pub biz_data: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    /// 初始化信息
    #[prost(message, optional, tag = "7")]
    pub init_data: ::core::option::Option<InitData>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitData {
    /// biz_data压缩类型
    #[prost(enumeration = "CompressorType", tag = "1")]
    pub accept_compressor: i32,
    /// 长连接心跳间隔，单位秒
    #[prost(uint32, tag = "2")]
    pub ping_interval: u32,
}
/// client消息类型
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ClientMsgType {
    /// 未使用
    Unused = 0,
    /// ws长连接初始化消息 client -> bati
    Init = 1,
    /// ws长简介初始化响应消息 client <- bati
    InitResp = 2,
    /// 业务消息 client <-> bati <-> service
    Biz = 3,
    /// ack消息
    Ack = 4,
    /// echo 消息用于测试 client <-> bati
    Echo = 100,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CompressorType {
    /// 消息biz_data不压缩
    Null = 0,
    /// 使用deflate压缩biz_data
    Deflate = 1,
}

pub fn serialize_cmsg(msg: &ClientMsg) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.reserve(msg.encoded_len());
    msg.encode(&mut buf);
    buf
}

pub fn deserialize_cmsg(buf: &[u8]) -> Result<ClientMsg, prost::DecodeError> {
    ClientMsg::decode(&mut Cursor::new(buf))
}

impl ClientMsg {
    pub fn validate(&self) -> Result<(), &'static str> {
        let msg_typ = ClientMsgType::from_i32(self.r#type);
        if msg_typ.is_none() {
            return Err("unknown msg type");
        }

        let msg_typ = msg_typ.unwrap();
        match msg_typ {
            ClientMsgType::Biz => {
                if self.biz_data.is_none() {
                    return Err("biz data missing for biz msg");
                }
                if self.service_id.is_none() {
                    return Err("service id missing for biz msg");
                }
            }
            ClientMsgType::Init => {
                if self.init_data.is_none() {
                    return Err("init data missing for init msg");
                }
            }
            _ => {}
        }

        if let Some(compressor) = self.compressor {
            if !CompressorType::is_valid(compressor) {
                return Err("unknown compressor");
            }
        }

        Ok(())
    }

    pub fn take_biz_data(&mut self) -> Result<Vec<u8>, String> {
        let data = self.biz_data.take();
        if data.is_none() {
            panic!("biz data mission");
        }

        let data = data.unwrap();
        if self.compressor.is_none() {
            return Ok(data);
        }

        let compressor = CompressorType::new_compressor(self.compressor.take().unwrap());
        let data = compressor.decode(data.as_slice());
        if data.is_err() {
            return Err(data.err().unwrap().to_string());
        }

        Ok(data.unwrap())
    }

    pub fn new_ack_msg(id: &str) -> Self {
        ClientMsg {
            id: id.to_string(),
            r#type: ClientMsgType::Ack as i32,
            ..Default::default()
        }
    }
}

impl ClientMsgType {
    pub fn str(&self) -> &'static str {
        match *self {
            ClientMsgType::Init => "init",
            ClientMsgType::InitResp => "init_resp",
            ClientMsgType::Biz => "biz",
            ClientMsgType::Ack => "ack",
            ClientMsgType::Echo => "echo",
            _ => "unknown",
        }
    }

    pub fn is_type(&self, t: ClientMsgType) -> bool {
        *self == t
    }

    pub fn from_must(typ: i32) -> Self {
        Self::from_i32(typ).unwrap()
    }
}

impl CompressorType {
    pub fn new_compressor(compressor: i32) -> Encoder {
        let name = match CompressorType::from_i32(compressor).unwrap_or(Self::Null) {
            Self::Null => NULLENCODER_NAME,
            Self::Deflate => DEFLATE_NAME,
            // Self::Zstd => ZSTD_NAME,
        };
        Encoder::new(name)
    }
}
