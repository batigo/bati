use crate::encoding::*;
use prost::Message;
use std::io::Cursor;

//option go_package = "github.com/batigo/cmsg";

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClientMsg {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(enumeration="ClientMsgType", tag="2")]
    pub r#type: i32,
    #[prost(int32, tag="3")]
    pub ack: i32,
    #[prost(string, optional, tag="4")]
    pub service_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(enumeration="CompressorType", optional, tag="5")]
    pub compressor: ::core::option::Option<i32>,
    #[prost(bytes="vec", optional, tag="6")]
    pub biz_data: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(message, optional, tag="7")]
    pub init_data: ::core::option::Option<InitData>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitData {
    #[prost(enumeration="CompressorType", tag="1")]
    pub accept_compressor: i32,
    #[prost(uint32, tag="2")]
    pub ping_interval: u32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ClientMsgType {
    Unused = 0,
    Init = 1,
    InitResp = 2,
    Biz = 3,
    Ack = 4,
    Echo = 100,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CompressorType {
    Null = 0,
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
        let name = match CompressorType::from_i32(compressor).unwrap() {
            Self::Null => NULLENCODER_NAME,
            Self::Deflate => DEFLATE_NAME,
            // Self::Zstd => ZSTD_NAME,
        };
        Encoder::new(name)
    }
}
