pub mod chanmsg;
mod channel;
mod chanregistry;
mod kafka;
mod postman;
use std::time::{SystemTime, UNIX_EPOCH};

pub use self::{chanmsg::*, channel::*, chanregistry::*, postman::*};
use std::convert::TryInto;

pub fn gen_msg_id() -> String {
    format!(
        "{:x}",
        md5::compute(&uuid::Uuid::new_v4().to_string().as_bytes())
    )
}

#[derive(Clone, Debug)]
pub struct PostmanMsg {
    pub data: bytes::Bytes,
    pub channel: Option<String>,
}

pub fn get_now_milli() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(ts) => ts.as_millis().try_into().unwrap_or(0),
        _ => 0,
    }
}
