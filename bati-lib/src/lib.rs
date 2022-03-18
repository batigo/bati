mod kafka;
mod postman;
mod service;
// pub mod service_msg;
mod service_registry;
pub mod smsg;

use std::time::{SystemTime, UNIX_EPOCH};

pub use self::smsg::bati_msg::BatiMsgType;
pub use self::smsg::biz_data::BizMsgType;
pub use self::smsg::service_msg::ServiceMsgType;
pub use self::{postman::*, service::*, service_registry::*, smsg::*};

use std::convert::TryInto;

pub fn gen_msg_id() -> String {
    format!(
        "{:x}",
        md5::compute(&uuid::Uuid::new_v4().to_string().as_bytes())
    )
}

pub fn get_now_milli() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(ts) => ts.as_millis().try_into().unwrap_or(0),
        _ => 0,
    }
}
