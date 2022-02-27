use futures::channel::mpsc;

pub type DeviceType = &'static str;
pub const DEVICE_TYPE_IOS: DeviceType = "ios";
pub const DEVICE_TYPE_ANDROID: DeviceType = "android";
pub const DEVICE_TYPE_WEB: DeviceType = "web";
pub const DEVICE_TYPE_X: DeviceType = "x";

pub fn trans_dt(dt: u8) -> DeviceType {
    match dt {
        0 => DEVICE_TYPE_ANDROID,
        1 => DEVICE_TYPE_IOS,
        3 => DEVICE_TYPE_WEB,
        _ => DEVICE_TYPE_X,
    }
}

pub type SendResult = Result<(), mpsc::SendError>;
