use crate::const_proto::*;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use ntex::util::Bytes;
use ntex::web::ws::Frame;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fmt;

pub enum ConnMsg {
    FromMaster(Master2ConnMsg),
    FromHub(Hub2ConnMsg),
    FromTimer(Timer2ConnMsg),
}

#[derive(Debug)]
pub enum Master2ConnMsg {
    Frame(Frame),
    Shutdown,
}

#[derive(Clone)]
pub struct ConnSender {
    pub id: String,
    sender: mpsc::Sender<ConnMsg>,
}

impl fmt::Display for ConnSender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "conn-sender, id: {}", self.id,)
    }
}

impl ConnSender {
    async fn send(&self, msg: ConnMsg) -> SendResult {
        self.sender.clone().send(msg).await
    }

    pub async fn send_master_msg(&self, msg: Master2ConnMsg) -> SendResult {
        self.send(ConnMsg::FromMaster(msg)).await
    }

    pub async fn send_hub_msg(&self, msg: Hub2ConnMsg) -> SendResult {
        self.send(ConnMsg::FromHub(msg)).await
    }

    pub async fn send_timer_msg(&self, msg: Timer2ConnMsg) -> SendResult {
        self.send(ConnMsg::FromTimer(msg)).await
    }
}

pub struct ConnReceiver {
    id: String,
    receiver: mpsc::Receiver<ConnMsg>,
}

impl ConnReceiver {
    pub async fn next(&mut self) -> Option<ConnMsg> {
        self.receiver.next().await
    }
}

impl fmt::Display for ConnReceiver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "conn-receiver, id: {}", self.id,)
    }
}

pub fn new_conn_channel(id: &str, buffer: usize) -> (ConnSender, ConnReceiver) {
    let (tx, rx) = mpsc::channel(buffer);
    (
        ConnSender {
            sender: tx,
            id: id.to_string(),
        },
        ConnReceiver {
            receiver: rx,
            id: id.to_string(),
        },
    )
}

// msg recv from Hub
#[derive(Clone, Debug)]
pub enum Hub2ConnMsg {
    QUIT,
    BIZ(Bytes),
}

#[derive(Debug, Clone)]
pub enum Timer2ConnMsg {
    HearBeatCheck,
}
