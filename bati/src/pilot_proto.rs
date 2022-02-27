use crate::hub_proto::HubSender;
use futures::{channel::mpsc, SinkExt, StreamExt};

use bati_lib::{ChannelConf, PostmanMsg};

type SendResult = Result<(), mpsc::SendError>;

pub enum PilotMessage {
    FromSession(Session2PilotMsg),
    FromHub(Hub2PilotMsg),
    FromMaster(PilotAddHubMsg),
    FromChanFinder(ChannelConf),
    FromPostman(PostmanMsg),
}

#[derive(Clone)]
pub struct PilotSender(mpsc::Sender<PilotMessage>);

impl PilotSender {
    async fn send(&mut self, msg: PilotMessage) -> SendResult {
        self.0.send(msg).await
    }

    pub async fn send_session_msg(&mut self, msg: Session2PilotMsg) -> SendResult {
        self.send(PilotMessage::FromSession(msg)).await
    }

    pub async fn send_hub_msg(&mut self, msg: Hub2PilotMsg) -> SendResult {
        self.send(PilotMessage::FromHub(msg)).await
    }

    pub async fn send_master_msg(&mut self, msg: PilotAddHubMsg) -> SendResult {
        self.send(PilotMessage::FromMaster(msg)).await
    }

    pub async fn send_chanfinder_msg(&mut self, msg: ChannelConf) -> SendResult {
        self.send(PilotMessage::FromChanFinder(msg)).await
    }

    pub async fn send_postman_msg(&mut self, msg: PostmanMsg) -> SendResult {
        self.send(PilotMessage::FromPostman(msg)).await
    }
}

type RecvResult = Option<PilotMessage>;

pub struct PilotReceiver(mpsc::Receiver<PilotMessage>);

impl PilotReceiver {
    pub async fn next(&mut self) -> RecvResult {
        self.0.next().await
    }
}

pub fn new_pilot_channel(buffer: usize) -> (PilotSender, PilotReceiver) {
    let (tx, rx) = mpsc::channel(buffer);
    (PilotSender(tx), PilotReceiver(rx))
}

pub struct PilotAddHubMsg {
    pub hub: HubSender,
    pub ix: usize,
}

// recv from hub
#[derive(Clone, Debug)]
pub enum Hub2PilotMsg {
    ChannelMsg(PilotChannelMsg),
    EncodingMsg(&'static str),
}

// recv from session
pub type Session2PilotMsg = PilotChannelMsg;

#[derive(Debug, Clone)]
pub struct PilotChannelMsg {
    pub channel: String,
    pub data: bytes::Bytes,
}
