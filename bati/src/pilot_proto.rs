use crate::encoding::*;
use crate::hub_proto::HubSender;
use bati_lib::{ServiceConf, ServiceMsg};
use futures::{channel::mpsc, SinkExt, StreamExt};
use ntex::util::Bytes;
use std::collections::*;

type SendResult = Result<(), mpsc::SendError>;

pub enum PilotMessage {
    FromConn(PilotServiceBizMsg),
    FromHub(Hub2PilotMsg),
    FromMaster(PilotAddHubMsg),
    FromServiceFinder(ServiceConf),
    FromPostman(ServiceMsg),
    FromTester(PilotQueryMsg),
}

#[derive(Clone)]
pub struct PilotSender(mpsc::Sender<PilotMessage>);

impl PilotSender {
    async fn send(&mut self, msg: PilotMessage) -> SendResult {
        self.0.send(msg).await
    }

    pub async fn send_conn_msg(&mut self, msg: PilotServiceBizMsg) -> SendResult {
        self.send(PilotMessage::FromConn(msg)).await
    }

    pub async fn send_hub_msg(&mut self, msg: Hub2PilotMsg) -> SendResult {
        self.send(PilotMessage::FromHub(msg)).await
    }

    pub async fn send_master_msg(&mut self, msg: PilotAddHubMsg) -> SendResult {
        self.send(PilotMessage::FromMaster(msg)).await
    }

    pub async fn send_servicefinder_msg(&mut self, msg: ServiceConf) -> SendResult {
        self.send(PilotMessage::FromServiceFinder(msg)).await
    }

    pub async fn send_postman_msg(&mut self, msg: ServiceMsg) -> SendResult {
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

#[derive(Clone, Debug)]
pub enum Hub2PilotMsg {
    BizMsg(PilotServiceBizMsg),
    EncodingMsg(&'static str),
}

#[derive(Debug, Clone)]
pub struct PilotServiceBizMsg {
    pub service: String,
    pub data: Bytes,
}

// only for test
pub struct PilotQueryMsg {
    pub sender: mpsc::Sender<PilotQueryData>,
}

pub struct PilotQueryData {
    pub hubs: Vec<Option<HubSender>>,
    pub postmen: HashMap<String, ServiceConf>,
    pub encoders: Vec<Encoder>,
}
