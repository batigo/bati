use crate::const_proto::*;
use crate::metric_proto::MetricTimerMsg::Collect;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;

#[derive(Debug)]
pub enum MetricMessage {
    FromHub(HubMetricMsg),
    FromTimer(MetricTimerMsg),
}

#[derive(Debug)]
pub struct HubMetricMsg {
    pub ix: usize,
    pub dt_sessions: HashMap<DeviceType, u64>,
    pub channel_sessions: HashMap<String, u64>,
}

#[derive(Debug)]
pub enum MetricTimerMsg {
    Collect,
}

#[derive(Clone)]
pub struct MetricSender(mpsc::Sender<MetricMessage>);

impl MetricSender {
    async fn send(&mut self, msg: MetricMessage) -> SendResult {
        self.0.send(msg).await
    }

    pub async fn send_timer_collect_msg(&mut self) -> SendResult {
        self.send(MetricMessage::FromTimer(Collect)).await
    }

    pub async fn send_hub_msg(&mut self, msg: HubMetricMsg) -> SendResult {
        self.send(MetricMessage::FromHub(msg)).await
    }
}

pub struct MetricReceiver(mpsc::Receiver<MetricMessage>);

impl MetricReceiver {
    pub async fn next(&mut self) -> Option<MetricMessage> {
        self.0.next().await
    }
}

pub fn new_metric_channel(buffer: usize) -> (MetricSender, MetricReceiver) {
    let (tx, rx) = mpsc::channel(buffer);
    (MetricSender(tx), MetricReceiver(rx))
}
