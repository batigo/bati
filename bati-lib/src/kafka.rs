use crate::{PostmanMsg, ServiceMsg};
use futures::channel::mpsc::{Receiver, Sender};
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use ntex::{rt, time};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{stream_consumer::StreamConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{Message, ToBytes};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use std::error::Error;
use std::time::Duration;

pub struct KafkaPostman {
    brokers: String,
    topic: String,
    service: String,
    group: String,
    msg_sender: Option<Sender<PostmanMsg>>,
    msg_receiver: Option<Receiver<PostmanMsg>>,
}

impl KafkaPostman {
    pub fn new(
        brokers: String,
        topic: String,
        channel: String,
        group: String,
        sender: Option<Sender<PostmanMsg>>,
        receiver: Option<Receiver<PostmanMsg>>,
    ) -> Self {
        KafkaPostman {
            brokers,
            topic,
            service: channel,
            group,
            msg_sender: sender,
            msg_receiver: receiver,
        }
    }

    pub fn run(&mut self) -> Result<(), Box<dyn Error>> {
        if self.msg_sender.is_some() {
            return match self.new_consumer() {
                Ok(consumer) => {
                    let sender = self.msg_sender.take().unwrap();
                    let topic = self.topic.clone();
                    let channel = self.service.clone();
                    rt::spawn(async move {
                        run_consumer(topic, channel, sender, consumer).await;
                    });
                    Ok(())
                }
                Err(e) => {
                    error!("failed to run kafka consumer: {}", e);
                    Err(Box::new(e))
                }
            };
        }

        if self.msg_receiver.is_some() {
            return match self.new_producer() {
                Ok(producer) => {
                    let receiver = self.msg_receiver.take().unwrap();
                    let topic = self.topic.clone();
                    rt::spawn(async move {
                        run_producer(topic, receiver, producer).await;
                    });
                    Ok(())
                }
                Err(e) => Err(Box::new(e)),
            };
        }

        Ok(())
    }

    fn new_consumer(&self) -> Result<LoggingConsumer, KafkaError> {
        let topics = vec![self.topic.as_str()];

        let consumer: LoggingConsumer = ClientConfig::new()
            .set("group.id", &self.group)
            .set("bootstrap.servers", &self.brokers)
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Error)
            .create_with_context(CustomContext)?;
        consumer.subscribe(&topics)?;
        Ok(consumer)
    }

    fn new_producer(&self) -> Result<FutureProducer, KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("message.timeout.ms", "10000")
            .create()?;
        Ok(producer)
    }
}

async fn run_producer(topic: String, mut recevier: Receiver<PostmanMsg>, producer: FutureProducer) {
    warn!("kafka producer run for: {}", topic);
    loop {
        match recevier.next().await {
            Some(PostmanMsg::Upper(msg)) => {
                debug!(
                    "recv pilot msg in kafka producer: {} - {}",
                    topic,
                    String::from_utf8(msg.data.as_ref().to_vec()).unwrap()
                );
                let rec = FutureRecord::to(&topic)
                    .payload(msg.data.to_bytes())
                    .key("");
                producer
                    .send(rec, Duration::from_secs(0))
                    .await
                    .unwrap_or_else(|(e, _)| {
                        error!("failed to produce kafka msg in topic: {}", e);
                        (-1, -1)
                    });
            }
            _ => {
                error!(target: "kafka_producer", "recv pipe broken: {}", topic);
                break;
            }
        }
    }
}

async fn run_consumer(
    topic: String,
    service: String,
    mut sender: Sender<PostmanMsg>,
    consumer: LoggingConsumer,
) {
    warn!("kafka consumer run for: {}", topic);
    loop {
        match consumer.recv().await {
            Ok(msg) => match msg.payload() {
                Some(bs) => {
                    let msg: serde_json::Result<ServiceMsg> = serde_json::from_slice(bs);
                    if msg.is_err() {
                        error!(
                            "recv bad msg from service - {}, failed to parse msg: {},  === {}",
                            service,
                            msg.err().unwrap().to_string(),
                            String::from_utf8(bs.to_vec()).unwrap()
                        );
                        continue;
                    }
                    let mut msg = msg.unwrap();
                    if msg.service.len() == 0 {
                        msg.service = service.clone();
                    }
                    if let Err(e) = msg.valiate() {
                        warn!("recv bad msg from service: {}, err: {}", service, e);
                        continue;
                    }
                    sender
                        .send(PostmanMsg::Downer(msg))
                        .await
                        .unwrap_or_else(|e| {
                            error!("failed to send PostmanMsg to : {}", e);
                        });
                }
                _ => {
                    warn!("recv empty kafka msg in topic: {}", topic);
                }
            },
            Err(e) => {
                error!("failed to consume kafka msg in topic: {} - {}", topic, e);
                Box::pin(time::sleep(time::Millis::from_secs(1))).await
            }
        }
    }
}

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        debug!("Committing offsets: {:?}", result);
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;
