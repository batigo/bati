use crate::kafka::KafkaPostman;
use crate::{ChannelConf, PostmanMsg};
use futures::channel::mpsc::{Receiver, Sender};
use std::error::Error;

pub struct Postman {
    kafka_consumer: Option<KafkaPostman>,
    kafka_producer: Option<KafkaPostman>,
}

impl Postman {
    pub fn new_upper(
        conf: ChannelConf,
        sender: Sender<PostmanMsg>,
        receiver: Receiver<PostmanMsg>,
    ) -> Result<Self, Box<dyn Error>> {
        Self::new(PostmanType::Upper, conf, sender, receiver)
    }

    pub fn new_downer(
        conf: ChannelConf,
        sender: Sender<PostmanMsg>,
        receiver: Receiver<PostmanMsg>,
    ) -> Result<Self, Box<dyn Error>> {
        Self::new(PostmanType::Downer, conf, sender, receiver)
    }

    pub fn new(
        typ: PostmanType,
        conf: ChannelConf,
        sender: Sender<PostmanMsg>,
        receiver: Receiver<PostmanMsg>,
    ) -> Result<Self, Box<dyn Error>> {
        if conf.kafka.is_none() {
            return Err("kafka conf empty".into());
        }

        let kafka_conf = conf.kafka.clone().unwrap();
        let mut group_id = "bati-consumer".to_string();
        if PostmanType::Upper == typ {
            let local_id = get_local_ip();
            if local_id.is_none() {
                return Err("faild to get local ip".into());
            }
            group_id = format!("{}_{}", group_id, local_id.unwrap());
        }

        let topic = format!("{}{}", conf.name, typ.read_channel_sufix());
        let kafka_consumer = KafkaPostman::new(
            kafka_conf.host_ports.clone(),
            topic,
            conf.name.clone(),
            group_id,
            Some(sender),
            None,
        );

        let topic = format!("{}{}", conf.name, typ.writer_channel_sufix());
        let kafka_producer = KafkaPostman::new(
            kafka_conf.host_ports,
            topic,
            conf.name,
            "".to_string(),
            None,
            Some(receiver),
        );

        Ok(Postman {
            kafka_consumer: Some(kafka_consumer),
            kafka_producer: Some(kafka_producer),
        })
    }

    pub fn run(&mut self) -> Result<(), Box<dyn Error>> {
        if self.kafka_consumer.is_some() {
            self.kafka_consumer.as_mut().unwrap().run()?;
        }

        if self.kafka_producer.is_some() {
            self.kafka_producer.as_mut().unwrap().run()?;
        }

        Ok(())
    }
}

#[derive(PartialEq, Copy, Clone)]
pub enum PostmanType {
    Upper,
    Downer,
}

impl PostmanType {
    pub fn writer_channel_sufix(self) -> String {
        match self {
            PostmanType::Upper => "_up".to_string(),
            _ => "_down".to_string(),
        }
    }

    pub fn read_channel_sufix(self) -> String {
        match self {
            PostmanType::Upper => "_down".to_string(),
            _ => "_up".to_string(),
        }
    }
}

fn get_local_ip() -> Option<String> {
    local_ipaddress::get()
}
