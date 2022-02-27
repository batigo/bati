use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ServiceConf {
    pub name: String,
    pub enable_close_notify: bool,
    pub enable_multi_rooms: bool,
    pub kafka: Option<KafkaConf>,
}

impl ServiceConf {
    pub fn new(channel: String) -> Self {
        ServiceConf {
            name: channel,
            enable_close_notify: false,
            enable_multi_rooms: false,
            kafka: None,
        }
    }
}

impl std::fmt::Display for ServiceConf {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = serde_json::to_string(self).unwrap();
        write!(f, "{}", s)
    }
}

#[derive(Clone, Serialize, Debug, Default, Deserialize)]
#[serde(default)]
pub struct KafkaConf {
    pub host_ports: String,
}

impl KafkaConf {
    pub fn new(host_ports: String) -> Self {
        KafkaConf { host_ports }
    }
}
