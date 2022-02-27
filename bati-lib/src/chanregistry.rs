use crate::channel::{ChannelConf, KafkaConf};
use log::debug;
use serde::Deserialize;
use serde_json;
use std::fs;

pub struct ChannelRegistryConf {
    pub file: Option<String>,
    pub consul: Option<ConsulConf>,
}

pub struct ConsulConf {
    pub addr: String,
    pub channel_path: String,
}

pub struct ChannelRegistry {
    cconf: Option<Cconfer>,
    fconf: Option<String>,
}

impl ChannelRegistry {
    pub fn new(conf: ChannelRegistryConf) -> Self {
        if conf.file.is_none() && conf.consul.is_none() {
            panic!("channel registry conf abnormal, file & consul empty");
        }

        let mut r = ChannelRegistry {
            fconf: conf.file,
            cconf: None,
        };
        if let Some(cconf) = conf.consul {
            r.cconf = Some(Cconfer::new(cconf.addr, cconf.channel_path));
        }
        r
    }

    pub async fn get_all_channels(&self) -> Result<Vec<ChannelConf>, String> {
        if let Some(c) = &self.cconf {
            let chans = c.get_path_keys("").await?;
            let mut confs: Vec<ChannelConf> = vec![];
            for chan in chans {
                let conf = self.get_consul_channel_conf(&chan).await?;
                confs.push(conf);
            }
            return Ok(confs);
        }

        match fs::read_to_string(self.fconf.as_ref().unwrap()) {
            Ok(s) => {
                let confs: serde_json::Result<Vec<ChannelConf>> = serde_json::from_str(&s);
                if confs.is_err() {
                    return Err(confs.err().unwrap().to_string());
                }
                Ok(confs.unwrap())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    async fn get_consul_channel_conf(&self, channel: &str) -> Result<ChannelConf, String> {
        let data = self.cconf.as_ref().unwrap().get_data(&channel).await?;
        debug!("get channel conf, {} - {}", channel, data);
        let ini_conf = ini::Ini::load_from_str(&data);
        if ini_conf.is_err() {
            return Err(ini_conf.err().unwrap().to_string());
        }
        let ini_conf = ini_conf.unwrap();
        let main_section = ini_conf.general_section();

        let mut conf = ChannelConf::new(channel.to_string());
        conf.enable_close_notify = Self::get_boolean_form_property(main_section, "quit_notify")?;
        conf.enable_multi_rooms = Self::get_boolean_form_property(main_section, "multi_rooms")?;
        if ini_conf.section(Some("kafka")).is_some() {
            let kafka_seciont = ini_conf.section(Some("kafka")).unwrap();
            let kafka_host_ports = kafka_seciont.get("hostports").unwrap_or("");
            if kafka_host_ports.is_empty() {
                return Err("kafka host ports empty".into());
            }
            conf.kafka = Some(KafkaConf::new(kafka_host_ports.to_string()));
        }

        Ok(conf)
    }

    fn get_boolean_form_property(p: &ini::Properties, k: &str) -> Result<bool, String> {
        match p.get(k).unwrap_or("false").parse::<bool>() {
            Ok(v) => Ok(v),
            Err(e) => Err(e.to_string()),
        }
    }
}

// consul conf-center
pub struct Cconfer {
    consul_addr: String,
    key_prefix: String,
}

impl Cconfer {
    pub fn new(consul_addr: String, key_prefix: String) -> Self {
        Cconfer {
            consul_addr,
            key_prefix,
        }
    }

    pub async fn get_data(&self, key: &str) -> Result<String, String> {
        let mut url = format!(
            "http://{}/v1/kv/{}/{}",
            self.consul_addr, self.key_prefix, key
        );
        if self.key_prefix.is_empty() {
            url = format!("http://{}/v1/kv/{}", self.consul_addr, key);
        }

        let data = self.get(&url).await?;

        #[derive(Deserialize)]
        struct ConsulData {
            #[serde(rename = "Value")]
            value: String,
        }
        let s: serde_json::Result<Vec<ConsulData>> = serde_json::from_str(&data);
        if s.is_err() {
            return Err(s.err().unwrap().to_string());
        }
        let mut data = s.as_ref().unwrap().get(0);
        if data.is_none() {
            return Err("no consul value found".into());
        }

        match base64::decode(data.take().unwrap().value.clone()) {
            Ok(data) => match String::from_utf8(data) {
                Ok(data) => Ok(data),
                Err(e) => Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        }
    }

    pub async fn get_path_keys(&self, path: &str) -> Result<Vec<String>, String> {
        let mut url = format!(
            "http://{}/v1/kv/{}/{}?keys",
            self.consul_addr, self.key_prefix, path
        );
        if self.key_prefix.is_empty() {
            url = format!("http://{}/v1/kv/{}?keys", self.consul_addr, path);
        }

        let data = self.get(&url).await?;
        let list: serde_json::Result<Vec<String>> = serde_json::from_str(&data);
        if list.is_err() {
            return Err(list.err().unwrap().to_string());
        }

        let mut trim_s = "".to_string();
        if !self.key_prefix.is_empty() {
            trim_s = self.key_prefix.to_string() + "/";
        }
        if !path.is_empty() {
            trim_s = trim_s + path + "/";
        }

        Ok(list
            .unwrap()
            .iter()
            .map(|s| s.trim_start_matches(&trim_s).to_string())
            .filter(|s| !s.is_empty())
            .collect())
    }

    pub async fn get(&self, url: &str) -> Result<String, String> {
        match reqwest::get(url).await {
            Err(e) => Err(e.to_string()),
            Ok(resp) => match resp.text().await {
                Ok(s) => Ok(s),
                Err(e) => Err(e.to_string()),
            },
        }
    }
}
