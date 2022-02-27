use crate::cfg::ServiceRegistryCfg;
use crate::pilot_proto::PilotSender;
use bati_lib::{self as lib, ServiceRegistry, ServcieRegistryConf};
use log::error;
use ntex::{rt, time};

// ChanFinder负责监听channel配置启动对应的postman
pub struct ServiceFinder {
    pilots: Vec<PilotSender>,
    service_registry: ServiceRegistryCfg,
}

impl ServiceFinder {
    pub fn new(chan_registry: ServiceRegistryCfg) -> Self {
        ServiceFinder {
            service_registry: chan_registry,
            pilots: vec![],
        }
    }

    pub fn add_pilot(&mut self, pilot: PilotSender) {
        self.pilots.push(pilot);
    }

    pub fn start(&self, pilots: Vec<PilotSender>) {
        let mut cfg = ServcieRegistryConf {
            file: self.service_registry.file.clone(),
            consul: None,
        };
        if let Some(v) = self.service_registry.consul.clone() {
            cfg.consul = Some(lib::ConsulConf {
                addr: v.addr,
                channel_path: v.channel_conf_path,
            });
        }
        let service_reg = ServiceRegistry::new(cfg);

        rt::Arbiter::new().exec_fn(move || {
            let mut s = time::Millis(10);
            rt::spawn(async move {
                loop {
                    let _ = Box::pin(time::sleep(s)).await;
                    if s < time::Millis(5000) {
                        s = time::Millis(5000)
                    }
                    let channels = service_reg.get_all_services().await;
                    if channels.is_err() {
                        error!("failed to get all channels: {}", channels.err().unwrap());
                        continue;
                    }
                    let channels = channels.unwrap().clone();
                    let chan_confs = channels.clone();
                    let pilots = pilots.clone();
                    rt::spawn(async move {
                        for mut pilot in pilots {
                            for conf in chan_confs.iter() {
                                pilot
                                    .send_servicefinder_msg(conf.clone())
                                    .await
                                    .unwrap_or_else(|e| {
                                        error!("failed to send channel conf message: {}", e);
                                    });
                            }
                        }
                    });
                }
            });
        });
    }
}
