use crate::hub_proto::HubSender;
use crate::pilot_proto::PilotSender;
use futures::future::ready;
use log::{error, info};
use ntex::rt;
use ntex::service::{fn_factory_with_config, fn_service, map_config, Service};
use ntex::web::{self, ws, App, Error, HttpRequest, HttpResponse};
use prometheus::Encoder;
use serde::Deserialize;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

mod cfg;
mod conn;
mod conn_proto;
mod const_proto;
mod encoding;
mod hub;
mod hub_proto;
mod metric;
mod metric_proto;
mod pilot;
mod pilot_proto;
mod service_finder;
mod timer;
mod utils;

#[macro_use]
extern crate prometheus;

#[macro_use]
extern crate lazy_static;

#[ntex::main]
async fn main() {
    let config = cfg::parse();
    if config.is_err() {
        println!("failed to load config: {}", config.err().unwrap());
        return;
    }

    let config = config.unwrap();
    init_logger(config.log.clone());

    let cpus = num_cpus::get();
    let mut chan_finder = service_finder::ServiceFinder::new(config.service_registry.clone());

    let metrics_collecor = metric::MetricCollector::new(cpus).start();

    let mut pilots = vec![];
    for ix in 0..cpus {
        let pilot = pilot::Pilot::new(ix, cpus).start();
        chan_finder.add_pilot(pilot.clone());
        pilots.push(pilot);
    }

    chan_finder.start(pilots.clone());

    let pilots = Arc::new(Mutex::new(pilots));

    let worker_index = Arc::new(AtomicUsize::new(0));
    let server = web::server(move || {
        let pilots = pilots.clone();
        let worker_index = worker_index.clone();
        let pilot: PilotSender;
        let ix = worker_index.fetch_add(1, Ordering::SeqCst);
        {
            let pilots = pilots.lock().unwrap().to_vec();
            pilot = pilots.get(ix).unwrap().clone();
        }

        let hub_sender = hub::Hub::new(ix, pilot.clone(), metrics_collecor.clone()).start();
        {
            let pilots = pilots.clone();
            let hub_sender = hub_sender.clone();
            rt::spawn(async move {
                for mut pilot in pilots.lock().unwrap().to_vec() {
                    pilot
                        .send_master_msg(pilot_proto::PilotAddHubMsg {
                            ix,
                            hub: hub_sender.clone(),
                        })
                        .await
                        .unwrap();
                }
            });
        }
        App::new()
            .state(hub_sender)
            .state(pilot)
            .state(ix)
            .service(web::resource("/ws").to(websocket_req_handler))
            .service(web::resource("/healthcheck").to(healthcheck_handler))
            .service(web::resource("/metrics").to(metrics_handler))
    })
    .bind(&config.server.addr);

    match server {
        Ok(server) => {
            server.run().await.unwrap_or_else(|e| {
                error!("failed to run server: {}", e);
            });
        }
        Err(e) => {
            error!("failed to bind server: {}", e);
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Param {
    token: String,
    uid: String,
    did: String,
    dt: u8,
}

#[derive(Clone)]
struct ConnLite {
    ix: usize,
    uid: String,
    did: String,
    ip: String,
    dt: u8,
    hub: HubSender,
    pilot: PilotSender,
}

async fn websocket_req_handler(
    req: HttpRequest,
    hub: web::types::State<HubSender>,
    pilot: web::types::State<PilotSender>,
    worker_index: web::types::State<usize>,
    param: web::types::Query<Param>,
) -> Result<HttpResponse, Error> {
    let ip = get_client_ip(&req);
    info!("new request: {}, ip: {}", req.query_string(), ip);

    let ss = ConnLite {
        ip,
        uid: param.uid.clone(),
        did: param.did.clone(),
        dt: param.dt,
        hub: hub.get_ref().clone(),
        pilot: pilot.as_ref().clone(),
        ix: *worker_index.as_ref(),
    };
    ws::start(
        req,
        map_config(fn_factory_with_config(websocket_service), move |cfg| {
            (cfg, ss.clone())
        }),
    )
    .await
}

/// WebSockets service factory
async fn websocket_service(
    (sink, ss): (ws::WsSink, ConnLite),
) -> Result<impl Service<ws::Frame, Response = Option<ws::Message>, Error = io::Error>, web::Error>
{
    let conn = conn::Conn::new(ss.uid, ss.did, ss.ip, ss.dt, ss.hub, ss.pilot, ss.ix);
    let conn_sender = conn.start(sink.clone());

    // handler service for incoming websockets frames
    let conn_sender2 = conn_sender.clone();
    Ok(fn_service(move |frame| {
        let sender = conn_sender2.clone();
        rt::spawn(async move {
            sender
                .send_master_msg(conn_proto::Master2ConnMsg::Frame(frame))
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "failed to send msg to conn: {} - {} ",
                        sender.id,
                        e.to_string()
                    )
                });
        });

        ready(Ok(None))
    })
    .on_shutdown(move || {
        rt::spawn(async move {
            conn_sender
                .send_master_msg(conn_proto::Master2ConnMsg::Shutdown)
                .await
                .unwrap_or(());
        });
    }))
}

async fn healthcheck_handler(_req: HttpRequest) -> &'static str {
    "Ok"
}

fn get_client_ip(r: &HttpRequest) -> String {
    let mut ip: String = "".to_string();

    if let Some(h) = r.headers().get("X-Forwarded-For") {
        if let Ok(v) = String::from_utf8(h.as_bytes().to_owned()) {
            ip = v
        }
    }

    if ip.is_empty() {
        if let Some(h) = r.headers().get("X-Real-IP") {
            if let Ok(v) = String::from_utf8(h.as_bytes().to_owned()) {
                ip = v
            }
        }
    }

    if !ip.is_empty() {
        let ss: Vec<&str> = ip.split(',').collect();
        if !ss.is_empty() {
            return ss.get(0).unwrap().to_string();
        }
    }

    match r.peer_addr() {
        Some(v) => v.ip().to_string(),
        _ => "127.0.0.1".to_string(),
    }
}

async fn metrics_handler(_: HttpRequest) -> String {
    let encoder = prometheus::TextEncoder::new();
    let mut buf = vec![];
    let mf = prometheus::gather();
    match encoder.encode(&mf, &mut buf) {
        Err(e) => {
            error!("failed to encode metrics: {:?}", e);
            "".to_string()
        }
        Ok(_) => match String::from_utf8(buf) {
            Ok(s) => s,
            Err(e) => {
                error!("failed to utf8-encode metrics data: {}", e);
                "".to_string()
            }
        },
    }
}

// 初始化日志
fn init_logger(cfg: cfg::LogCfg) {
    let config = "
appenders:
    file:
        kind: rolling_file
        path: __LOG_FILE__
        append: true
        encoder:
            kind: pattern
        policy:
            kind: compound
            trigger:
                kind: size
                limit: _LOG_ROLLSIZE__
            roller:
                kind: fixed_window
                pattern: __LOG_FILE__.{}
                base: 1
                count: 10
root:
    level: __LOG_LEVEL__
    appenders:
        - file
";
    let config = config
        .replace("__LOG_FILE__", &cfg.file)
        .replace("__LOG_LEVEL__", &cfg.level)
        .replace("_LOG_ROLLSIZE__", &cfg.roll_size);

    let log_raw_cfg: log4rs::config::RawConfig = serde_yaml::from_str(&config).unwrap();
    log4rs::init_raw_config(log_raw_cfg).unwrap();
}
