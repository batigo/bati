use crate::const_proto::*;
use crate::metric_proto::{
    new_metric_channel, HubMetricMsg, MetricMessage, MetricReceiver, MetricSender, MetricTimerMsg,
};
use crate::utils::get_service_name;
use log::{debug, error, info, warn};
use ntex::rt;
use prometheus::{
    exponential_buckets, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::Read;
use std::process;

lazy_static! {
    static ref SEND_MSG_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        format!("{}_send_client_msgs", get_service_name()),
        "send client msgs",
        &["channel"]
    )
    .unwrap();
    static ref RECV_MSG_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        format!("{}_recv_client_msgs", get_service_name()),
        "recv client msgs",
        &["type"]
    )
    .unwrap();
    static ref DT_SESSIONS_VEC: IntGaugeVec = register_int_gauge_vec!(
        format!("{}_nconnected_clients", get_service_name()),
        "connected clients by device type",
        &["dt"]
    )
    .unwrap();
    static ref CHANNEL_SESSIONS_VEC: IntGaugeVec = register_int_gauge_vec!(
        format!("{}_nconnected_channel_clients", get_service_name()),
        "connected clients by channel",
        &["channel"]
    )
    .unwrap();
    static ref CHANNEL_MSG_LATENCY: HistogramVec = register_histogram_vec!(
        format!("{}_channel_msg_latency", get_service_name()),
        "channel msg latency",
        &["channel"],
        exponential_buckets(1.0, 2.0, 8).unwrap()
    )
    .unwrap();
    static ref CPU_USAGE: IntCounter = register_int_counter!(
        format!("{}_process_cpu_seconds_total", get_service_name()),
        "Total user and system CPU time spent in second"
    )
    .unwrap();
    static ref RSS_USAGE: IntGauge = register_int_gauge!(
        format!("{}_process_resident_memory_bytes", get_service_name()),
        "Resident memory size in bytes"
    )
    .unwrap();
}

pub fn update_service_msg_latency(channel: &str, latency: u64) {
    let latency: i32 = latency.try_into().unwrap_or(0);
    let latency: f64 = latency.try_into().unwrap_or(0 as f64);
    CHANNEL_MSG_LATENCY
        .with_label_values(&[channel])
        .observe(latency);
}

pub fn inc_send_msg(channel: &str, count: u64) {
    SEND_MSG_COUNTER_VEC
        .with_label_values(&[channel])
        .inc_by(count);
}

pub fn inc_recv_msg(typ: &str, count: u64) {
    RECV_MSG_COUNTER_VEC.with_label_values(&[typ]).inc_by(count);
}

pub struct MetricCollector {
    dt_sessions: Vec<HashMap<DeviceType, u64>>,
    channel_sessions: Vec<HashMap<String, u64>>,
    cpu_total: u64,
    msg_sender: MetricSender,
    msg_receiver: MetricReceiver,
}

impl MetricCollector {
    pub fn new(hub_size: usize) -> Self {
        let (msg_sender, msg_receiver) = new_metric_channel(128);
        let mut c = MetricCollector {
            msg_sender,
            msg_receiver,
            dt_sessions: Vec::with_capacity(hub_size),
            channel_sessions: Vec::with_capacity(hub_size),
            cpu_total: 0,
        };
        for _ in 0..hub_size {
            c.dt_sessions.push(HashMap::new());
            c.channel_sessions.push(HashMap::new());
        }
        c
    }

    pub fn start(self) -> MetricSender {
        use crate::timer::start_metric_cron;
        let sender = self.msg_sender.clone();

        rt::spawn(async move {
            let mut collector = self;
            loop {
                let msg = collector.msg_receiver.next().await;
                if msg.is_none() {
                    panic!("failed to receive metric msg");
                }

                let msg = msg.unwrap();
                debug!("recv metric hub msg: {:?}", msg);

                match msg {
                    MetricMessage::FromTimer(msg) => match msg {
                        MetricTimerMsg::Collect => {
                            collector.metrics_stat_cron();
                        }
                    },
                    MetricMessage::FromHub(msg) => {
                        if msg.ix >= collector.channel_sessions.len() {
                            error!("get anbormal hub index: {:?}", msg);
                            continue;
                        }
                        let HubMetricMsg {
                            ix,
                            dt_conns: dt_sessions,
                            service_conns: channel_sessions,
                        } = msg;
                        collector.channel_sessions[ix] = channel_sessions;
                        collector.dt_sessions[ix] = dt_sessions;
                    }
                }
            }
        });

        start_metric_cron(sender.clone());
        sender
    }

    fn metrics_stat_cron(&mut self) {
        let mut dt_session_stat: HashMap<String, u64> = HashMap::new();
        for sessions in &self.dt_sessions {
            for (&dt, &n) in sessions {
                let count = match dt_session_stat.get(dt) {
                    Some(&m) => n + m,
                    None => n,
                };
                dt_session_stat.insert(dt.to_string(), count);
            }
        }
        dt_session_stat.iter().for_each(|(dt, n)| {
            DT_SESSIONS_VEC.with_label_values(&[dt]).set(*n as i64);
        });

        let mut channel_session_stat: HashMap<String, u64> = HashMap::new();
        for sessions in &self.channel_sessions {
            for (dt, &n) in sessions {
                let count = match channel_session_stat.get(dt) {
                    Some(&m) => n + m,
                    None => n,
                };
                channel_session_stat.insert(dt.to_string(), count);
            }
        }
        channel_session_stat.iter().for_each(|(channel, &n)| {
            CHANNEL_SESSIONS_VEC
                .with_label_values(&[channel])
                .set(n as i64);
        });

        // stat rss & cpu metrics
        let (rss_usage, cpu_useage) = get_rss_cpu_usage();
        RSS_USAGE.set(rss_usage as i64);
        CPU_USAGE.inc_by(cpu_useage - self.cpu_total);
        self.cpu_total = cpu_useage;
    }
}

#[cfg(target_os = "linux")]
fn get_rss_cpu_usage() -> (u64, u64) {
    let mut rss_usage: u64 = 0;
    let mut cpu_usage: u64 = 0;
    let pid = process::id();
    let cache_page_size = page_size::get();

    let proc_stat_file = format!("/proc/{}/stat", pid);
    match std::fs::File::open(&proc_stat_file) {
        Ok(mut f) => {
            let mut s = "".to_string();
            match f.read_to_string(&mut s) {
                Ok(_) => {
                    let stats: Vec<&str> = s.trim().split(" ").collect();
                    if stats.len() < 24 {
                        warn!("bad process file content: {} - {}", proc_stat_file, s);
                        return (0, 0);
                    }
                    let rss = &stats[23].parse::<u64>();
                    if rss.is_ok() {
                        rss_usage = rss.clone().unwrap_or(0) * cache_page_size as u64;
                    }
                    let utime = &stats[13].parse::<u64>();
                    let stime = &stats[14].parse::<u64>();
                    if utime.is_ok() && stime.is_ok() {
                        cpu_usage = utime.clone().unwrap_or(0) + stime.clone().unwrap_or(0);
                    }
                }
                Err(e) => {
                    warn!(
                        "failed to read process stat file: {} - {}",
                        proc_stat_file, e
                    );
                }
            }
        }
        Err(e) => {
            warn!(
                "failed to open process stat file: {} - {}",
                proc_stat_file, e
            );
        }
    }

    (rss_usage, cpu_usage / 100)
}

#[cfg(not(target_os = "linux"))]
fn get_rss_cpu_usage() -> (u64, u64) {
    (0, 0)
}
