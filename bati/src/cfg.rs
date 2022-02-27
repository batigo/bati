use clap::Parser;
use serde::Deserialize;
use std::fs;

#[derive(Deserialize, Clone)]
pub struct Cfg {
    pub server: ServerCfg,
    pub channel_registry: ChannelRegistryCfg,
    pub log: LogCfg,
}

#[derive(Deserialize, Clone)]
pub struct ServerCfg {
    pub addr: String,
}

#[derive(Deserialize, Clone)]
pub struct ChannelRegistryCfg {
    pub consul: Option<ConsulCfg>,
    pub file: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct ConsulCfg {
    pub addr: String,
    pub channel_conf_path: String,
}

#[derive(Deserialize, Clone)]
pub struct LogCfg {
    pub file: String,
    pub level: String,
    pub roll_size: String,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// config file
    #[clap(short, long)]
    file: String,
}
pub fn parse() -> Result<Cfg, String> {
    let args = Args::parse();

    let s = fs::read_to_string(args.file).unwrap();
    let r: Result<Cfg, _> = serde_yaml::from_str(&s);
    if r.is_err() {
        return Err(r.err().unwrap().to_string());
    }

    Ok(r.unwrap())
}
