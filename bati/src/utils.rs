use bati_lib as lib;

pub fn gen_conn_id(did: &str, uid: &str, worker_index: usize) -> String {
    let s = format!("{}_{}_{}", did, uid, lib::get_now_milli());
    let id: String = format!("{:x}", md5::compute(s));
    format!("{}_{}", id, worker_index)
}

pub fn get_worker_index_from_cid(cid: &str) -> Option<usize> {
    let s: Vec<&str> = cid.split('_').collect();
    if s.len() != 2 {
        return None;
    }

    if let Ok(s) = s[1].parse::<usize>() {
        return Some(s);
    }

    None
}

pub fn get_service_name() -> String {
    let s = std::env::var("PROJECT_NAME").unwrap_or("bati".to_string());
    s.replace("-", "_")
}
