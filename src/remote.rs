//! Deliberately narrow HTTP protocol for the network historical-scale study.
//! This is not a SPARQL endpoint. Bodies are UTF-8, line-oriented TSV: raw
//! rows are `timestamp<TAB>subject<TAB>predicate<TAB>object<TAB>graph`; aggregate
//! rows are `subject<TAB>average`. Request bodies are `key=value` lines.
use crate::sources::{HistoricalSource, Observation, SegmentedHistoricalSource};
use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
pub struct NetworkProfile {
    pub name: &'static str,
    pub rtt_ms: f64,
    pub bandwidth_bits_per_second: f64,
    pub emulated: bool,
}
pub const PROFILES: [NetworkProfile; 4] = [
    NetworkProfile {
        name: "native-localhost",
        rtt_ms: 0.0,
        bandwidth_bits_per_second: 0.0,
        emulated: false,
    },
    NetworkProfile {
        name: "lan",
        rtt_ms: 1.0,
        bandwidth_bits_per_second: 1_000_000_000.0,
        emulated: true,
    },
    NetworkProfile {
        name: "moderate-edge",
        rtt_ms: 10.0,
        bandwidth_bits_per_second: 100_000_000.0,
        emulated: true,
    },
    NetworkProfile {
        name: "constrained-edge",
        rtt_ms: 30.0,
        bandwidth_bits_per_second: 10_000_000.0,
        emulated: true,
    },
];
pub fn profile(name: &str) -> Option<NetworkProfile> {
    PROFILES.into_iter().find(|p| p.name == name)
}

/// Controlled one-way transfer cost for UTF-8 HTTP *body* bytes.  This is not
/// socket- or OS-level traffic accounting (HTTP framing is intentionally out
/// of scope for this experiment).
pub fn emulated_bandwidth_delay_ms(
    application_payload_bytes: u64,
    bandwidth_bits_per_second: f64,
) -> f64 {
    if bandwidth_bits_per_second == 0.0 {
        0.0
    } else {
        1000.0 * 8.0 * application_payload_bytes as f64 / bandwidth_bits_per_second
    }
}
#[derive(Default, Clone)]
pub struct RemoteMetrics {
    pub storage_ms: f64,
    pub operator_ms: f64,
    pub serialization_ms: f64,
    /// Exclusive server-side response-body construction time.
    pub response_encoding_ms: f64,
    pub request_send_ms: f64,
    pub network_wait_ms: f64,
    pub response_receive_ms: f64,
    /// Inclusive client wall-clock request/response duration; never additive
    /// with server-side phase timings.
    pub http_request_total_ms: f64,
    pub deserialization_ms: f64,
    pub request_payload_bytes: u64,
    pub response_payload_bytes: u64,
    pub scanned: u64,
    pub rows: u64,
    pub historical_records_examined: u64,
    pub historical_records_matched: u64,
    pub historical_records_returned: u64,
    pub index_entries_examined: u64,
    pub segments_touched: u64,
    pub subject_index_used: bool,
    pub emulated_rtt_ms: f64,
    pub emulated_bandwidth_delay_ms: f64,
    pub total_emulated_transport_delay_ms: f64,
    pub client_transport_and_framework_residual_ms: f64,
}
pub struct RemoteClient {
    address: String,
}
impl RemoteClient {
    pub fn new(address: String) -> Self {
        Self { address }
    }
    fn post(&self, path: &str, body: String) -> Result<(String, RemoteMetrics), String> {
        let request_total = Instant::now();
        let mut m = RemoteMetrics {
            request_payload_bytes: body.len() as u64,
            ..Default::default()
        };
        let mut s = TcpStream::connect(&self.address).map_err(|e| e.to_string())?;
        s.set_nodelay(true).map_err(|e| e.to_string())?;
        let req=format!("POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",self.address,body.len(),body);
        let t = Instant::now();
        s.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
        s.flush().map_err(|e| e.to_string())?;
        s.shutdown(Shutdown::Write).map_err(|e| e.to_string())?;
        m.request_send_ms = t.elapsed().as_secs_f64() * 1000.0;
        let t = Instant::now();
        let mut raw = Vec::new();
        s.read_to_end(&mut raw).map_err(|e| e.to_string())?;
        m.network_wait_ms = t.elapsed().as_secs_f64() * 1000.0;
        let t = Instant::now();
        let text = String::from_utf8(raw).map_err(|e| e.to_string())?;
        let (headers, response) = text
            .split_once("\r\n\r\n")
            .ok_or("malformed HTTP response")?;
        m.response_receive_ms = t.elapsed().as_secs_f64() * 1000.0;
        m.http_request_total_ms = request_total.elapsed().as_secs_f64() * 1000.0;
        m.response_payload_bytes = response.len() as u64;
        for line in headers.lines().skip(1) {
            if let Some((k, v)) = line.split_once(':') {
                let n = v.trim().parse::<f64>().unwrap_or(0.0);
                match k.to_ascii_lowercase().as_str() {
                    "x-storage-ms" => m.storage_ms = n,
                    "x-operator-ms" => m.operator_ms = n,
                    "x-serialization-ms" => m.serialization_ms = n,
                    "x-response-encoding-ms" => m.response_encoding_ms = n,
                    "x-scanned" => m.scanned = n as u64,
                    "x-rows" => m.rows = n as u64,
                    "x-records-examined" => m.historical_records_examined = n as u64,
                    "x-records-matched" => m.historical_records_matched = n as u64,
                    "x-records-returned" => m.historical_records_returned = n as u64,
                    "x-index-entries-examined" => m.index_entries_examined = n as u64,
                    "x-segments-touched" => m.segments_touched = n as u64,
                    "x-subject-index-used" => m.subject_index_used = v.trim() == "true",
                    "x-emulated-rtt-ms" => m.emulated_rtt_ms = n,
                    "x-emulated-bandwidth-delay-ms" => m.emulated_bandwidth_delay_ms = n,
                    "x-total-emulated-transport-delay-ms" => {
                        m.total_emulated_transport_delay_ms = n
                    }
                    _ => {}
                }
            }
        }
        m.client_transport_and_framework_residual_ms = (m.http_request_total_ms
            - m.storage_ms
            - m.operator_ms
            - m.response_encoding_ms
            - m.total_emulated_transport_delay_ms)
            .max(0.0);
        Ok((response.to_string(), m))
    }
    pub fn window(
        &self,
        start: u64,
        end: u64,
    ) -> Result<(Vec<Observation>, RemoteMetrics), String> {
        let (body, mut m) = self.post("/window", format!("start={start}\nend={end}\n"))?;
        let t = Instant::now();
        let rows = body
            .lines()
            .filter(|x| !x.is_empty())
            .map(|x| {
                let p: Vec<_> = x.split('\t').collect();
                if p.len() != 5 {
                    return Err("bad raw row".to_string());
                }
                let ts = p[0].parse().map_err(|_| "bad timestamp".to_string())?;
                let value = p[3].parse().map_err(|_| "bad value".to_string())?;
                Ok(Observation::new(ts, p[1], value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        m.deserialization_ms = t.elapsed().as_secs_f64() * 1000.;
        Ok((rows, m))
    }
    pub fn aggregate(
        &self,
        start: u64,
        end: u64,
        bindings: Option<&HashSet<String>>,
    ) -> Result<(HashMap<String, f64>, RemoteMetrics), String> {
        let mut b = format!("start={start}\nend={end}\n");
        let path = if let Some(set) = bindings {
            b.push_str(&format!(
                "bindings={}\n",
                set.iter().cloned().collect::<Vec<_>>().join(",")
            ));
            "/aggregate-bound"
        } else {
            "/aggregate"
        };
        let (body, mut m) = self.post(path, b)?;
        let t = Instant::now();
        let a = body
            .lines()
            .filter(|x| !x.is_empty())
            .map(|x| {
                let (s, v) = x.split_once('\t').ok_or("bad aggregate row")?;
                Ok((s.to_string(), v.parse::<f64>().map_err(|_| "bad average")?))
            })
            .collect::<Result<HashMap<_, _>, String>>()?;
        m.deserialization_ms = t.elapsed().as_secs_f64() * 1000.;
        Ok((a, m))
    }
    pub fn aggregate_indexed(
        &self,
        start: u64,
        end: u64,
        bindings: &HashSet<String>,
    ) -> Result<(HashMap<String, f64>, RemoteMetrics), String> {
        let mut b = format!("start={start}\nend={end}\n");
        b.push_str(&format!(
            "bindings={}\n",
            bindings.iter().cloned().collect::<Vec<_>>().join(",")
        ));
        let (body, mut m) = self.post("/aggregate-bound-indexed", b)?;
        let t = Instant::now();
        let values = body
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                let (subject, value) = line.split_once('\t').ok_or("bad aggregate row")?;
                Ok((
                    subject.to_string(),
                    value.parse::<f64>().map_err(|_| "bad average")?,
                ))
            })
            .collect::<Result<HashMap<_, _>, String>>()?;
        m.deserialization_ms = t.elapsed().as_secs_f64() * 1000.;
        Ok((values, m))
    }
}
fn parse(body: &str) -> HashMap<&str, &str> {
    body.lines().filter_map(|l| l.split_once('=')).collect()
}
pub fn serve(
    address: &str,
    archive: &str,
    quads: usize,
    segment_quads: usize,
    p: NetworkProfile,
) -> Result<(), String> {
    let store = SegmentedHistoricalSource::open_existing(archive, quads, segment_quads)
        .map_err(|e| e.to_string())?;
    let listener = TcpListener::bind(address).map_err(|e| e.to_string())?;
    for incoming in listener.incoming() {
        let mut s = incoming.map_err(|e| e.to_string())?;
        let mut raw = String::new();
        s.read_to_string(&mut raw).map_err(|e| e.to_string())?;
        let (h, b) = raw.split_once("\r\n\r\n").ok_or("malformed request")?;
        let path = h.split_whitespace().nth(1).ok_or("missing path")?;
        let q = parse(b);
        let start = q
            .get("start")
            .ok_or("missing start")?
            .parse()
            .map_err(|_| "bad start")?;
        let end = q
            .get("end")
            .ok_or("missing end")?
            .parse()
            .map_err(|_| "bad end")?;
        let bindings = q.get("bindings").map(|value| {
            value
                .split(',')
                .filter(|subject| !subject.is_empty())
                .map(ToString::to_string)
                .collect::<HashSet<_>>()
        });
        let storage = Instant::now();
        let (rows, access) = if path == "/aggregate-bound-indexed" {
            let bindings = bindings.as_ref().ok_or("missing bindings")?;
            store
                .rows_for_subjects(start, end, bindings)
                .map_err(|e| e.to_string())
                .map(|(rows, metrics)| (rows, Some(metrics)))?
        } else {
            store
                .rows_with_metrics(start, end)
                .map_err(|e| e.to_string())
                .map(|(rows, metrics)| (rows, Some(metrics)))?
        };
        let storage_ms = storage.elapsed().as_secs_f64() * 1000.;
        let op = Instant::now();
        let aggregates = match path {
            "/window" => None,
            "/aggregate" | "/aggregate-bound" | "/aggregate-bound-indexed" => {
                let bindings = if path == "/aggregate" {
                    None
                } else {
                    bindings.as_ref()
                };
                let mut sums = HashMap::<String, (f64, u64)>::new();
                for r in &rows {
                    if bindings.as_ref().is_some_and(|x| !x.contains(&r.sensor)) {
                        continue;
                    }
                    let e = sums.entry(r.sensor.clone()).or_insert((0., 0));
                    e.0 += r.value;
                    e.1 += 1;
                }
                Some(
                    sums.into_iter()
                        .map(|(s, (sum, n))| (s, sum / n as f64))
                        .collect::<Vec<_>>(),
                )
            }
            _ => return Err("unsupported endpoint".into()),
        };
        let operator_ms = op.elapsed().as_secs_f64() * 1000.;
        let ser = Instant::now();
        let response = match aggregates {
            None => rows
                .iter()
                .map(|r| {
                    format!(
                        "{}\t{}\t{}\t{}\t{}",
                        r.rdf.timestamp, r.rdf.subject, r.rdf.predicate, r.rdf.object, r.rdf.graph
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"),
            Some(values) => values
                .into_iter()
                .map(|(s, value)| format!("{s}\t{value}"))
                .collect::<Vec<_>>()
                .join("\n"),
        };
        let response_bytes = response.len();
        let serialization_ms = ser.elapsed().as_secs_f64() * 1000.;
        // The server physically injects this controlled delay.  Therefore the
        // client wall-clock execution already includes it and callers must not
        // analytically add it again.
        let bandwidth_delay = if p.emulated {
            emulated_bandwidth_delay_ms(
                (b.len() + response_bytes) as u64,
                p.bandwidth_bits_per_second,
            )
        } else {
            0.0
        };
        let delay = if p.emulated {
            p.rtt_ms + bandwidth_delay
        } else {
            0.0
        };
        if delay > 0. {
            std::thread::sleep(Duration::from_secs_f64(delay / 1000.));
        }
        let examined = access
            .as_ref()
            .map(|m| m.records_examined)
            .unwrap_or(store.record_count() as u64);
        let matched = access
            .as_ref()
            .map(|m| m.records_matched)
            .unwrap_or(rows.len() as u64);
        let returned = access
            .as_ref()
            .map(|m| m.records_returned)
            .unwrap_or(rows.len() as u64);
        let index_entries = access
            .as_ref()
            .map(|m| m.index_entries_examined)
            .unwrap_or(0);
        let segments_touched = access.as_ref().map(|m| m.segments_touched).unwrap_or(0);
        let subject_index_used = access
            .as_ref()
            .is_some_and(|metrics| metrics.subject_index_used);
        let reply=format!("HTTP/1.1 200 OK\r\nContent-Length: {response_bytes}\r\nConnection: close\r\nX-Storage-Ms: {storage_ms:.6}\r\nX-Operator-Ms: {operator_ms:.6}\r\nX-Serialization-Ms: {serialization_ms:.6}\r\nX-Response-Encoding-Ms: {serialization_ms:.6}\r\nX-Scanned: {}\r\nX-Rows: {}\r\nX-Records-Examined: {examined}\r\nX-Records-Matched: {matched}\r\nX-Records-Returned: {returned}\r\nX-Index-Entries-Examined: {index_entries}\r\nX-Segments-Touched: {segments_touched}\r\nX-Subject-Index-Used: {}\r\nX-Emulated-Rtt-Ms: {:.6}\r\nX-Emulated-Bandwidth-Delay-Ms: {bandwidth_delay:.6}\r\nX-Total-Emulated-Transport-Delay-Ms: {delay:.6}\r\n\r\n{response}",store.record_count(),if path=="/window"{rows.len()}else{response.lines().filter(|x|!x.is_empty()).count()}, subject_index_used, if p.emulated { p.rtt_ms } else { 0.0 });
        s.write_all(reply.as_bytes()).map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::emulated_bandwidth_delay_ms;

    #[test]
    fn application_payload_bandwidth_delay_matches_controlled_model() {
        let delay = emulated_bandwidth_delay_ms(107_000_000, 10_000_000.0);
        assert!((delay - 85_600.0).abs() < 0.001);
    }
}
