use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls, connect_async,
    tungstenite::{handshake::client::Response, protocol::Message},
};

use crate::{domain::now_ms, strategy::LeaderFillEvent};

use super::{
    LeaderPositionSnapshot, leader_fill_event_from_user_fill,
    leader_position_snapshots_from_clearinghouse_state,
};

type CopyWatcherWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone)]
pub struct SmartMoneyLeaderWatch {
    pub leader_id: String,
    pub leader_address: String,
}

#[derive(Debug, Clone)]
pub struct ReadOnlyLeaderWatcherConfig {
    pub environment: String,
    pub ws_url: Option<String>,
    pub dex: Option<String>,
    pub leaders: Vec<SmartMoneyLeaderWatch>,
}

#[derive(Debug, Clone)]
pub enum CopyLeaderWatcherEvent {
    Fill {
        leader_id: String,
        leader_address: String,
        fill: LeaderFillEvent,
        is_snapshot: bool,
    },
    PositionSnapshots {
        leader_id: String,
        leader_address: String,
        dex: Option<String>,
        snapshots: Vec<LeaderPositionSnapshot>,
    },
    OrderUpdate {
        leader_id: String,
        leader_address: String,
        coin: String,
        oid: u64,
        status: String,
        status_timestamp_ms: u64,
    },
}

#[derive(Debug, Deserialize)]
struct CopyWsEnvelope {
    channel: String,
    #[serde(default)]
    data: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CopyWsUserFillsData {
    #[serde(default)]
    is_snapshot: Option<bool>,
    user: String,
    #[serde(default)]
    fills: Vec<crate::hyperliquid::UserFill>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CopyWsAllDexsClearinghouseData {
    user: String,
    clearinghouse_states: CopyWsAllDexsClearinghouseStates,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum CopyWsAllDexsClearinghouseStates {
    Map(HashMap<String, crate::hyperliquid::ClearinghouseState>),
    Pairs(Vec<(String, crate::hyperliquid::ClearinghouseState)>),
}

impl CopyWsAllDexsClearinghouseStates {
    fn into_pairs(self) -> Vec<(String, crate::hyperliquid::ClearinghouseState)> {
        match self {
            Self::Map(states) => states.into_iter().collect(),
            Self::Pairs(states) => states,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CopyWsClearinghouseData {
    #[serde(default)]
    dex: Option<String>,
    user: String,
    #[serde(flatten)]
    state: crate::hyperliquid::ClearinghouseState,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CopyWsOrderUpdate {
    order: CopyWsBasicOrder,
    status: String,
    status_timestamp: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CopyWsBasicOrder {
    coin: String,
    oid: u64,
}

pub fn read_only_leader_watcher_subscriptions(
    leaders: &[SmartMoneyLeaderWatch],
    dex: Option<&str>,
) -> Vec<Value> {
    let mut subscriptions = Vec::new();
    for leader in leaders {
        let user = leader.leader_address.clone();
        subscriptions.push(json!({ "type": "userFills", "user": user }));
        subscriptions.push(json!({ "type": "userEvents", "user": user }));
        subscriptions.push(json!({ "type": "orderUpdates", "user": user }));
        subscriptions.push(json!({ "type": "allDexsClearinghouseState", "user": user }));
        if let Some(dex) = normalized_optional_dex(dex) {
            subscriptions.push(json!({
                "type": "clearinghouseState",
                "user": leader.leader_address,
                "dex": dex,
            }));
        }
    }
    subscriptions
}

pub fn parse_read_only_leader_watcher_message(
    leaders: &[SmartMoneyLeaderWatch],
    configured_dex: Option<&str>,
    text: &str,
    received_at_ms: u64,
) -> Result<Vec<CopyLeaderWatcherEvent>> {
    if !text.trim_start().starts_with('{') {
        return Ok(Vec::new());
    }
    let envelope: CopyWsEnvelope =
        serde_json::from_str(text).context("failed to parse copy watcher websocket envelope")?;
    let leader_by_address = leaders
        .iter()
        .map(|leader| (leader.leader_address.to_ascii_lowercase(), leader))
        .collect::<HashMap<_, _>>();

    match envelope.channel.as_str() {
        "userFills" => {
            let data: CopyWsUserFillsData = serde_json::from_value(envelope.data)
                .context("failed to parse copy watcher userFills")?;
            Ok(copy_watcher_fill_events(
                &leader_by_address,
                &data.user,
                data.fills,
                data.is_snapshot.unwrap_or(false),
                received_at_ms,
            ))
        }
        "user" | "userEvents" => {
            let user = envelope
                .data
                .get("user")
                .and_then(Value::as_str)
                .or_else(|| {
                    leader_for_single_user_stream(&leader_by_address)
                        .map(|leader| leader.leader_address.as_str())
                })
                .unwrap_or_default()
                .to_string();
            let fills = envelope.data.get("fills").cloned().unwrap_or(Value::Null);
            if fills.is_null() {
                return Ok(Vec::new());
            }
            let fills = serde_json::from_value::<Vec<crate::hyperliquid::UserFill>>(fills)
                .context("failed to parse copy watcher userEvents fills")?;
            Ok(copy_watcher_fill_events(
                &leader_by_address,
                &user,
                fills,
                false,
                received_at_ms,
            ))
        }
        "orderUpdates" => {
            let updates = serde_json::from_value::<Vec<CopyWsOrderUpdate>>(envelope.data)
                .context("failed to parse copy watcher orderUpdates")?;
            let mut events = Vec::new();
            for update in updates {
                let Some(leader) = leader_for_single_user_stream(&leader_by_address) else {
                    continue;
                };
                events.push(CopyLeaderWatcherEvent::OrderUpdate {
                    leader_id: leader.leader_id.clone(),
                    leader_address: leader.leader_address.clone(),
                    coin: update.order.coin,
                    oid: update.order.oid,
                    status: update.status,
                    status_timestamp_ms: update.status_timestamp,
                });
            }
            Ok(events)
        }
        "allDexsClearinghouseState" => {
            let data: CopyWsAllDexsClearinghouseData = serde_json::from_value(envelope.data)
                .context("failed to parse copy watcher allDexsClearinghouseState")?;
            let mut events = Vec::new();
            let Some(leader) = leader_by_address.get(&data.user.to_ascii_lowercase()) else {
                return Ok(events);
            };
            for (dex, state) in data.clearinghouse_states.into_pairs() {
                if configured_dex
                    .is_some_and(|configured| !configured.trim().eq_ignore_ascii_case(dex.trim()))
                {
                    continue;
                }
                let snapshots = leader_position_snapshots_from_clearinghouse_state(
                    &leader.leader_id,
                    watcher_market_for_dex(Some(&dex)),
                    Some(dex.clone()),
                    &state,
                    received_at_ms,
                );
                events.push(CopyLeaderWatcherEvent::PositionSnapshots {
                    leader_id: leader.leader_id.clone(),
                    leader_address: leader.leader_address.clone(),
                    dex: Some(dex),
                    snapshots,
                });
            }
            Ok(events)
        }
        "clearinghouseState" => {
            let Ok(data) = serde_json::from_value::<CopyWsClearinghouseData>(envelope.data) else {
                return Ok(Vec::new());
            };
            let Some(leader) = leader_by_address.get(&data.user.to_ascii_lowercase()) else {
                return Ok(Vec::new());
            };
            let dex = data.dex.as_deref().or(configured_dex);
            let snapshots = leader_position_snapshots_from_clearinghouse_state(
                &leader.leader_id,
                watcher_market_for_dex(dex),
                dex.map(str::to_string),
                &data.state,
                received_at_ms,
            );
            Ok(vec![CopyLeaderWatcherEvent::PositionSnapshots {
                leader_id: leader.leader_id.clone(),
                leader_address: leader.leader_address.clone(),
                dex: dex.map(str::to_string),
                snapshots,
            }])
        }
        "subscriptionResponse" | "pong" => Ok(Vec::new()),
        _ => Ok(Vec::new()),
    }
}

pub async fn run_read_only_leader_watcher_once(
    config: ReadOnlyLeaderWatcherConfig,
    sender: mpsc::Sender<CopyLeaderWatcherEvent>,
) -> Result<()> {
    let ws_url = copy_watcher_ws_url(config.ws_url.as_deref(), &config.environment);
    let (stream, _) = connect_copy_watcher_websocket(ws_url.as_str())
        .await
        .with_context(|| format!("failed to connect read-only copy watcher websocket {ws_url}"))?;
    let (mut writer, mut reader) = stream.split();
    for subscription in
        read_only_leader_watcher_subscriptions(&config.leaders, config.dex.as_deref())
    {
        writer
            .send(Message::Text(
                json!({
                    "method": "subscribe",
                    "subscription": subscription,
                })
                .to_string(),
            ))
            .await
            .context("failed to send copy watcher subscription")?;
    }

    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                writer.send(Message::Ping(Vec::new())).await.context("failed to send copy watcher ping")?;
            }
            message = reader.next() => {
                let Some(message) = message else { break };
                let text = match message.context("failed to read copy watcher websocket message")? {
                    Message::Text(text) => text,
                    Message::Ping(payload) => {
                        writer.send(Message::Pong(payload)).await.context("failed to send copy watcher pong")?;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => break,
                    _ => continue,
                };
                for event in parse_read_only_leader_watcher_message(
                    &config.leaders,
                    config.dex.as_deref(),
                    &text,
                    now_ms(),
                )? {
                    sender.send(event).await.context("copy watcher receiver closed")?;
                }
            }
        }
    }
    Ok(())
}

async fn connect_copy_watcher_websocket(ws_url: &str) -> Result<(CopyWatcherWsStream, Response)> {
    if let Some(proxy_url) = copy_watcher_proxy_url_for_ws(ws_url) {
        return connect_copy_watcher_websocket_via_http_proxy(ws_url, &proxy_url)
            .await
            .with_context(|| {
                format!(
                    "failed to connect through HTTP proxy {}",
                    redact_copy_watcher_proxy_url(&proxy_url)
                )
            });
    }
    connect_async(ws_url)
        .await
        .context("failed to connect directly")
}

async fn connect_copy_watcher_websocket_via_http_proxy(
    ws_url: &str,
    proxy_url: &str,
) -> Result<(CopyWatcherWsStream, Response)> {
    let target = copy_watcher_ws_endpoint(ws_url)?;
    let proxy = copy_watcher_http_proxy_endpoint(proxy_url)?;
    let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port))
        .await
        .with_context(|| format!("failed to connect HTTP proxy {}", proxy.authority()))?;
    let connect_target = target.authority();
    let request = format!(
        "CONNECT {connect_target} HTTP/1.1\r\nHost: {connect_target}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .context("failed to send HTTP CONNECT request")?;
    stream
        .flush()
        .await
        .context("failed to flush HTTP CONNECT request")?;

    let mut response = Vec::new();
    let mut buf = [0_u8; 1024];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        if response.len() >= 8192 {
            bail!("HTTP proxy CONNECT response exceeded 8192 bytes");
        }
        let n = stream
            .read(&mut buf)
            .await
            .context("failed to read HTTP proxy CONNECT response")?;
        if n == 0 {
            bail!("HTTP proxy closed before CONNECT response completed");
        }
        response.extend_from_slice(&buf[..n]);
    }
    let response_text = String::from_utf8_lossy(&response);
    let status_line = response_text.lines().next().unwrap_or_default();
    if !status_line.starts_with("HTTP/") || !status_line.contains(" 200") {
        bail!("HTTP proxy CONNECT rejected target {connect_target}: {status_line}");
    }

    client_async_tls(ws_url, stream)
        .await
        .context("failed to complete websocket TLS handshake through HTTP proxy")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyWatcherEndpoint {
    host: String,
    port: u16,
}

impl CopyWatcherEndpoint {
    fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn copy_watcher_proxy_url_for_ws(ws_url: &str) -> Option<String> {
    let wss = ws_url.trim_start().starts_with("wss://");
    let names: &[&str] = if wss {
        &["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"]
    } else {
        &["HTTP_PROXY", "http_proxy"]
    };
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn copy_watcher_ws_endpoint(ws_url: &str) -> Result<CopyWatcherEndpoint> {
    let trimmed = ws_url.trim();
    let (rest, default_port) = if let Some(rest) = trimmed.strip_prefix("wss://") {
        (rest, 443)
    } else if let Some(rest) = trimmed.strip_prefix("ws://") {
        (rest, 80)
    } else {
        bail!("copy watcher websocket URL must start with ws:// or wss://");
    };
    let authority = rest.split('/').next().unwrap_or_default();
    parse_copy_watcher_authority(authority, default_port, "websocket URL")
}

fn copy_watcher_http_proxy_endpoint(proxy_url: &str) -> Result<CopyWatcherEndpoint> {
    let trimmed = proxy_url.trim();
    let rest = if let Some(rest) = trimmed.strip_prefix("http://") {
        rest
    } else if trimmed.contains("://") {
        bail!("copy watcher only supports HTTP proxy URLs");
    } else {
        trimmed
    };
    let authority = rest.split('/').next().unwrap_or_default();
    parse_copy_watcher_authority(authority, 8080, "HTTP proxy URL")
}

fn parse_copy_watcher_authority(
    authority: &str,
    default_port: u16,
    label: &str,
) -> Result<CopyWatcherEndpoint> {
    let authority = authority.trim();
    if authority.is_empty() {
        bail!("copy watcher {label} is missing host");
    }
    if authority.contains('@') {
        bail!("authenticated {label} values are not supported");
    }
    if authority.starts_with('[') {
        let Some(end) = authority.find(']') else {
            bail!("copy watcher {label} has invalid IPv6 host");
        };
        let host = authority[1..end].trim();
        if host.is_empty() {
            bail!("copy watcher {label} is missing IPv6 host");
        }
        let suffix = authority[end + 1..].trim();
        let port = if suffix.is_empty() {
            default_port
        } else {
            let Some(port_text) = suffix.strip_prefix(':') else {
                bail!("copy watcher {label} has invalid IPv6 authority");
            };
            parse_copy_watcher_port(port_text, label)?
        };
        return Ok(CopyWatcherEndpoint {
            host: host.to_string(),
            port,
        });
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port_text)) if !port_text.is_empty() => {
            (host.trim(), parse_copy_watcher_port(port_text, label)?)
        }
        Some((_, _)) => bail!("copy watcher {label} has empty port"),
        None => (authority, default_port),
    };
    let host = host.trim();
    if host.is_empty() {
        bail!("copy watcher {label} is missing host");
    }
    Ok(CopyWatcherEndpoint {
        host: host.to_string(),
        port,
    })
}

fn parse_copy_watcher_port(port_text: &str, label: &str) -> Result<u16> {
    let port = port_text
        .trim()
        .parse::<u16>()
        .with_context(|| format!("copy watcher {label} has invalid port"))?;
    if port == 0 {
        bail!("copy watcher {label} port must be greater than zero");
    }
    Ok(port)
}

fn redact_copy_watcher_proxy_url(proxy_url: &str) -> String {
    let trimmed = proxy_url.trim();
    if let Some((scheme, rest)) = trimmed.split_once("://") {
        if let Some((_, host)) = rest.rsplit_once('@') {
            return format!("{scheme}://***@{host}");
        }
    }
    trimmed.to_string()
}

fn copy_watcher_fill_events(
    leader_by_address: &HashMap<String, &SmartMoneyLeaderWatch>,
    user: &str,
    fills: Vec<crate::hyperliquid::UserFill>,
    is_snapshot: bool,
    received_at_ms: u64,
) -> Vec<CopyLeaderWatcherEvent> {
    let Some(leader) = leader_by_address.get(&user.to_ascii_lowercase()) else {
        return Vec::new();
    };
    fills
        .iter()
        .filter_map(|fill| {
            leader_fill_event_from_user_fill(
                &leader.leader_id,
                &leader.leader_address,
                fill,
                received_at_ms,
            )
        })
        .map(|fill| CopyLeaderWatcherEvent::Fill {
            leader_id: leader.leader_id.clone(),
            leader_address: leader.leader_address.clone(),
            fill,
            is_snapshot,
        })
        .collect()
}

fn leader_for_single_user_stream<'a>(
    leader_by_address: &'a HashMap<String, &'a SmartMoneyLeaderWatch>,
) -> Option<&'a SmartMoneyLeaderWatch> {
    if leader_by_address.len() == 1 {
        leader_by_address.values().next().copied()
    } else {
        None
    }
}

fn normalized_optional_dex(dex: Option<&str>) -> Option<String> {
    dex.map(str::trim)
        .filter(|dex| !dex.is_empty())
        .map(str::to_ascii_lowercase)
}

fn watcher_market_for_dex(dex: Option<&str>) -> Option<String> {
    match normalized_optional_dex(dex) {
        Some(dex) if dex == "spot" => Some("spot".to_string()),
        Some(dex) => Some(format!("{dex}_perp")),
        None => Some("hl_perp".to_string()),
    }
}

pub(crate) fn copy_watcher_ws_url(override_url: Option<&str>, environment: &str) -> String {
    if let Some(url) = override_url
        && !url.trim().is_empty()
    {
        return url.trim().to_string();
    }
    if environment.trim().eq_ignore_ascii_case("testnet") {
        "wss://api.hyperliquid-testnet.xyz/ws".to_string()
    } else {
        "wss://api.hyperliquid.xyz/ws".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_watcher_ws_endpoint_parses_defaults_and_path() {
        assert_eq!(
            copy_watcher_ws_endpoint("wss://api.hyperliquid.xyz/ws").unwrap(),
            CopyWatcherEndpoint {
                host: "api.hyperliquid.xyz".to_string(),
                port: 443,
            }
        );
        assert_eq!(
            copy_watcher_ws_endpoint("ws://127.0.0.1:9001/ws?token=x").unwrap(),
            CopyWatcherEndpoint {
                host: "127.0.0.1".to_string(),
                port: 9001,
            }
        );
    }

    #[test]
    fn copy_watcher_http_proxy_endpoint_parses_common_local_proxy_values() {
        assert_eq!(
            copy_watcher_http_proxy_endpoint("http://127.0.0.1:7890").unwrap(),
            CopyWatcherEndpoint {
                host: "127.0.0.1".to_string(),
                port: 7890,
            }
        );
        assert_eq!(
            copy_watcher_http_proxy_endpoint("localhost").unwrap(),
            CopyWatcherEndpoint {
                host: "localhost".to_string(),
                port: 8080,
            }
        );
    }

    #[test]
    fn copy_watcher_http_proxy_endpoint_rejects_unsupported_proxy_schemes() {
        let err = copy_watcher_http_proxy_endpoint("socks5://127.0.0.1:7890")
            .unwrap_err()
            .to_string();
        assert!(err.contains("only supports HTTP proxy URLs"));
    }

    #[test]
    fn copy_watcher_proxy_url_redacts_credentials() {
        assert_eq!(
            redact_copy_watcher_proxy_url("http://user:pass@127.0.0.1:7890"),
            "http://***@127.0.0.1:7890"
        );
    }

    #[test]
    fn watcher_market_for_dex_maps_supported_markets() {
        assert_eq!(watcher_market_for_dex(None).as_deref(), Some("hl_perp"));
        assert_eq!(
            watcher_market_for_dex(Some("xyz")).as_deref(),
            Some("xyz_perp")
        );
        assert_eq!(
            watcher_market_for_dex(Some("spot")).as_deref(),
            Some("spot")
        );
    }
}
