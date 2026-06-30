use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::{
    config::{AppConfig, MARKET_CASH_PERP, MARKET_HL_PERP, MARKET_SPOT, MARKET_XYZ_PERP},
    domain::now_ms,
    hyperliquid::{
        ClearinghouseState, OpenOrder, SpotClearinghouseState, UserFill, fetch_clearinghouse_state,
        fetch_default_clearinghouse_state, fetch_open_orders, fetch_spot_clearinghouse_state,
    },
};

const REALTIME_FRESH_MS: u64 = 30_000;
const REALTIME_MAX_FILLS: usize = 200;
const WS_RECONNECT_BASE_MS: u64 = 1_000;
const WS_RECONNECT_MAX_MS: u64 = 30_000;
const WS_HEARTBEAT_SECS: u64 = 30;

#[derive(Debug, Clone)]
pub struct RealtimeState {
    inner: Arc<RwLock<RealtimeInner>>,
}

#[derive(Debug, Default)]
struct RealtimeInner {
    started_at_ms: u64,
    health: HashMap<String, RealtimeStreamHealth>,
    mids: HashMap<String, RealtimeMidsEntry>,
    active_asset_ctxs: HashMap<String, RealtimeAssetCtxEntry>,
    open_orders: HashMap<String, RealtimeOpenOrdersEntry>,
    clearinghouse_states: HashMap<String, RealtimeClearinghouseEntry>,
    spot_states: HashMap<String, RealtimeSpotStateEntry>,
    fills: HashMap<String, RealtimeFillsEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct RealtimeStreamHealth {
    pub connected: bool,
    pub last_message_ms: Option<u64>,
    pub last_error: Option<String>,
    pub reconnect_count: u64,
}

#[derive(Debug, Clone)]
struct RealtimeMidsEntry {
    mids: HashMap<String, String>,
    updated_at_ms: u64,
}

#[derive(Debug, Clone)]
struct RealtimeAssetCtxEntry {
    value: Value,
    updated_at_ms: u64,
}

#[derive(Debug, Clone)]
struct RealtimeOpenOrdersEntry {
    orders: Vec<OpenOrder>,
    updated_at_ms: u64,
}

#[derive(Debug, Clone)]
struct RealtimeClearinghouseEntry {
    state: ClearinghouseState,
    updated_at_ms: u64,
}

#[derive(Debug, Clone)]
struct RealtimeSpotStateEntry {
    state: SpotClearinghouseState,
    updated_at_ms: u64,
}

#[derive(Debug, Clone)]
struct RealtimeFillsEntry {
    fills: Vec<UserFill>,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RealtimeStatus {
    pub started_at_ms: u64,
    pub fetched_at_ms: u64,
    pub streams: HashMap<String, RealtimeStreamStatus>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RealtimeStreamStatus {
    pub connected: bool,
    pub last_message_ms: Option<u64>,
    pub last_error: Option<String>,
    pub reconnect_count: u64,
}

#[derive(Debug, Deserialize)]
struct WsEnvelope {
    channel: String,
    #[serde(default)]
    data: Value,
}

#[derive(Debug, Deserialize)]
struct WsAllMidsData {
    #[serde(default)]
    mids: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsOpenOrdersData {
    #[serde(default)]
    dex: Option<String>,
    user: String,
    #[serde(default)]
    orders: Vec<OpenOrder>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsAllDexsClearinghouseData {
    user: String,
    clearinghouse_states: WsAllDexsClearinghouseStates,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WsAllDexsClearinghouseStates {
    Map(HashMap<String, ClearinghouseState>),
    Pairs(Vec<(String, ClearinghouseState)>),
}

impl WsAllDexsClearinghouseStates {
    fn into_pairs(self) -> Vec<(String, ClearinghouseState)> {
        match self {
            Self::Map(states) => states.into_iter().collect(),
            Self::Pairs(states) => states,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsClearinghouseData {
    #[serde(default)]
    dex: Option<String>,
    user: String,
    #[serde(flatten)]
    state: ClearinghouseState,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsSpotStateData {
    user: String,
    spot_state: SpotClearinghouseState,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsUserFillsData {
    #[serde(default)]
    is_snapshot: Option<bool>,
    user: String,
    #[serde(default)]
    fills: Vec<UserFill>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsOrderUpdate {
    order: WsBasicOrder,
    status: String,
    status_timestamp: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsBasicOrder {
    coin: String,
    side: String,
    limit_px: String,
    sz: String,
    oid: u64,
    timestamp: u64,
    orig_sz: String,
    #[serde(default)]
    cloid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WsNonUserCancel {
    coin: String,
    oid: u64,
}

impl RealtimeState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RealtimeInner {
                started_at_ms: now_ms(),
                ..RealtimeInner::default()
            })),
        }
    }

    pub fn status(&self) -> RealtimeStatus {
        let inner = self.inner.read().expect("realtime state lock poisoned");
        RealtimeStatus {
            started_at_ms: inner.started_at_ms,
            fetched_at_ms: now_ms(),
            streams: inner
                .health
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        RealtimeStreamStatus {
                            connected: value.connected,
                            last_message_ms: value.last_message_ms,
                            last_error: value.last_error.clone(),
                            reconnect_count: value.reconnect_count,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn mark_stream_connected(&self, stream_id: &str, connected: bool) {
        if let Ok(mut inner) = self.inner.write() {
            let entry = inner.health.entry(stream_id.to_string()).or_default();
            entry.connected = connected;
            if connected {
                entry.last_error = None;
            }
        }
    }

    pub fn mark_stream_message(&self, stream_id: &str) {
        if let Ok(mut inner) = self.inner.write() {
            let entry = inner.health.entry(stream_id.to_string()).or_default();
            entry.connected = true;
            entry.last_message_ms = Some(now_ms());
        }
    }

    pub fn mark_stream_error(&self, stream_id: &str, error: impl Into<String>) {
        if let Ok(mut inner) = self.inner.write() {
            let entry = inner.health.entry(stream_id.to_string()).or_default();
            entry.connected = false;
            entry.last_error = Some(error.into());
            entry.reconnect_count = entry.reconnect_count.saturating_add(1);
        }
    }

    pub fn update_mids(&self, market_id: &str, mids: HashMap<String, String>) {
        if let Ok(mut inner) = self.inner.write() {
            inner.mids.insert(
                market_id.to_string(),
                RealtimeMidsEntry {
                    mids,
                    updated_at_ms: now_ms(),
                },
            );
        }
    }

    pub fn mid_price(&self, market_id: &str, coin_candidates: &[String]) -> Option<f64> {
        let inner = self.inner.read().ok()?;
        let entry = inner.mids.get(market_id)?;
        if !fresh(entry.updated_at_ms) {
            return None;
        }
        coin_candidates.iter().find_map(|coin| {
            entry
                .mids
                .get(coin)
                .or_else(|| entry.mids.get(&coin.to_ascii_uppercase()))
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|value| *value > 0.0)
        })
    }

    pub fn update_active_asset_ctx(&self, market_id: &str, coin: &str, value: Value) {
        if let Ok(mut inner) = self.inner.write() {
            inner.active_asset_ctxs.insert(
                asset_key(market_id, coin),
                RealtimeAssetCtxEntry {
                    value,
                    updated_at_ms: now_ms(),
                },
            );
        }
    }

    pub fn active_asset_ctx(&self, market_id: &str, coin: &str) -> Option<Value> {
        let inner = self.inner.read().ok()?;
        let entry = inner.active_asset_ctxs.get(&asset_key(market_id, coin))?;
        fresh(entry.updated_at_ms).then(|| entry.value.clone())
    }

    pub fn replace_open_orders(&self, market_id: &str, address: &str, orders: Vec<OpenOrder>) {
        if let Ok(mut inner) = self.inner.write() {
            inner.open_orders.insert(
                account_market_key(market_id, address),
                RealtimeOpenOrdersEntry {
                    orders,
                    updated_at_ms: now_ms(),
                },
            );
        }
    }

    pub fn open_orders(&self, market_id: &str, address: &str) -> Option<Vec<OpenOrder>> {
        let inner = self.inner.read().ok()?;
        let entry = inner
            .open_orders
            .get(&account_market_key(market_id, address))?;
        fresh(entry.updated_at_ms).then(|| entry.orders.clone())
    }

    fn apply_order_update(&self, address: &str, update: WsOrderUpdate) {
        let market_id = classify_coin_market(&update.order.coin);
        if order_update_is_open(&update.status) {
            let order = open_order_from_ws(update);
            self.upsert_open_order(market_id, address, order);
        } else {
            self.remove_open_order(
                address,
                Some(&update.order.coin),
                Some(update.order.oid),
                update.order.cloid.as_deref(),
            );
        }
    }

    fn upsert_open_order(&self, market_id: &str, address: &str, order: OpenOrder) {
        if let Ok(mut inner) = self.inner.write() {
            let key = account_market_key(market_id, address);
            let entry = inner
                .open_orders
                .entry(key)
                .or_insert_with(|| RealtimeOpenOrdersEntry {
                    orders: Vec::new(),
                    updated_at_ms: now_ms(),
                });
            if let Some(existing) = entry.orders.iter_mut().find(|existing| {
                existing.oid == order.oid
                    || existing
                        .cloid
                        .as_deref()
                        .zip(order.cloid.as_deref())
                        .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
            }) {
                *existing = merge_open_order(existing, order);
            } else {
                entry.orders.push(order);
            }
            entry.updated_at_ms = now_ms();
        }
    }

    pub fn remove_open_order(
        &self,
        address: &str,
        coin: Option<&str>,
        oid: Option<u64>,
        cloid: Option<&str>,
    ) {
        if let Ok(mut inner) = self.inner.write() {
            let markets = if let Some(coin) = coin {
                vec![classify_coin_market(coin).to_string()]
            } else {
                vec![
                    MARKET_HL_PERP.to_string(),
                    MARKET_XYZ_PERP.to_string(),
                    MARKET_SPOT.to_string(),
                ]
            };
            for market_id in markets {
                let key = account_market_key(&market_id, address);
                if let Some(entry) = inner.open_orders.get_mut(&key) {
                    entry.orders.retain(|order| {
                        if let Some(oid) = oid
                            && order.oid == oid
                        {
                            return false;
                        }
                        if let Some(cloid) = cloid
                            && order
                                .cloid
                                .as_deref()
                                .is_some_and(|order_cloid| order_cloid.eq_ignore_ascii_case(cloid))
                        {
                            return false;
                        }
                        true
                    });
                    entry.updated_at_ms = now_ms();
                }
            }
        }
    }

    pub fn update_clearinghouse_state(
        &self,
        market_id: &str,
        address: &str,
        state: ClearinghouseState,
    ) {
        if let Ok(mut inner) = self.inner.write() {
            inner.clearinghouse_states.insert(
                account_market_key(market_id, address),
                RealtimeClearinghouseEntry {
                    state,
                    updated_at_ms: now_ms(),
                },
            );
        }
    }

    pub fn clearinghouse_state(
        &self,
        market_id: &str,
        address: &str,
    ) -> Option<ClearinghouseState> {
        let inner = self.inner.read().ok()?;
        let entry = inner
            .clearinghouse_states
            .get(&account_market_key(market_id, address))?;
        fresh(entry.updated_at_ms).then(|| entry.state.clone())
    }

    pub fn update_spot_state(&self, address: &str, state: SpotClearinghouseState) {
        if let Ok(mut inner) = self.inner.write() {
            inner.spot_states.insert(
                normalize_address(address),
                RealtimeSpotStateEntry {
                    state,
                    updated_at_ms: now_ms(),
                },
            );
        }
    }

    pub fn spot_state(&self, address: &str) -> Option<SpotClearinghouseState> {
        let inner = self.inner.read().ok()?;
        let entry = inner.spot_states.get(&normalize_address(address))?;
        fresh(entry.updated_at_ms).then(|| entry.state.clone())
    }

    pub fn update_fills(
        &self,
        address: &str,
        market_id: &str,
        fills: Vec<UserFill>,
        snapshot: bool,
    ) {
        if fills.is_empty() && !snapshot {
            return;
        }
        if let Ok(mut inner) = self.inner.write() {
            let key = account_market_key(market_id, address);
            let entry = inner
                .fills
                .entry(key)
                .or_insert_with(|| RealtimeFillsEntry {
                    fills: Vec::new(),
                    updated_at_ms: now_ms(),
                });
            if snapshot {
                entry.fills.clear();
            }
            let mut seen = entry
                .fills
                .iter()
                .map(fill_identity)
                .collect::<HashSet<_>>();
            for fill in fills {
                if seen.insert(fill_identity(&fill)) {
                    entry.fills.push(fill);
                }
            }
            entry.fills.sort_by_key(|fill| fill.time);
            if entry.fills.len() > REALTIME_MAX_FILLS {
                let overflow = entry.fills.len().saturating_sub(REALTIME_MAX_FILLS);
                entry.fills.drain(0..overflow);
            }
            entry.updated_at_ms = now_ms();
        }
    }

    pub fn fills(&self, market_id: &str, address: &str) -> Option<Vec<UserFill>> {
        let inner = self.inner.read().ok()?;
        let entry = inner.fills.get(&account_market_key(market_id, address))?;
        Some(entry.fills.clone())
    }
}

impl Default for RealtimeState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn spawn_realtime_runtime(config: AppConfig, state: RealtimeState) {
    let default_market = RealtimeMarketScope {
        market_id: MARKET_HL_PERP.to_string(),
        dex: String::new(),
    };
    let xyz_market = RealtimeMarketScope {
        market_id: MARKET_XYZ_PERP.to_string(),
        dex: config.hyperliquid.dex.clone(),
    };
    for market in [default_market, xyz_market] {
        let state = state.clone();
        let environment = config.app.environment.clone();
        tokio::spawn(async move {
            run_all_mids_stream(state, environment, market).await;
        });
    }

    for (index, account) in config
        .accounts
        .iter()
        .filter(|account| account.enabled && account.worker_enabled)
        .enumerate()
    {
        let state = state.clone();
        let config = config.clone();
        let account_id = account.account_id.clone();
        let address = account.address.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis((index as u64).saturating_mul(750))).await;
            seed_account_snapshot(&state, &config, &account_id, &address).await;
            run_account_stream(state, config, account_id, address).await;
        });
    }
}

#[derive(Debug, Clone)]
struct RealtimeMarketScope {
    market_id: String,
    dex: String,
}

async fn run_all_mids_stream(
    state: RealtimeState,
    environment: String,
    market: RealtimeMarketScope,
) {
    let stream_id = format!("allMids:{}", market.market_id);
    let mut attempt = 0_u64;
    loop {
        match run_all_mids_stream_once(&state, &environment, &market, &stream_id).await {
            Ok(()) => state.mark_stream_error(&stream_id, "websocket stream ended"),
            Err(error) => state.mark_stream_error(&stream_id, format!("{error:#}")),
        }
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(reconnect_delay(attempt)).await;
    }
}

async fn run_all_mids_stream_once(
    state: &RealtimeState,
    environment: &str,
    market: &RealtimeMarketScope,
    stream_id: &str,
) -> Result<()> {
    let (mut writer, mut reader) = connect_ws(environment).await?;
    state.mark_stream_connected(stream_id, true);
    let mut subscription = json!({ "type": "allMids" });
    if !market.dex.trim().is_empty() {
        subscription["dex"] = json!(market.dex.trim().to_ascii_lowercase());
    }
    send_subscribe(&mut writer, subscription).await?;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(WS_HEARTBEAT_SECS));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                send_ping(&mut writer).await?;
            }
            message = reader.next() => {
                let Some(message) = message else { break };
                let text = match message? {
                    Message::Text(text) => text,
                    Message::Ping(payload) => {
                        writer.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => break,
                    _ => continue,
                };
                let envelope: WsEnvelope = serde_json::from_str(&text)?;
                if envelope.channel.eq_ignore_ascii_case("allMids") {
                    let data: WsAllMidsData = serde_json::from_value(envelope.data)?;
                    state.update_mids(&market.market_id, data.mids);
                    state.mark_stream_message(stream_id);
                }
            }
        }
    }
    Ok(())
}

async fn run_account_stream(
    state: RealtimeState,
    config: AppConfig,
    account_id: String,
    address: String,
) {
    let stream_id = format!("account:{account_id}");
    let mut attempt = 0_u64;
    loop {
        match run_account_stream_once(&state, &config, &address, &stream_id).await {
            Ok(()) => state.mark_stream_error(&stream_id, "websocket stream ended"),
            Err(error) => state.mark_stream_error(&stream_id, format!("{error:#}")),
        }
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(reconnect_delay(attempt)).await;
        seed_account_snapshot(&state, &config, &account_id, &address).await;
    }
}

async fn run_account_stream_once(
    state: &RealtimeState,
    config: &AppConfig,
    address: &str,
    stream_id: &str,
) -> Result<()> {
    let (mut writer, mut reader) = connect_ws(&config.app.environment).await?;
    state.mark_stream_connected(stream_id, true);
    send_subscribe(
        &mut writer,
        json!({ "type": "openOrders", "user": address }),
    )
    .await?;
    if !config.hyperliquid.dex.trim().is_empty() {
        send_subscribe(
            &mut writer,
            json!({
                "type": "openOrders",
                "user": address,
                "dex": config.hyperliquid.dex.trim().to_ascii_lowercase(),
            }),
        )
        .await?;
    }
    send_subscribe(
        &mut writer,
        json!({ "type": "orderUpdates", "user": address }),
    )
    .await?;
    send_subscribe(&mut writer, json!({ "type": "userFills", "user": address })).await?;
    send_subscribe(
        &mut writer,
        json!({ "type": "userEvents", "user": address }),
    )
    .await?;
    send_subscribe(
        &mut writer,
        json!({ "type": "allDexsClearinghouseState", "user": address }),
    )
    .await?;
    send_subscribe(&mut writer, json!({ "type": "spotState", "user": address })).await?;

    let mut heartbeat = tokio::time::interval(Duration::from_secs(WS_HEARTBEAT_SECS));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                send_ping(&mut writer).await?;
            }
            message = reader.next() => {
                let Some(message) = message else { break };
                let text = match message? {
                    Message::Text(text) => text,
                    Message::Ping(payload) => {
                        writer.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => break,
                    _ => continue,
                };
                handle_account_ws_message(state, config, address, stream_id, &text).with_context(|| {
                    format!("failed to process account websocket message for {address}")
                })?;
            }
        }
    }
    Ok(())
}

fn handle_account_ws_message(
    state: &RealtimeState,
    config: &AppConfig,
    address: &str,
    stream_id: &str,
    text: &str,
) -> Result<()> {
    if !text.trim_start().starts_with('{') {
        return Ok(());
    }
    let envelope: WsEnvelope = serde_json::from_str(text)?;
    match envelope.channel.as_str() {
        "openOrders" => {
            let data: WsOpenOrdersData = serde_json::from_value(envelope.data)?;
            apply_open_orders_snapshot(state, config, &data);
            state.mark_stream_message(stream_id);
        }
        "orderUpdates" => {
            let updates: Vec<WsOrderUpdate> = serde_json::from_value(envelope.data)?;
            for update in updates {
                state.apply_order_update(address, update);
            }
            state.mark_stream_message(stream_id);
        }
        "userFills" => {
            let data: WsUserFillsData = serde_json::from_value(envelope.data)?;
            apply_user_fills(
                state,
                &data.user,
                data.fills,
                data.is_snapshot.unwrap_or(false),
            );
            state.mark_stream_message(stream_id);
        }
        "user" | "userEvents" => {
            apply_user_event(state, address, envelope.data)?;
            state.mark_stream_message(stream_id);
        }
        "allDexsClearinghouseState" => {
            let data: WsAllDexsClearinghouseData = serde_json::from_value(envelope.data)?;
            for (dex, state_value) in data.clearinghouse_states.into_pairs() {
                if let Some(market_id) =
                    market_id_for_clearinghouse_dex(&dex, &config.hyperliquid.dex)
                {
                    state.update_clearinghouse_state(market_id, &data.user, state_value);
                }
            }
            state.mark_stream_message(stream_id);
        }
        "clearinghouseState" => {
            let data: WsClearinghouseData = serde_json::from_value(envelope.data)?;
            let market_id = market_id_for_clearinghouse_dex(
                data.dex.as_deref().unwrap_or(""),
                &config.hyperliquid.dex,
            )
            .unwrap_or(MARKET_HL_PERP);
            state.update_clearinghouse_state(market_id, &data.user, data.state);
            state.mark_stream_message(stream_id);
        }
        "spotState" => {
            let data: WsSpotStateData = serde_json::from_value(envelope.data)?;
            state.update_spot_state(&data.user, data.spot_state);
            state.mark_stream_message(stream_id);
        }
        "subscriptionResponse" | "pong" => {}
        other => {
            if other.eq_ignore_ascii_case("error") {
                state.mark_stream_error(stream_id, text.to_string());
            }
        }
    }
    Ok(())
}

fn apply_open_orders_snapshot(state: &RealtimeState, config: &AppConfig, data: &WsOpenOrdersData) {
    let mut by_market: HashMap<String, Vec<OpenOrder>> = HashMap::new();
    for order in data.orders.clone() {
        by_market
            .entry(classify_open_order_market(config, data.dex.as_deref(), &order).to_string())
            .or_default()
            .push(order);
    }
    let target_markets = if data
        .dex
        .as_deref()
        .is_some_and(|dex| dex.eq_ignore_ascii_case(&config.hyperliquid.dex))
    {
        vec![MARKET_XYZ_PERP.to_string()]
    } else if data
        .dex
        .as_deref()
        .is_some_and(|dex| dex.eq_ignore_ascii_case("cash"))
    {
        vec![MARKET_CASH_PERP.to_string()]
    } else {
        vec![MARKET_HL_PERP.to_string(), MARKET_SPOT.to_string()]
    };
    for market_id in target_markets {
        state.replace_open_orders(
            &market_id,
            &data.user,
            by_market.remove(&market_id).unwrap_or_default(),
        );
    }
}

fn apply_user_event(state: &RealtimeState, address: &str, data: Value) -> Result<()> {
    if let Some(fills) = data.get("fills").cloned() {
        let fills: Vec<UserFill> = serde_json::from_value(fills)?;
        apply_user_fills(state, address, fills, false);
    }
    if let Some(cancels) = data.get("nonUserCancel").cloned() {
        let cancels: Vec<WsNonUserCancel> = serde_json::from_value(cancels)?;
        for cancel in cancels {
            state.remove_open_order(address, Some(&cancel.coin), Some(cancel.oid), None);
        }
    }
    Ok(())
}

fn apply_user_fills(state: &RealtimeState, address: &str, fills: Vec<UserFill>, snapshot: bool) {
    let mut by_market: HashMap<String, Vec<UserFill>> = HashMap::new();
    for fill in fills {
        by_market
            .entry(classify_coin_market(&fill.coin).to_string())
            .or_default()
            .push(fill);
    }
    for (market_id, fills) in by_market {
        state.update_fills(address, &market_id, fills, snapshot);
    }
}

async fn seed_account_snapshot(
    state: &RealtimeState,
    config: &AppConfig,
    account_id: &str,
    address: &str,
) {
    if let Err(error) = seed_account_snapshot_inner(state, config, address).await {
        tracing::warn!(%account_id, %address, error = %format!("{error:#}"), "failed to seed realtime account snapshot");
    }
}

async fn seed_account_snapshot_inner(
    state: &RealtimeState,
    config: &AppConfig,
    address: &str,
) -> Result<()> {
    match fetch_open_orders(&config.app.environment, "", address).await {
        Ok(default_orders) => {
            let spot_and_default = split_default_and_spot_orders(default_orders);
            state.replace_open_orders(MARKET_HL_PERP, address, spot_and_default.default_perp);
            state.replace_open_orders(MARKET_SPOT, address, spot_and_default.spot);
        }
        Err(error) => {
            tracing::warn!(
                %address,
                error = %format!("{error:#}"),
                "failed to seed default/spot open orders; preserving existing realtime cache"
            );
        }
    }
    match fetch_open_orders(&config.app.environment, &config.hyperliquid.dex, address).await {
        Ok(xyz_orders) => state.replace_open_orders(MARKET_XYZ_PERP, address, xyz_orders),
        Err(error) => {
            tracing::warn!(
                %address,
                dex = %config.hyperliquid.dex,
                error = %format!("{error:#}"),
                "failed to seed dex open orders; preserving existing realtime cache"
            );
        }
    }
    match fetch_open_orders(&config.app.environment, "cash", address).await {
        Ok(cash_orders) => state.replace_open_orders(MARKET_CASH_PERP, address, cash_orders),
        Err(error) => {
            tracing::warn!(
                %address,
                dex = "cash",
                error = %format!("{error:#}"),
                "failed to seed dex open orders; preserving existing realtime cache"
            );
        }
    }

    if let Ok(default_state) =
        fetch_default_clearinghouse_state(&config.app.environment, address).await
    {
        state.update_clearinghouse_state(MARKET_HL_PERP, address, default_state);
    }
    if let Ok(xyz_state) =
        fetch_clearinghouse_state(&config.app.environment, &config.hyperliquid.dex, address).await
    {
        state.update_clearinghouse_state(MARKET_XYZ_PERP, address, xyz_state);
    }
    if let Ok(cash_state) =
        fetch_clearinghouse_state(&config.app.environment, "cash", address).await
    {
        state.update_clearinghouse_state(MARKET_CASH_PERP, address, cash_state);
    }
    if let Ok(spot_state) = fetch_spot_clearinghouse_state(&config.app.environment, address).await {
        state.update_spot_state(address, spot_state);
    }
    Ok(())
}

struct SplitOrders {
    default_perp: Vec<OpenOrder>,
    spot: Vec<OpenOrder>,
}

fn split_default_and_spot_orders(orders: Vec<OpenOrder>) -> SplitOrders {
    let mut default_perp = Vec::new();
    let mut spot = Vec::new();
    for order in orders {
        if classify_coin_market(&order.coin) == MARKET_SPOT {
            spot.push(order);
        } else {
            default_perp.push(order);
        }
    }
    SplitOrders { default_perp, spot }
}

async fn connect_ws(
    environment: &str,
) -> Result<(
    futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
)> {
    let (stream, _) = connect_async(ws_url(environment)).await?;
    Ok(stream.split())
}

async fn send_subscribe(
    writer: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    subscription: Value,
) -> Result<()> {
    writer
        .send(Message::Text(
            json!({
                "method": "subscribe",
                "subscription": subscription,
            })
            .to_string(),
        ))
        .await?;
    Ok(())
}

async fn send_ping(
    writer: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
) -> Result<()> {
    writer
        .send(Message::Text(json!({ "method": "ping" }).to_string()))
        .await?;
    Ok(())
}

fn ws_url(environment: &str) -> &'static str {
    if environment.trim().eq_ignore_ascii_case("testnet") {
        "wss://api.hyperliquid-testnet.xyz/ws"
    } else {
        "wss://api.hyperliquid.xyz/ws"
    }
}

fn reconnect_delay(attempt: u64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(5) as u32;
    let delay = WS_RECONNECT_BASE_MS
        .saturating_mul(2_u64.saturating_pow(exponent))
        .min(WS_RECONNECT_MAX_MS);
    let jitter = (now_ms() % 377).saturating_add((attempt * 113) % 997);
    Duration::from_millis(delay.saturating_add(jitter))
}

fn classify_open_order_market<'a>(
    config: &'a AppConfig,
    dex: Option<&str>,
    order: &OpenOrder,
) -> &'a str {
    if dex.is_some_and(|value| value.eq_ignore_ascii_case(&config.hyperliquid.dex)) {
        MARKET_XYZ_PERP
    } else {
        classify_coin_market(&order.coin)
    }
}

fn classify_coin_market(coin: &str) -> &'static str {
    let coin = coin.trim();
    if let Some((dex, _symbol)) = coin.split_once(':') {
        if dex.trim().eq_ignore_ascii_case("cash") {
            MARKET_CASH_PERP
        } else {
            MARKET_XYZ_PERP
        }
    } else if coin.contains('/') || coin.starts_with('@') {
        MARKET_SPOT
    } else {
        MARKET_HL_PERP
    }
}

fn market_id_for_clearinghouse_dex(dex: &str, xyz_dex: &str) -> Option<&'static str> {
    let dex = dex.trim().to_ascii_lowercase();
    if dex.is_empty() {
        Some(MARKET_HL_PERP)
    } else if !xyz_dex.trim().is_empty() && dex == xyz_dex.trim().to_ascii_lowercase() {
        Some(MARKET_XYZ_PERP)
    } else if dex == "cash" {
        Some(MARKET_CASH_PERP)
    } else {
        None
    }
}

fn account_market_key(market_id: &str, address: &str) -> String {
    format!(
        "{}|{}",
        market_id.trim().to_ascii_lowercase(),
        normalize_address(address)
    )
}

fn asset_key(market_id: &str, coin: &str) -> String {
    format!(
        "{}|{}",
        market_id.trim().to_ascii_lowercase(),
        coin.trim().to_ascii_uppercase()
    )
}

fn normalize_address(address: &str) -> String {
    address.trim().to_ascii_lowercase()
}

fn fresh(updated_at_ms: u64) -> bool {
    now_ms().saturating_sub(updated_at_ms) <= REALTIME_FRESH_MS
}

fn order_update_is_open(status: &str) -> bool {
    let status = status.trim().to_ascii_lowercase();
    status == "open" || status == "resting" || status.contains("open")
}

fn open_order_from_ws(update: WsOrderUpdate) -> OpenOrder {
    OpenOrder {
        coin: update.order.coin,
        limit_px: update.order.limit_px,
        oid: update.order.oid,
        side: update.order.side,
        sz: update.order.sz,
        timestamp: update.order.timestamp.max(update.status_timestamp),
        trigger_condition: String::new(),
        is_trigger: false,
        trigger_px: String::new(),
        is_position_tpsl: false,
        reduce_only: false,
        order_type: "Limit".to_string(),
        orig_sz: update.order.orig_sz,
        cloid: update.order.cloid,
    }
}

fn merge_open_order(existing: &OpenOrder, mut next: OpenOrder) -> OpenOrder {
    next.trigger_condition = existing.trigger_condition.clone();
    next.is_trigger = existing.is_trigger;
    next.trigger_px = existing.trigger_px.clone();
    next.is_position_tpsl = existing.is_position_tpsl;
    next.reduce_only = existing.reduce_only;
    if next.order_type.trim().is_empty() || next.order_type == "Limit" {
        next.order_type = existing.order_type.clone();
    }
    next
}

fn fill_identity(fill: &UserFill) -> String {
    format!("{}:{}:{}:{}", fill.hash, fill.oid, fill.time, fill.coin)
}
