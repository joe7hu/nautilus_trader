// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{
    collections::HashMap,
    io::{self, BufWriter, Write},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::keccak256;
use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_hyperliquid::{
    common::{
        consts::{info_url, ws_url},
        enums::HyperliquidEnvironment,
    },
    http::{
        models::PerpMeta,
        parse::{create_instrument_from_def, parse_perp_instruments},
    },
    websocket::{client::HyperliquidWebSocketClient, messages::NautilusWsMessage},
};
use nautilus_model::{
    enums::{BookAction, OrderSide},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{
    SocketState, SocketStateSink,
    http::{HttpClient, HttpRedirectPolicy},
    websocket::TransportBackend,
};
use serde::Serialize;
use serde_json::{Value, json};

const CAMPAIGN_SECONDS: u64 = 80;
const RECONNECT_SECONDS: u64 = 40;
const SETUP_SECONDS: u64 = 30;
const MAX_EVENT_AGE_NS: u64 = 5_000_000_000;
const MAX_FUTURE_SKEW_NS: u64 = 1_000_000_000;

#[derive(Clone, Copy, Default, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    #[default]
    BeforeReconnect,
    ReconnectPending,
    AfterReconnected,
}

#[derive(Default, Serialize)]
struct Counts {
    trades: u64,
    quotes: u64,
    l2_snapshots: u64,
    fresh_trades: u64,
    fresh_quotes: u64,
    fresh_l2_snapshots: u64,
    non_millisecond_timestamps: u64,
    foreign_instruments: u64,
    invalid_l2_payloads: u64,
}

impl Counts {
    fn qualified(&self) -> bool {
        self.fresh_trades > 0
            && self.fresh_quotes > 0
            && self.fresh_l2_snapshots > 0
            && self.non_millisecond_timestamps == 0
            && self.foreign_instruments == 0
            && self.invalid_l2_payloads == 0
    }

    fn observe(&mut self, expected: InstrumentId, actual: InstrumentId, event: UnixNanos) {
        self.foreign_instruments += u64::from(expected != actual);
        self.non_millisecond_timestamps +=
            u64::from(event.as_u64() == 0 || !event.as_u64().is_multiple_of(1_000_000));
    }
}

#[derive(Default)]
struct Qualification {
    phase: Phase,
    before: Counts,
    pending: Counts,
    after: Counts,
    reconnect_requested: bool,
    reconnect_accepted: bool,
    post_request_reconnected: bool,
    native_reconnected_events: u64,
    transport_connected_edges: u64,
    transport_disconnected_edges: u64,
    post_request_connected_edges: u64,
    post_request_disconnected_edges: u64,
    native_errors: u64,
    unexpected_native_messages: u64,
}

impl Qualification {
    fn counts(&mut self) -> &mut Counts {
        match self.phase {
            Phase::BeforeReconnect => &mut self.before,
            Phase::ReconnectPending => &mut self.pending,
            Phase::AfterReconnected => &mut self.after,
        }
    }

    fn passed(&self) -> bool {
        self.reconnect_accepted
            && self.post_request_reconnected
            && self.post_request_connected_edges > 0
            && self.before.qualified()
            && self.after.qualified()
            && self.pending.non_millisecond_timestamps == 0
            && self.pending.foreign_instruments == 0
            && self.pending.invalid_l2_payloads == 0
            && self.native_errors == 0
            && self.unexpected_native_messages == 0
    }
}

struct SocketObservation {
    state: SocketState,
    callback_time: SystemTime,
    callback_elapsed: Duration,
}

struct Recorder {
    output: BufWriter<io::Stdout>,
    sequence: u64,
}

impl Recorder {
    fn write(
        &mut self,
        kind: &str,
        phase: Phase,
        received: u64,
        payload: Value,
    ) -> anyhow::Result<()> {
        self.sequence += 1;
        serde_json::to_writer(
            &mut self.output,
            &json!({
                "schema": "hyperliquid_btc_reconnect_v1",
                "sequence": self.sequence,
                "kind": kind,
                "phase": phase,
                "local_receipt_ns": received.to_string(),
                "payload": payload,
            }),
        )?;
        self.output.write_all(b"\n")?;
        self.output.flush()?;
        Ok(())
    }
}

fn unix_ns(time: SystemTime) -> anyhow::Result<u64> {
    u64::try_from(time.duration_since(UNIX_EPOCH)?.as_nanos())
        .context("observer system time exceeds UnixNanos")
}

fn now_ns() -> anyhow::Result<u64> {
    unix_ns(SystemTime::now())
}

fn clocks(event: UnixNanos, init: UnixNanos) -> Value {
    json!({
        "original_ts_event_ns": event.as_u64().to_string(),
        "original_ts_init_ns": init.as_u64().to_string(),
    })
}

fn fresh(event: UnixNanos, received: u64) -> bool {
    let event = event.as_u64();
    event > 0
        && received.saturating_sub(event) <= MAX_EVENT_AGE_NS
        && event.saturating_sub(received) <= MAX_FUTURE_SKEW_NS
}

fn source_hash(path: &str, source: &[u8]) -> Value {
    json!({"path": path, "algorithm": "keccak256", "hash": keccak256(source).to_string()})
}

async fn setup(
    client: &mut HyperliquidWebSocketClient,
    recorder: &mut Recorder,
) -> anyhow::Result<InstrumentId> {
    let http = HttpClient::builder()
        .headers(HashMap::from([(
            "Content-Type".to_string(),
            "application/json".to_string(),
        )]))
        .header_keys(vec![
            "date".to_string(),
            "content-type".to_string(),
            "server".to_string(),
        ])
        .timeout_secs(15)
        .redirect_policy(HttpRedirectPolicy::Reject)
        .use_system_proxy(false)
        .build()?;
    let request_started = now_ns()?;
    let response = http
        .post(
            info_url(HyperliquidEnvironment::Mainnet).to_string(),
            None,
            None,
            Some(br#"{"type":"meta"}"#.to_vec()),
            Some(15),
            None,
        )
        .await?;
    let metadata_received = now_ns()?;
    recorder.write(
        "original_http_metadata_response",
        Phase::BeforeReconnect,
        metadata_received,
        json!({
            "method": "POST",
            "endpoint": info_url(HyperliquidEnvironment::Mainnet),
            "original_request_body_utf8": r#"{"type":"meta"}"#,
            "request_started_ns": request_started.to_string(),
            "status": response.status.as_u16(),
            "selected_response_headers": response.headers,
            "original_response_body_utf8": std::str::from_utf8(&response.body)?,
            "original_response_body_keccak256": keccak256(&response.body).to_string(),
            "venue_event_clock": null,
        }),
    )?;
    anyhow::ensure!(
        response.status.as_u16() == 200,
        "public metadata HTTP status is not 200"
    );
    let metadata: PerpMeta = serde_json::from_slice(&response.body)?;
    let definitions = parse_perp_instruments(&metadata, 0).map_err(anyhow::Error::msg)?;
    let definition = definitions
        .iter()
        .find(|definition| definition.raw_symbol.as_str() == "BTC")
        .context("public mainnet metadata contains no BTC perpetual")?;
    let instrument = create_instrument_from_def(definition, UnixNanos::from(metadata_received))
        .context("native converter could not construct the public BTC perpetual")?;
    anyhow::ensure!(
        matches!(instrument, InstrumentAny::CryptoPerpetual(_)),
        "BTC is not a perpetual"
    );
    let instrument_id = instrument.id();
    recorder.write(
        "original_native_instrument",
        Phase::BeforeReconnect,
        now_ns()?,
        json!({
            "origin": "parse_perp_instruments + create_instrument_from_def",
            "original_native_definition": definition,
            "event_clock": "metadata_conversion_receipt_clock",
            "native": instrument,
            "original_metadata_response_retained": true,
        }),
    )?;
    client.cache_instruments(vec![instrument]);
    client.connect().await?;
    recorder.write(
        "connect_call_completed",
        Phase::BeforeReconnect,
        now_ns()?,
        json!({"source": "client.connect() return", "venue_event_clock": null}),
    )?;
    client.subscribe_trades(instrument_id).await?;
    client.subscribe_quotes(instrument_id).await?;
    client.subscribe_book(instrument_id).await?;
    client.subscribe_mark_prices(instrument_id).await?;
    client.subscribe_index_prices(instrument_id).await?;
    client.subscribe_funding_rates(instrument_id).await?;
    recorder.write(
        "subscriptions_requested",
        Phase::BeforeReconnect,
        now_ns()?,
        json!({
            "instrument_id": instrument_id,
            "channels": ["trades", "bbo", "l2Book", "activeAssetCtx"],
            "acknowledgements_exposed_by_native_api": false,
        }),
    )?;
    Ok(instrument_id)
}

fn record_socket(
    observation: SocketObservation,
    qualification: &mut Qualification,
    recorder: &mut Recorder,
    count_for_qualification: bool,
) -> anyhow::Result<()> {
    let state = match observation.state {
        SocketState::Connected => {
            if count_for_qualification {
                qualification.transport_connected_edges += 1;
                qualification.post_request_connected_edges += u64::from(
                    qualification.reconnect_requested
                        && observation.callback_elapsed >= Duration::from_secs(RECONNECT_SECONDS),
                );
            }
            "Connected"
        }
        SocketState::Disconnected => {
            if count_for_qualification {
                qualification.transport_disconnected_edges += 1;
                qualification.post_request_disconnected_edges += u64::from(
                    qualification.reconnect_requested
                        && observation.callback_elapsed >= Duration::from_secs(RECONNECT_SECONDS),
                );
            }
            "Disconnected"
        }
    };
    recorder.write(
        "transport_state",
        qualification.phase,
        now_ns()?,
        json!({
            "native_state": state,
            "callback_observed_ns": unix_ns(observation.callback_time)?.to_string(),
            "callback_elapsed_ns": observation.callback_elapsed.as_nanos().to_string(),
            "event_clock": "local_transport_callback_only",
            "original_ts_event_ns": null,
            "original_ts_init_ns": null,
            "counted_for_qualification": count_for_qualification,
        }),
    )
}

fn record_message(
    message: NautilusWsMessage,
    expected: InstrumentId,
    qualification: &mut Qualification,
    recorder: &mut Recorder,
) -> anyhow::Result<()> {
    let received = now_ns()?;
    let phase = qualification.phase;
    let (kind, payload) = match message {
        NautilusWsMessage::Trades(trades) => {
            let mut native_clocks = Vec::with_capacity(trades.len());

            for trade in &trades {
                let counts = qualification.counts();
                counts.trades += 1;
                counts.observe(expected, trade.instrument_id, trade.ts_event);
                counts.fresh_trades += u64::from(fresh(trade.ts_event, received));
                native_clocks.push(clocks(trade.ts_event, trade.ts_init));
            }

            (
                "Trades",
                json!({"native": trades, "original_clocks": native_clocks,
                "event_clock": "venue_milliseconds_scaled_to_nanoseconds"}),
            )
        }
        NautilusWsMessage::Quote(quote) => {
            let counts = qualification.counts();
            counts.quotes += 1;
            counts.observe(expected, quote.instrument_id, quote.ts_event);
            counts.fresh_quotes += u64::from(fresh(quote.ts_event, received));
            (
                "Quote",
                json!({"native": quote, "original_clocks": clocks(quote.ts_event, quote.ts_init),
                "event_clock": "venue_milliseconds_scaled_to_nanoseconds"}),
            )
        }
        NautilusWsMessage::Deltas(book) => {
            let clear = book
                .deltas
                .first()
                .is_some_and(|delta| delta.action == BookAction::Clear);
            let bid = book.deltas.iter().any(|delta| {
                delta.action == BookAction::Add
                    && delta.order.side == Some(OrderSide::Buy)
                    && delta.order.size.is_positive()
            });
            let ask = book.deltas.iter().any(|delta| {
                delta.action == BookAction::Add
                    && delta.order.side == Some(OrderSide::Sell)
                    && delta.order.size.is_positive()
            });
            let consistent = book.deltas.iter().all(|delta| {
                delta.instrument_id == expected
                    && delta.ts_event == book.ts_event
                    && delta.ts_init == book.ts_init
            });
            let snapshot = clear && bid && ask && consistent;
            let counts = qualification.counts();
            counts.observe(expected, book.instrument_id, book.ts_event);
            counts.invalid_l2_payloads += u64::from(!snapshot);
            counts.l2_snapshots += u64::from(snapshot);
            counts.fresh_l2_snapshots += u64::from(snapshot && fresh(book.ts_event, received));
            (
                "Deltas",
                json!({"native": book, "original_clocks": clocks(book.ts_event, book.ts_init),
                "event_clock": "venue_milliseconds_scaled_to_nanoseconds",
                "snapshot_clear_positive_bid_and_ask": snapshot}),
            )
        }
        NautilusWsMessage::MarkPrice(mark) => {
            qualification.counts().foreign_instruments += u64::from(mark.instrument_id != expected);
            (
                "MarkPrice",
                json!({"native": mark,
                "original_clocks": clocks(mark.ts_event, mark.ts_init),
                "event_clock": "adapter_receipt_clock", "venue_event_clock": null}),
            )
        }
        NautilusWsMessage::IndexPrice(index) => {
            qualification.counts().foreign_instruments +=
                u64::from(index.instrument_id != expected);
            (
                "IndexPrice",
                json!({"native": index,
                "original_clocks": clocks(index.ts_event, index.ts_init),
                "event_clock": "adapter_receipt_clock", "venue_event_clock": null}),
            )
        }
        NautilusWsMessage::FundingRate(funding) => {
            qualification.counts().foreign_instruments +=
                u64::from(funding.instrument_id != expected);
            (
                "FundingRate",
                json!({"native": funding,
                "original_clocks": clocks(funding.ts_event, funding.ts_init),
                "event_clock": "adapter_receipt_clock", "venue_event_clock": null,
                "settlement_boundary_qualified": false}),
            )
        }
        NautilusWsMessage::Reconnected => {
            qualification.native_reconnected_events += 1;

            if qualification.reconnect_accepted {
                qualification.post_request_reconnected = true;
                qualification.phase = Phase::AfterReconnected;
            }

            return recorder.write(
                "Reconnected",
                qualification.phase,
                received,
                json!({"native_variant": "Reconnected", "venue_event_clock": null,
                    "original_ts_event_ns": null, "original_ts_init_ns": null}),
            );
        }
        NautilusWsMessage::Error(error) => {
            qualification.native_errors += 1;
            ("Error", json!({"native": error, "venue_event_clock": null}))
        }
        other => {
            qualification.unexpected_native_messages += 1;
            (
                "unexpected_native_message",
                json!({"native_debug": format!("{other:?}"),
                "serialization": "Debug only: wrapper has no Serialize implementation"}),
            )
        }
    };
    recorder.write(kind, phase, received, payload)
}

async fn capture(
    client: &mut HyperliquidWebSocketClient,
    states: &mut tokio::sync::mpsc::UnboundedReceiver<SocketObservation>,
    instrument_id: InstrumentId,
    started: tokio::time::Instant,
    qualification: &mut Qualification,
    recorder: &mut Recorder,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(started + Duration::from_secs(CAMPAIGN_SECONDS)) => break,
            () = tokio::time::sleep_until(started + Duration::from_secs(RECONNECT_SECONDS)),
                if !qualification.reconnect_requested => {
                qualification.reconnect_requested = true;
                qualification.phase = Phase::ReconnectPending;
                qualification.reconnect_accepted = client.request_reconnect();
                recorder.write("reconnect_requested", qualification.phase, now_ns()?, json!({
                    "method": "HyperliquidWebSocketClient::request_reconnect",
                    "accepted": qualification.reconnect_accepted,
                    "elapsed_ns": started.elapsed().as_nanos().to_string(),
                }))?;
            }
            Some(observation) = states.recv() => {
                record_socket(observation, qualification, recorder, true)?;
            }
            message = client.next_event() => {
                let message = message.context("native WebSocket output ended before the deadline")?;
                record_message(message, instrument_id, qualification, recorder)?;
            }
        }
    }

    while let Ok(observation) = states.try_recv() {
        record_socket(observation, qualification, recorder, true)?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let started = tokio::time::Instant::now();
    let mut recorder = Recorder {
        output: BufWriter::new(io::stdout()),
        sequence: 0,
    };
    recorder.write("campaign_declaration", Phase::BeforeReconnect, now_ns()?, json!({
        "environment": "mainnet", "coin": "BTC", "credentials": false, "proxy": null,
        "http_endpoint": info_url(HyperliquidEnvironment::Mainnet),
        "http_system_proxy_enabled": false,
        "http_redirect_policy": "reject",
        "websocket_transport_backend": format!("{:?}", TransportBackend::default()),
        "websocket_endpoint": ws_url(HyperliquidEnvironment::Mainnet),
        "campaign_seconds_including_setup": CAMPAIGN_SECONDS,
        "setup_timeout_seconds": SETUP_SECONDS,
        "reconnect_at_elapsed_seconds": RECONNECT_SECONDS,
        "disconnect_timeout_seconds": 5,
        "max_event_age_ns": MAX_EVENT_AGE_NS.to_string(),
        "max_future_skew_ns": MAX_FUTURE_SKEW_NS.to_string(),
        "pass_criteria": {
            "fresh_btc_trades_quotes_and_clear_bid_ask_l2_snapshots_each_phase": true,
            "every_trade_quote_book_event_timestamp_is_nonzero_integral_milliseconds": true,
            "accepted_reconnect_request_and_native_reconnected_marker": true,
            "transport_connect_edge_after_request": true,
            "native_errors_foreign_instruments_invalid_books_unexpected_messages": 0,
            "complete_duration_and_successful_disconnect": true,
        },
        "limitations": {
            "websocket_wire_frames_exposed_by_selected_native_api": false,
            "adapter_manual_reconnect_loss_callback_guaranteed": false,
            "raw_wire_timestamp_equality_tested": false,
            "context_native_event_clock": "adapter_receipt_clock",
            "context_venue_timestamp_or_funding_boundary_qualified": false,
            "phases_follow_consumer_observation_of_request_and_native_reconnected_marker": true,
        },
        "package_version": env!("CARGO_PKG_VERSION"),
        "high_precision_feature": cfg!(feature = "high-precision"),
        "build_source_hashes": [
            source_hash("bin/ws_btc_reconnect_qualification.rs", include_bytes!("ws_btc_reconnect_qualification.rs")),
            source_hash("Cargo.toml", include_bytes!("../Cargo.toml")),
            source_hash("src/common/parse.rs", include_bytes!("../src/common/parse.rs")),
            source_hash("src/http/parse.rs", include_bytes!("../src/http/parse.rs")),
            source_hash("src/websocket/parse.rs", include_bytes!("../src/websocket/parse.rs")),
            source_hash("src/websocket/handler.rs", include_bytes!("../src/websocket/handler.rs")),
            source_hash("src/websocket/client.rs", include_bytes!("../src/websocket/client.rs")),
            source_hash("crates/network/src/http/client.rs", include_bytes!("../../../network/src/http/client.rs")),
            source_hash("crates/network/src/http/connector.rs", include_bytes!("../../../network/src/http/connector.rs")),
            source_hash("crates/network/src/websocket/client.rs", include_bytes!("../../../network/src/websocket/client.rs")),
        ],
    }))?;
    let (state_tx, mut states) = tokio::sync::mpsc::unbounded_channel();
    let sink = SocketStateSink::new(move |state| {
        let _ = state_tx.send(SocketObservation {
            state,
            callback_time: SystemTime::now(),
            callback_elapsed: started.elapsed(),
        });
    });
    let mut client = HyperliquidWebSocketClient::new(
        None,
        HyperliquidEnvironment::Mainnet,
        None,
        TransportBackend::default(),
        None,
    )
    .with_state_sink(sink);
    let mut qualification = Qualification::default();
    let outcome: anyhow::Result<()> = async {
        let instrument_id = tokio::time::timeout_at(
            started + Duration::from_secs(SETUP_SECONDS),
            setup(&mut client, &mut recorder),
        )
        .await
        .context("public metadata/connection/subscription setup timed out")??;
        capture(
            &mut client,
            &mut states,
            instrument_id,
            started,
            &mut qualification,
            &mut recorder,
        )
        .await
    }
    .await;
    let capture_error = outcome.err().map(|e| format!("{e:#}"));
    let disconnect = tokio::time::timeout(Duration::from_secs(5), client.disconnect()).await;
    let disconnect_error = match disconnect {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(format!("{e:#}")),
        Err(e) => Some(e.to_string()),
    };
    recorder.write(
        "disconnect_call_completed",
        qualification.phase,
        now_ns()?,
        json!({
            "error": disconnect_error, "venue_event_clock": null,
        }),
    )?;

    while let Ok(observation) = states.try_recv() {
        record_socket(observation, &mut qualification, &mut recorder, false)?;
    }

    let passed = capture_error.is_none() && disconnect_error.is_none() && qualification.passed();
    recorder.write("qualification_summary", qualification.phase, now_ns()?, json!({
        "passed": passed,
        "elapsed_ns": started.elapsed().as_nanos().to_string(),
        "capture_error": capture_error, "disconnect_error": disconnect_error,
        "before_reconnect": qualification.before,
        "reconnect_pending": qualification.pending,
        "after_reconnected": qualification.after,
        "reconnect_request_accepted": qualification.reconnect_accepted,
        "post_request_native_reconnected": qualification.post_request_reconnected,
        "native_reconnected_events": qualification.native_reconnected_events,
        "transport_connected_edges": qualification.transport_connected_edges,
        "transport_disconnected_edges": qualification.transport_disconnected_edges,
        "post_request_transport_connected_edges": qualification.post_request_connected_edges,
        "post_request_transport_disconnected_edges": qualification.post_request_disconnected_edges,
        "native_errors": qualification.native_errors,
        "unexpected_native_messages": qualification.unexpected_native_messages,
    }))?;
    anyhow::ensure!(
        passed,
        "BTC public reconnect qualification failed; inspect the JSONL summary"
    );
    Ok(())
}
