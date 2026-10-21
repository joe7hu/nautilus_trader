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

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::{Clock, TestClock},
    live::{LiveClock, set_exec_event_sender},
    messages::{
        ExecutionEvent,
        execution::{BatchCancelOrders, CancelAllOrders, CancelOrder, QueryAccount, SubmitOrder},
    },
    msgbus::{self, MessageBus, MessagingSwitchboard, TypedHandler},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::{
    client::core::ExecutionClientCore, engine::ExecutionEngine, funding::FundingCheckpoint,
};
use nautilus_live::runner::{AsyncRunner, AsyncTimeEventSender};
use nautilus_model::{
    accounts::Account,
    data::{FundingRateUpdate, MarkPriceUpdate, QuoteTick},
    enums::{AccountType, BookType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{AccountState, OrderEventAny},
    identifiers::{AccountId, ClientId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orders::{Order, OrderTestBuilder},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_sandbox::{
    SandboxExecutionClient, SandboxExecutionClientConfig, SandboxExecutionClientFactory,
};
use rstest::rstest;
use rust_decimal::Decimal;

fn make_client(clock: Rc<RefCell<dyn Clock>>, cache: Rc<RefCell<Cache>>) -> SandboxExecutionClient {
    let config = SandboxExecutionClientConfig::builder()
        .account_id(AccountId::from("BINANCE-001"))
        .venue(Venue::from("BINANCE"))
        .starting_balances(vec![Money::from("10000 USDT")])
        .base_currency(Currency::USDT())
        .account_type(AccountType::Margin)
        .oms_type(OmsType::Netting)
        .book_type(BookType::L1_MBP)
        .bar_execution(false)
        .trade_execution(false)
        .build();
    let core = ExecutionClientCore::new(
        TraderId::from("TESTER-001"),
        ClientId::from("SANDBOX"),
        config.venue,
        config.oms_type,
        config.account_id,
        config.account_type,
        config.base_currency,
        cache.clone(),
    );
    SandboxExecutionClient::new(core, config, clock, cache)
}

#[tokio::test(flavor = "current_thread")]
async fn live_funding_barrier_observes_queued_fill_and_restores_original_receipt() {
    *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
    let (time_tx, mut time_rx) = tokio::sync::mpsc::unbounded_channel();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(LiveClock::new(Some(Arc::new(
        AsyncTimeEventSender::new(time_tx),
    )))));
    let cache = Rc::new(RefCell::new(Cache::default()));
    let mut perpetual = crypto_perpetual_ethusdt();
    perpetual.taker_fee = Decimal::new(5, 4);
    perpetual.maker_fee = Decimal::new(2, 4);
    perpetual.margin_init = Decimal::new(1, 1);
    perpetual.margin_maint = Decimal::new(1, 1);
    let instrument = InstrumentAny::CryptoPerpetual(perpetual);
    cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    let mut portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
    let engine = Rc::new(RefCell::new(ExecutionEngine::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    ExecutionEngine::register_msgbus_handlers(&engine);
    let (exec_tx, mut exec_rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(exec_tx);
    let mut client = make_client(clock.clone(), cache.clone());
    client.enable_funding().unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();
    let now = clock.borrow().timestamp_ns();
    let boundary = UnixNanos::from(now.as_u64() + 500_000_000);
    let update = FundingRateUpdate::new(
        instrument.id(),
        Decimal::new(1, 4),
        Some(480),
        Some(boundary),
        now,
        now,
    );
    client.process_funding_rate(update).unwrap();
    let mark = MarkPriceUpdate::new(instrument.id(), Price::from("4000.00"), now, now);
    cache.borrow_mut().add_mark_price(mark).unwrap();
    let quote = QuoteTick::new(
        instrument.id(),
        Price::from("3999.99"),
        Price::from("4000.00"),
        Quantity::from("0.100"),
        Quantity::from("0.100"),
        now,
        now,
    );
    cache.borrow_mut().add_quote(quote).unwrap();
    client.process_quote_tick(&quote).unwrap();
    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .instrument_id(instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.010"))
        .price(Price::from("4000.00"))
        .time_in_force(TimeInForce::Gtc)
        .client_order_id("FUNDING-QUEUED-001".into())
        .ts_init(now)
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(client.client_id()), false)
        .unwrap();
    client
        .submit_order(SubmitOrder::from_order(
            &order,
            TraderId::from("TESTER-001"),
            Some(client.client_id()),
            None,
            UUID4::new(),
            now,
        ))
        .unwrap();
    assert!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
    let timer = tokio::time::timeout(Duration::from_secs(5), time_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(timer.event().ts_event >= boundary);
    assert!(AsyncRunner::handle_time_event(timer));
    assert!(client.funding_checkpoint().is_err());
    assert!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
    let mut saw_fill = false;
    let mut saw_barrier = false;

    while let Ok(event) = exec_rx.try_recv() {
        match &event {
            ExecutionEvent::Order(OrderEventAny::Filled(_)) => saw_fill = true,
            ExecutionEvent::FundingBoundary(_) => {
                assert!(saw_fill);
                saw_barrier = true;
            }
            _ => {}
        }
        AsyncRunner::handle_exec_event(event);
    }
    assert!(saw_fill && saw_barrier);
    let checkpoint = client
        .funding_checkpoint()
        .expect("funding must reach a coherent applied cut")
        .unwrap();
    let receipt = serde_json::to_value(&checkpoint).unwrap();
    assert_eq!(
        receipt["receipts"][0]["progress"], "Applied",
        "native receipt: {}",
        receipt["receipts"]
    );
    let account_id = AccountId::from("BINANCE-001");
    let account = cache.borrow().account_owned(&account_id).unwrap();
    assert_eq!(
        account.balance_total(Some(Currency::USDT())),
        Some(Money::from("9999.97600000 USDT")),
        "original receipt: {}",
        receipt["receipts"]
    );
    let position = cache
        .borrow()
        .positions_open(None, None, None, Some(&account_id), None)[0]
        .cloned();
    assert_eq!(position.quantity, Quantity::from("0.010"));
    assert_eq!(position.avg_px_open, 4000.0);
    assert_eq!(position.realized_pnl, Some(Money::from("-0.02400000 USDT")));
    assert_eq!(position.adjustments.len(), 1);
    assert_eq!(
        position.adjustments[0].pnl_change,
        Some(Money::from("-0.00400000 USDT"))
    );
    assert_eq!(
        portfolio
            .equity(&Venue::from("BINANCE"), Some(&account_id))
            .get(&Currency::USDT()),
        Some(&Money::from("9999.97600000 USDT"))
    );
    let original_json = serde_json::to_value(&checkpoint).unwrap();
    client.process_funding_rate(update).unwrap();
    assert!(exec_rx.try_recv().is_err());
    assert_eq!(
        serde_json::to_value(client.funding_checkpoint().unwrap().unwrap()).unwrap(),
        original_json
    );
    client.stop().unwrap();

    let restored_cache = Rc::new(RefCell::new(Cache::default()));
    restored_cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    restored_cache.borrow_mut().add_account(account).unwrap();
    restored_cache
        .borrow_mut()
        .add_position(&position, OmsType::Netting)
        .unwrap();
    let original_order = cache
        .borrow()
        .order_owned(&order.client_order_id())
        .unwrap();
    restored_cache
        .borrow_mut()
        .add_order(original_order, None, Some(ClientId::from("SANDBOX")), false)
        .unwrap();
    let restored_checkpoint: FundingCheckpoint =
        serde_json::from_value(original_json.clone()).unwrap();
    let mut restored = make_client(clock.clone(), restored_cache.clone());
    restored.restore_funding(restored_checkpoint).unwrap();
    restored.restore_quote_only().unwrap();
    assert_eq!(
        serde_json::to_value(restored.funding_checkpoint().unwrap().unwrap()).unwrap(),
        original_json
    );
    restored.connect().await.unwrap();
    restored.start().unwrap();
    restored.process_funding_rate(update).unwrap();
    assert!(exec_rx.try_recv().is_err());
    assert_eq!(
        serde_json::to_value(restored.funding_checkpoint().unwrap().unwrap()).unwrap(),
        original_json
    );
    let mut conflict = update;
    conflict.rate = Decimal::new(2, 4);
    assert!(restored.process_funding_rate(conflict).is_err());
    assert!(restored.funding_checkpoint().is_err());
    assert_eq!(
        restored_cache
            .borrow()
            .account_owned(&account_id)
            .unwrap()
            .balance_total(Some(Currency::USDT())),
        Some(Money::from("9999.97600000 USDT"))
    );
    restored.stop().unwrap();
}

struct FundingHarness {
    client: SandboxExecutionClient,
    clock: Rc<RefCell<TestClock>>,
    cache: Rc<RefCell<Cache>>,
    instrument: InstrumentAny,
    _engine: Rc<RefCell<ExecutionEngine>>,
    portfolio: Portfolio,
    rx: tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
}

impl FundingHarness {
    async fn new() -> Self {
        *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
        let clock = Rc::new(RefCell::new(TestClock::new()));
        clock.borrow_mut().set_time(1_000_000_000.into());
        let cache = Rc::new(RefCell::new(Cache::default()));
        let mut perpetual = crypto_perpetual_ethusdt();
        perpetual.taker_fee = Decimal::new(5, 4);
        perpetual.maker_fee = Decimal::new(2, 4);
        perpetual.margin_init = Decimal::new(1, 1);
        perpetual.margin_maint = Decimal::new(1, 1);
        let instrument = InstrumentAny::CryptoPerpetual(perpetual);
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        let portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
        let engine = Rc::new(RefCell::new(ExecutionEngine::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        ExecutionEngine::register_msgbus_handlers(&engine);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        nautilus_common::live::runner::replace_exec_event_sender(tx);
        let mut client = make_client(clock.clone(), cache.clone());
        client.enable_funding().unwrap();
        client.connect().await.unwrap();
        client.start().unwrap();
        client
            .process_funding_rate(FundingRateUpdate::new(
                instrument.id(),
                Decimal::new(1, 4),
                Some(480),
                Some(3_000_000_000.into()),
                1_000_000_000.into(),
                1_000_000_000.into(),
            ))
            .unwrap();
        Self {
            client,
            clock,
            cache,
            instrument,
            _engine: engine,
            portfolio,
            rx,
        }
    }

    fn rate(&self, rate: Decimal, boundary: u64) -> FundingRateUpdate {
        FundingRateUpdate::new(
            self.instrument.id(),
            rate,
            Some(480),
            Some(boundary.into()),
            1_000_000_000.into(),
            1_000_000_000.into(),
        )
    }

    fn drain(&mut self) {
        while let Ok(event) = self.rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
    }

    fn open(&mut self, side: OrderSide, quantity: &str) {
        let now = self.clock.borrow().timestamp_ns();
        let quote = QuoteTick::new(
            self.instrument.id(),
            Price::from("4000.00"),
            Price::from("4000.00"),
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            now,
            now,
        );
        self.cache.borrow_mut().add_quote(quote).unwrap();
        self.client.process_quote_tick(&quote).unwrap();
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from("TESTER-001"))
            .instrument_id(self.instrument.id())
            .side(side)
            .quantity(Quantity::from(quantity))
            .price(Price::from("4000.00"))
            .time_in_force(TimeInForce::Gtc)
            .client_order_id("FUNDING-SIGN-001".into())
            .ts_init(now)
            .build();
        self.cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(self.client.client_id()), false)
            .unwrap();
        self.client
            .submit_order(SubmitOrder::from_order(
                &order,
                TraderId::from("TESTER-001"),
                Some(self.client.client_id()),
                None,
                UUID4::new(),
                now,
            ))
            .unwrap();
        self.drain();
    }

    fn mark(&self) {
        self.cache
            .borrow_mut()
            .add_mark_price(MarkPriceUpdate::new(
                self.instrument.id(),
                Price::from("4000.00"),
                2_000_000_000.into(),
                2_000_000_000.into(),
            ))
            .unwrap();
    }

    fn checkpoint_json(&self) -> serde_json::Value {
        serde_json::to_value(self.client.funding_checkpoint().unwrap().unwrap()).unwrap()
    }
}

#[rstest]
#[case::long_positive(
    OrderSide::Buy,
    "0.010",
    1,
    "-0.00400000 USDT",
    "9999.97600000 USDT",
    "-0.02400000 USDT"
)]
#[case::short_positive(
    OrderSide::Sell,
    "0.010",
    1,
    "0.00400000 USDT",
    "9999.98400000 USDT",
    "-0.01600000 USDT"
)]
#[case::long_negative(OrderSide::Buy, "0.010", -1, "0.00400000 USDT", "9999.98400000 USDT", "-0.01600000 USDT")]
#[case::short_negative(OrderSide::Sell, "0.010", -1, "-0.00400000 USDT", "9999.97600000 USDT", "-0.02400000 USDT")]
#[case::partial_long(
    OrderSide::Buy,
    "0.004",
    1,
    "-0.00160000 USDT",
    "9999.99040000 USDT",
    "-0.00960000 USDT"
)]
#[tokio::test(flavor = "current_thread")]
async fn native_funding_sign_currency_and_margin_invariants(
    #[case] side: OrderSide,
    #[case] quantity: &str,
    #[case] sign: i64,
    #[case] funding: &str,
    #[case] total: &str,
    #[case] realized: &str,
) {
    let mut harness = FundingHarness::new().await;
    harness.open(side, quantity);
    let before = harness
        .cache
        .borrow()
        .account_owned(&AccountId::from("BINANCE-001"))
        .unwrap();
    let original = harness
        .cache
        .borrow()
        .positions_open(None, None, None, None, None)[0]
        .cloned();
    harness.mark();
    harness.clock.borrow_mut().set_time(3_000_000_000.into());
    let rate = harness.rate(Decimal::new(sign, 4), 3_000_000_000);
    let topic =
        nautilus_common::msgbus::switchboard::get_funding_rate_topic(harness.instrument.id());
    msgbus::publish_funding_rate(topic, &rate);
    assert!(harness.client.funding_checkpoint().is_err());
    harness.drain();
    let current = harness
        .cache
        .borrow()
        .positions_open(None, None, None, None, None)[0]
        .cloned();
    assert_eq!(current.quantity, original.quantity);
    assert_eq!(current.avg_px_open, original.avg_px_open);
    assert_eq!(
        current.adjustments[0].pnl_change,
        Some(Money::from(funding))
    );
    assert_eq!(current.realized_pnl, Some(Money::from(realized)));
    let account = harness
        .cache
        .borrow()
        .account_owned(&AccountId::from("BINANCE-001"))
        .unwrap();
    assert_eq!(
        account.balance_total(Some(Currency::USDT())),
        Some(Money::from(total))
    );
    assert_eq!(
        account.balance_locked(Some(Currency::USDT())),
        before.balance_locked(Some(Currency::USDT()))
    );
    assert_eq!(
        harness
            .portfolio
            .equity(&Venue::from("BINANCE"), Some(&account.id()))
            .get(&Currency::USDT()),
        Some(&Money::from(total))
    );
    let checkpoint = harness.checkpoint_json();
    harness.client.process_funding_rate(rate).unwrap();
    assert!(harness.rx.try_recv().is_err());
    assert_eq!(harness.checkpoint_json(), checkpoint);
    harness.client.reset();
    assert!(ExecutionClient::reset(&mut harness.client).is_err());
    harness.client.process_funding_rate(rate).unwrap();
    assert!(harness.rx.try_recv().is_err());
    harness.client.disconnect().await.unwrap();
    harness.client.connect().await.unwrap();
    assert_eq!(harness.checkpoint_json(), checkpoint);
    let mut repeated_observation = rate;
    repeated_observation.ts_event = 2_000_000_000.into();
    repeated_observation.ts_init = 2_000_000_000.into();
    harness
        .client
        .process_funding_rate(repeated_observation)
        .unwrap();
    assert!(harness.rx.try_recv().is_err());
    assert_eq!(harness.checkpoint_json(), checkpoint);
    harness.client.stop().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn pending_and_empty_funding_boundaries_survive_disarmed_native_restore() {
    let mut harness = FundingHarness::new().await;
    let update = harness.rate(Decimal::new(1, 4), 3_000_000_000);
    harness.client.process_funding_rate(update).unwrap();
    let pending = harness.checkpoint_json();
    assert_eq!(pending["pending"].as_array().unwrap().len(), 1);
    assert!(pending["receipts"].as_array().unwrap().is_empty());
    harness.client.stop().unwrap();
    let mut restored = make_client(harness.clock.clone(), harness.cache.clone());
    restored
        .restore_funding(serde_json::from_value(pending.clone()).unwrap())
        .unwrap();
    assert_eq!(
        serde_json::to_value(restored.funding_checkpoint().unwrap().unwrap()).unwrap(),
        pending
    );
    assert!(harness.rx.try_recv().is_err());
    harness.clock.borrow_mut().set_time(3_000_000_000.into());
    restored.start().unwrap();
    harness.drain();
    let empty = serde_json::to_value(restored.funding_checkpoint().unwrap().unwrap()).unwrap();
    assert_eq!(empty["receipts"][0]["progress"], "Empty");
    assert_eq!(empty["account"], pending["account"]);
    restored.stop().unwrap();
    let mut second = make_client(harness.clock.clone(), harness.cache.clone());
    second
        .restore_funding(serde_json::from_value(empty.clone()).unwrap())
        .unwrap();
    second.start().unwrap();
    second.process_funding_rate(update).unwrap();
    assert!(harness.rx.try_recv().is_err());
    assert_eq!(
        serde_json::to_value(second.funding_checkpoint().unwrap().unwrap()).unwrap(),
        empty
    );
    second.stop().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unacknowledged_native_account_publication_halts_without_repeating_funding() {
    let mut harness = FundingHarness::new().await;
    harness.open(OrderSide::Buy, "0.010");
    harness.mark();
    let before = harness
        .cache
        .borrow()
        .account_owned(&AccountId::from("BINANCE-001"))
        .unwrap();
    msgbus::register_account_state_endpoint(
        MessagingSwitchboard::portfolio_update_account(),
        TypedHandler::from(|_: &AccountState| {}),
    );
    harness.clock.borrow_mut().set_time(3_000_000_000.into());
    let rate = harness.rate(Decimal::new(1, 4), 3_000_000_000);
    harness.client.process_funding_rate(rate).unwrap();
    harness.drain();
    assert!(harness.client.funding_checkpoint().is_err());
    harness.client.reset();
    assert!(ExecutionClient::reset(&mut harness.client).is_err());
    assert!(harness.client.process_funding_rate(rate).is_err());
    harness
        .client
        .query_account(QueryAccount::new(
            TraderId::from("TESTER-001"),
            Some(harness.client.client_id()),
            AccountId::from("BINANCE-001"),
            UUID4::new(),
            3_000_000_000.into(),
            None,
            None,
        ))
        .unwrap();
    assert!(harness.rx.try_recv().is_err());
    let current = harness
        .cache
        .borrow()
        .positions_open(None, None, None, None, None)[0]
        .cloned();
    assert_eq!(current.adjustments.len(), 1);
    assert_eq!(current.realized_pnl, Some(Money::from("-0.02400000 USDT")));
    let account = harness
        .cache
        .borrow()
        .account_owned(&AccountId::from("BINANCE-001"))
        .unwrap();
    assert_eq!(
        serde_json::to_value(account).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    harness.client.stop().unwrap();
    assert!(harness.client.funding_checkpoint().is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn funding_checkpoint_rejects_missing_consumed_receipt_and_changed_original_state() {
    let mut harness = FundingHarness::new().await;
    harness.open(OrderSide::Buy, "0.010");
    harness.mark();
    harness.clock.borrow_mut().set_time(3_000_000_000.into());
    harness
        .client
        .process_funding_rate(harness.rate(Decimal::new(1, 4), 3_000_000_000))
        .unwrap();
    harness.drain();
    let original = harness.checkpoint_json();
    harness.client.stop().unwrap();

    for challenge in 0..4 {
        let mut altered = original.clone();
        match challenge {
            0 => altered["receipts"] = serde_json::json!([]),
            1 => altered["venue"] = serde_json::json!("FOREIGN"),
            2 => {
                altered["receipts"][0]["adjustments"][0]["reason"] =
                    serde_json::json!("changed_settlement_identity");
            }
            3 => altered["positions"][0]["avg_px_open"] = serde_json::json!(4100.0),
            _ => unreachable!(),
        }
        let restored = make_client(harness.clock.clone(), harness.cache.clone());
        assert!(
            restored
                .restore_funding(serde_json::from_value(altered).unwrap())
                .is_err()
        );
        assert!(harness.rx.try_recv().is_err());
        assert_eq!(
            serde_json::to_value(
                harness
                    .cache
                    .borrow()
                    .account_owned(&AccountId::from("BINANCE-001"))
                    .unwrap()
            )
            .unwrap(),
            original["account"]
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unpriced_restored_boundary_defers_with_zero_financial_changes() {
    let mut harness = FundingHarness::new().await;
    harness.open(OrderSide::Buy, "0.010");
    let pending = harness.checkpoint_json();
    harness.client.stop().unwrap();
    let mut restored = make_client(harness.clock.clone(), harness.cache.clone());
    restored
        .restore_funding(serde_json::from_value(pending.clone()).unwrap())
        .unwrap();
    restored.restore_quote_only().unwrap();
    harness.clock.borrow_mut().set_time(3_000_000_000.into());
    restored.start().unwrap();
    harness.drain();
    assert!(restored.funding_checkpoint().is_err());
    let position = harness
        .cache
        .borrow()
        .positions_open(None, None, None, None, None)[0]
        .cloned();
    assert!(position.adjustments.is_empty());
    assert_eq!(position.realized_pnl, Some(Money::from("-0.02000000 USDT")));
    assert_eq!(
        serde_json::to_value(
            harness
                .cache
                .borrow()
                .account_owned(&AccountId::from("BINANCE-001"))
                .unwrap()
        )
        .unwrap(),
        pending["account"]
    );
    restored.stop().unwrap();
    assert_eq!(
        serde_json::to_value(restored.funding_checkpoint().unwrap().unwrap()).unwrap(),
        pending
    );
}

#[tokio::test(flavor = "current_thread")]
async fn stop_fences_an_undispatched_funding_callback_and_preserves_pending_identity() {
    let mut harness = FundingHarness::new().await;
    harness.open(OrderSide::Buy, "0.010");
    harness.mark();
    let pending = harness.checkpoint_json();
    harness.clock.borrow_mut().set_time(3_000_000_000.into());
    harness
        .client
        .process_funding_rate(harness.rate(Decimal::new(1, 4), 3_000_000_000))
        .unwrap();
    harness.client.stop().unwrap();
    harness.drain();
    assert_eq!(harness.checkpoint_json(), pending);
    assert!(
        harness
            .cache
            .borrow()
            .positions_open(None, None, None, None, None)[0]
            .adjustments
            .is_empty()
    );
}

#[rstest]
#[case::due_single(0, 0)]
#[case::due_all(0, 1)]
#[case::due_batch(0, 2)]
#[case::inflight_single(1, 0)]
#[case::inflight_all(1, 1)]
#[case::inflight_batch(1, 2)]
#[case::uncertain_single(2, 0)]
#[case::uncertain_all(2, 1)]
#[case::uncertain_batch(2, 2)]
#[tokio::test(flavor = "current_thread")]
async fn funded_cancellation_cannot_cross_due_inflight_or_uncertain_boundary(
    #[case] state: u8,
    #[case] command: u8,
) {
    let mut harness = FundingHarness::new().await;
    harness.open(OrderSide::Buy, "0.010");
    let now = harness.clock.borrow().timestamp_ns();
    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .instrument_id(harness.instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.010"))
        .price(Price::from("3900.00"))
        .time_in_force(TimeInForce::Gtc)
        .client_order_id("FUNDING-CANCEL-GUARD".into())
        .ts_init(now)
        .build();
    harness
        .cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(harness.client.client_id()), false)
        .unwrap();
    harness
        .client
        .submit_order(SubmitOrder::from_order(
            &order,
            TraderId::from("TESTER-001"),
            Some(harness.client.client_id()),
            None,
            UUID4::new(),
            now,
        ))
        .unwrap();
    harness.drain();
    let before = harness
        .cache
        .borrow()
        .account_owned(&AccountId::from("BINANCE-001"))
        .unwrap();
    harness.mark();

    if state == 2 {
        msgbus::register_account_state_endpoint(
            MessagingSwitchboard::portfolio_update_account(),
            TypedHandler::from(|_: &AccountState| {}),
        );
    }
    harness.clock.borrow_mut().set_time(3_000_000_000.into());
    if state > 0 {
        harness
            .client
            .process_funding_rate(harness.rate(Decimal::new(1, 4), 3_000_000_000))
            .unwrap();
    }

    if state == 2 {
        harness.drain();
    }
    let cancel = CancelOrder::new(
        TraderId::from("TESTER-001"),
        Some(harness.client.client_id()),
        order.strategy_id(),
        harness.instrument.id(),
        order.client_order_id(),
        None,
        UUID4::new(),
        3_000_000_000.into(),
        None,
        None,
    );

    match command {
        0 => harness.client.cancel_order(cancel).unwrap(),
        1 => harness
            .client
            .cancel_all_orders(CancelAllOrders::new(
                TraderId::from("TESTER-001"),
                Some(harness.client.client_id()),
                order.strategy_id(),
                harness.instrument.id(),
                None,
                UUID4::new(),
                3_000_000_000.into(),
                None,
                None,
            ))
            .unwrap(),
        2 => harness
            .client
            .batch_cancel_orders(BatchCancelOrders::new(
                TraderId::from("TESTER-001"),
                Some(harness.client.client_id()),
                order.strategy_id(),
                harness.instrument.id(),
                vec![cancel],
                UUID4::new(),
                3_000_000_000.into(),
                None,
                None,
            ))
            .unwrap(),
        _ => unreachable!(),
    }

    while let Ok(event) = harness.rx.try_recv() {
        assert!(!matches!(
            &event,
            ExecutionEvent::Order(OrderEventAny::Canceled(_))
        ));
        AsyncRunner::handle_exec_event(event);
    }
    assert_eq!(
        harness
            .cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    let account = harness
        .cache
        .borrow()
        .account_owned(&AccountId::from("BINANCE-001"))
        .unwrap();
    assert_eq!(
        account.balance_locked(Some(Currency::USDT())),
        before.balance_locked(Some(Currency::USDT()))
    );

    if state != 1 {
        assert_eq!(
            serde_json::to_value(&account).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }
    harness.client.stop().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn funded_factory_capture_handle_survives_boxing_and_retains_original_state() {
    let harness = FundingHarness::new().await;
    let native_config = harness.client.config().clone();
    let checkpoint = harness.client.funding_checkpoint().unwrap().unwrap();
    let expected = serde_json::to_value(&checkpoint).unwrap();
    let (mut client, capture) = SandboxExecutionClientFactory::new()
        .create_funded_quote_only(
            TraderId::from("TESTER-001"),
            "PAPER",
            &native_config,
            harness.cache.clone(),
            Some(checkpoint),
        )
        .unwrap();
    assert_eq!(
        serde_json::to_value(capture.checkpoint().unwrap()).unwrap(),
        expected
    );
    client.connect().await.unwrap();
    assert_eq!(
        serde_json::to_value(capture.checkpoint().unwrap()).unwrap(),
        expected
    );
}

#[tokio::test(flavor = "current_thread")]
async fn funded_connect_cannot_republish_unapplied_initialization() {
    *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
    let cache = Rc::new(RefCell::new(Cache::default()));
    let clock = Rc::new(RefCell::new(TestClock::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let sink = events.clone();
    msgbus::register_account_state_endpoint(
        MessagingSwitchboard::portfolio_update_account(),
        TypedHandler::from(move |state: &AccountState| sink.borrow_mut().push(state.clone())),
    );
    let mut client = make_client(clock, cache.clone());
    client.enable_funding().unwrap();
    assert!(client.connect().await.is_err());
    assert!(client.connect().await.is_err());
    assert_eq!(events.borrow().len(), 1);
    assert!(
        cache
            .borrow()
            .account(&AccountId::from("BINANCE-001"))
            .is_none()
    );
    assert!(client.funding_checkpoint().is_err());
}
