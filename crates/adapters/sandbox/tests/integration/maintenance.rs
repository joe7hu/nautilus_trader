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

use std::{cell::RefCell, rc::Rc, sync::Arc};

use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::{Clock, TestClock},
    live::{LiveClock, set_exec_event_sender},
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, QueryAccount, SubmitOrder,
            SubmitOrderList,
        },
    },
    msgbus::{self, MessageBus},
};
use nautilus_core::{DurationNanos, UUID4, UnixNanos};
use nautilus_execution::funding::FundingProgress;
use nautilus_execution::models::latency::{LatencyModelAny, StaticLatencyModel};
use nautilus_execution::{client::core::ExecutionClientCore, engine::ExecutionEngine};
use nautilus_live::runner::{AsyncRunner, AsyncTimeEventSender};
use nautilus_model::{
    accounts::{Account, AccountAny},
    data::{
        BookOrder, FundingRateUpdate, MarkPriceUpdate, OrderBookDelta, OrderBookDeltas, QuoteTick,
    },
    enums::{AccountType, BookAction, BookType, OmsType, OrderSide, OrderType, TimeInForce},
    identifiers::{AccountId, ClientId, OrderListId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orders::{Order, OrderList, OrderTestBuilder},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_sandbox::{
    SandboxExecutionClient, SandboxExecutionClientConfig, SandboxMaintenanceCheckpoint,
};
use rstest::rstest;
use rust_decimal::Decimal;

fn make_window_client(
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    window: Option<(u64, u64)>,
) -> SandboxExecutionClient {
    make_latency_client(clock, cache, window, None)
}

fn make_latency_client(
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    window: Option<(u64, u64)>,
    latency: Option<LatencyModelAny>,
) -> SandboxExecutionClient {
    make_balance_client(clock, cache, window, latency, Money::from("10 USDT"))
}

fn make_balance_client(
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    window: Option<(u64, u64)>,
    latency: Option<LatencyModelAny>,
    balance: Money,
) -> SandboxExecutionClient {
    let mut config = SandboxExecutionClientConfig::builder()
        .account_id(AccountId::from("BINANCE-001"))
        .venue(Venue::from("BINANCE"))
        .starting_balances(vec![balance])
        .base_currency(Currency::USDT())
        .account_type(AccountType::Margin)
        .oms_type(OmsType::Netting)
        .book_type(BookType::L1_MBP)
        .bar_execution(false)
        .trade_execution(false)
        .build();

    if let Some((first, deadline)) = window {
        config.expected_funding_ns = Some(first.into());
        config.execution_deadline_ns = Some(deadline.into());
    }
    config.latency_model = latency;
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

#[derive(Clone, Copy, Debug)]
enum CloseCase {
    Full,
    Above,
    Partial,
    OpeningRemainder,
    NoBook,
    RemainderNoBook,
    MissingAccountApplication,
    MissingOpeningApplication,
    ForeignAccount,
    CoveredLong,
    ZeroMaintenance,
    Delayed,
    ThinLong,
    NoBounds,
    Funded,
    SubmitList,
    UserCancelAll,
    UserCancelBatch,
    StalePendingSource,
    ConflictingPendingSource,
    DispatchFailure,
    AttemptBudget,
    ResidualRestore,
    DeadlinePending,
    MissingCancellationApplication,
}

#[rstest]
#[case(CloseCase::Full)]
#[case(CloseCase::Above)]
#[case(CloseCase::Partial)]
#[case(CloseCase::OpeningRemainder)]
#[case(CloseCase::NoBook)]
#[case(CloseCase::RemainderNoBook)]
#[case(CloseCase::MissingAccountApplication)]
#[case(CloseCase::MissingOpeningApplication)]
#[case(CloseCase::ForeignAccount)]
#[case(CloseCase::CoveredLong)]
#[case(CloseCase::ZeroMaintenance)]
#[case(CloseCase::Delayed)]
#[case(CloseCase::ThinLong)]
#[case(CloseCase::NoBounds)]
#[case(CloseCase::Funded)]
#[case(CloseCase::SubmitList)]
#[case(CloseCase::UserCancelAll)]
#[case(CloseCase::UserCancelBatch)]
#[case(CloseCase::StalePendingSource)]
#[case(CloseCase::ConflictingPendingSource)]
#[case(CloseCase::DispatchFailure)]
#[case(CloseCase::AttemptBudget)]
#[case(CloseCase::ResidualRestore)]
#[case(CloseCase::DeadlinePending)]
#[case(CloseCase::MissingCancellationApplication)]
#[tokio::test(flavor = "current_thread")]
async fn live_maintenance_applies_queued_short_before_equality_and_original_close(
    #[case] close_case: CloseCase,
) {
    let long = matches!(close_case, CloseCase::CoveredLong);
    let buy = long || matches!(close_case, CloseCase::ThinLong);
    let delayed = matches!(close_case, CloseCase::Delayed);
    let funded = matches!(close_case, CloseCase::Funded);
    let remainder = matches!(
        close_case,
        CloseCase::OpeningRemainder
            | CloseCase::RemainderNoBook
            | CloseCase::UserCancelAll
            | CloseCase::UserCancelBatch
            | CloseCase::StalePendingSource
            | CloseCase::ConflictingPendingSource
            | CloseCase::MissingCancellationApplication
    );
    let partial = matches!(
        close_case,
        CloseCase::Partial | CloseCase::AttemptBudget | CloseCase::ResidualRestore
    );
    let (
        opening_size,
        opening_quantity,
        opening_balance,
        healthy_ask,
        threshold_price,
        initial_pnl,
        final_balance,
        final_realized,
    ) = match close_case {
        CloseCase::Full
        | CloseCase::Partial
        | CloseCase::NoBook
        | CloseCase::MissingAccountApplication
        | CloseCase::MissingOpeningApplication
        | CloseCase::ForeignAccount
        | CloseCase::CoveredLong
        | CloseCase::ZeroMaintenance
        | CloseCase::Delayed
        | CloseCase::NoBounds
        | CloseCase::Funded
        | CloseCase::SubmitList
        | CloseCase::DispatchFailure
        | CloseCase::AttemptBudget
        | CloseCase::ResidualRestore
        | CloseCase::DeadlinePending => (
            "0.100",
            "0.010",
            "9.98000000 USDT",
            "4597.99",
            "4598.00",
            "-5.97990000 USDT",
            "3.97701000 USDT",
            "-6.02299000 USDT",
        ),
        CloseCase::ThinLong => (
            "0.100",
            "0.010",
            "9.98000000 USDT",
            "3402.02",
            "3402.00",
            "-5.97990000 USDT",
            "3.98299000 USDT",
            "-6.01701000 USDT",
        ),
        CloseCase::Above => (
            "0.100",
            "0.010",
            "9.98000000 USDT",
            "4597.99",
            "4598.00",
            "-5.97990000 USDT",
            "3.97690995 USDT",
            "-6.02309005 USDT",
        ),
        CloseCase::OpeningRemainder
        | CloseCase::RemainderNoBook
        | CloseCase::UserCancelAll
        | CloseCase::UserCancelBatch
        | CloseCase::StalePendingSource
        | CloseCase::ConflictingPendingSource
        | CloseCase::MissingCancellationApplication => (
            "0.004",
            "0.004",
            "9.99200000 USDT",
            "6097.99",
            "6098.00",
            "-8.39196000 USDT",
            "1.58780400 USDT",
            "-8.41219600 USDT",
        ),
    };
    *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
    let (time_tx, mut time_rx) = tokio::sync::mpsc::unbounded_channel();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(LiveClock::new(Some(Arc::new(
        AsyncTimeEventSender::new(time_tx),
    )))));
    let mut cache = Rc::new(RefCell::new(Cache::default()));
    let mut perpetual = crypto_perpetual_ethusdt();
    perpetual.taker_fee = Decimal::new(5, 4);
    perpetual.maker_fee = Decimal::new(2, 4);
    perpetual.margin_init = Decimal::new(1, 1);
    perpetual.margin_maint = if matches!(close_case, CloseCase::ZeroMaintenance) {
        Decimal::ZERO
    } else {
        Decimal::new(1, 1)
    };

    if matches!(close_case, CloseCase::NoBounds) {
        perpetual.min_price = None;
        perpetual.max_price = None;
    }
    let instrument = InstrumentAny::CryptoPerpetual(perpetual);
    cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    let mut portfolio = Some(Portfolio::new(clock.clone(), cache.clone(), None));
    let mut engine = Rc::new(RefCell::new(ExecutionEngine::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    ExecutionEngine::register_msgbus_handlers(&engine);
    let (exec_tx, mut exec_rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(exec_tx);
    let now = clock.borrow().timestamp_ns();
    let boundary = UnixNanos::from(
        now.as_u64()
            + if funded {
                1_000_000_000
            } else {
                60_000_000_000
            },
    );
    let deadline = now.as_u64()
        + if matches!(close_case, CloseCase::DeadlinePending) {
            1_000_000_000
        } else {
            20_000_000_000
        };
    let window = Some((boundary.as_u64(), deadline));
    let latency = delayed.then(|| {
        LatencyModelAny::Static(StaticLatencyModel::new(
            DurationNanos::from_millis(2),
            DurationNanos::ZERO,
            DurationNanos::ZERO,
            DurationNanos::ZERO,
        ))
    });
    let mut client = make_balance_client(
        clock.clone(),
        cache.clone(),
        window,
        latency,
        Money::from(if long { "10000 USDT" } else { "10 USDT" }),
    );

    if funded {
        client.enable_funding().unwrap();
    }
    client.enable_maintenance(Decimal::ONE).unwrap();
    let update = FundingRateUpdate::new(
        instrument.id(),
        Decimal::new(1, 4),
        Some(480),
        Some(boundary),
        now,
        now,
    );
    cache.borrow_mut().add_funding_rate(update).unwrap();
    if funded {
        client.process_funding_rate(update).unwrap();
    }
    client.connect().await.unwrap();
    client.start().unwrap();
    let mark = MarkPriceUpdate::new(instrument.id(), Price::from("4000.00"), now, now);
    cache.borrow_mut().add_mark_price(mark).unwrap();
    let quote = QuoteTick::new(
        instrument.id(),
        Price::from(if buy { "3999.99" } else { "4000.00" }),
        Price::from(if buy { "4000.00" } else { "4000.01" }),
        Quantity::from(opening_size),
        Quantity::from("0.100"),
        now,
        now,
    );
    cache.borrow_mut().add_quote(quote).unwrap();
    client.process_quote_tick(&quote).unwrap();

    while let Ok(event) = exec_rx.try_recv() {
        AsyncRunner::handle_exec_event(event);
    }
    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .instrument_id(instrument.id())
        .side(if buy { OrderSide::Buy } else { OrderSide::Sell })
        .quantity(Quantity::from("0.010"))
        .price(Price::from("4000.00"))
        .time_in_force(TimeInForce::Gtc)
        .client_order_id("MAINTENANCE-QUEUED-001".into())
        .ts_init(now)
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(client.client_id()), false)
        .unwrap();

    if matches!(close_case, CloseCase::SubmitList) {
        client
            .submit_order_list(SubmitOrderList::new(
                order.trader_id(),
                Some(client.client_id()),
                order.strategy_id(),
                OrderList::new(
                    OrderListId::from("MAINTENANCE-OPEN-LIST"),
                    instrument.id(),
                    order.strategy_id(),
                    vec![order.client_order_id()],
                    now,
                ),
                vec![order.init_event().clone()],
                None,
                None,
                None,
                UUID4::new(),
                now,
                None,
            ))
            .unwrap();
    } else {
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
    }
    assert!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );

    if delayed {
        let timer = tokio::time::timeout(std::time::Duration::from_secs(2), time_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(AsyncRunner::handle_time_event(timer));
        assert!(
            cache
                .borrow()
                .positions_open(None, None, None, None, None)
                .is_empty()
        );
    }

    if matches!(close_case, CloseCase::MissingOpeningApplication) {
        drop(portfolio.take());
    }

    if matches!(
        close_case,
        CloseCase::StalePendingSource | CloseCase::ConflictingPendingSource
    ) {
        let invalid_time = if matches!(close_case, CloseCase::StalePendingSource) {
            UnixNanos::from(now.as_u64() - 6_000_000_000)
        } else {
            now
        };
        let invalid = QuoteTick::new(
            instrument.id(),
            Price::from("9999.99"),
            Price::from("10000.00"),
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            invalid_time,
            invalid_time,
        );
        assert!(client.process_quote_tick(&invalid).is_err());
    }

    if remainder {
        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        let original =
            serde_json::to_value(client.maintenance_handle().checkpoint().unwrap()).unwrap();
        client.process_quote_tick(&quote).unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        assert_eq!(
            serde_json::to_value(client.maintenance_handle().checkpoint().unwrap()).unwrap(),
            original
        );
        let delta_time = clock.borrow().timestamp_ns();
        let deltas = OrderBookDeltas::new(
            instrument.id(),
            vec![OrderBookDelta::new(
                instrument.id(),
                BookAction::Add,
                BookOrder::new(
                    OrderSide::Buy,
                    Price::from("10000.00"),
                    Quantity::from("0.100"),
                    1,
                ),
                0,
                1,
                delta_time,
                delta_time,
            )],
        );
        client.process_order_book_deltas(&deltas).unwrap();
        msgbus::publish_deltas("data.book.deltas.BINANCE.ETHUSDT-PERP".into(), &deltas);

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        assert_eq!(
            serde_json::to_value(client.maintenance_handle().checkpoint().unwrap()).unwrap(),
            original
        );
    }
    let healthy_time = clock.borrow().timestamp_ns();
    let healthy = QuoteTick::new(
        instrument.id(),
        Price::from(if remainder {
            "3999.99"
        } else if buy {
            "3402.01"
        } else {
            "4597.98"
        }),
        Price::from(healthy_ask),
        Quantity::from("0.100"),
        Quantity::from("0.100"),
        healthy_time,
        healthy_time,
    );
    cache.borrow_mut().add_quote(healthy).unwrap();
    client.process_quote_tick(&healthy).unwrap();

    while let Ok(event) = exec_rx.try_recv() {
        AsyncRunner::handle_exec_event(event);
    }
    client.process_quote_tick(&healthy).unwrap();

    while let Ok(event) = exec_rx.try_recv() {
        AsyncRunner::handle_exec_event(event);
    }

    if matches!(close_case, CloseCase::MissingOpeningApplication) {
        assert!(client.maintenance_handle().checkpoint().is_err());
        let account = cache
            .borrow()
            .account_owned(&AccountId::from("BINANCE-001"))
            .unwrap();
        assert_eq!(
            account.balance_total(Some(Currency::USDT())),
            Some(Money::from("10 USDT"))
        );
        assert_eq!(
            cache.borrow().positions_open(None, None, None, None, None)[0].quantity,
            Quantity::from("0.010")
        );
        assert_eq!(cache.borrow().orders(None, None, None, None, None).len(), 1);
        assert!(client.maintenance_threshold(instrument.id()).is_err());
        assert!(
            client
                .submit_order(SubmitOrder::from_order(
                    &order,
                    TraderId::from("TESTER-001"),
                    Some(client.client_id()),
                    None,
                    UUID4::new(),
                    clock.borrow().timestamp_ns()
                ))
                .is_err()
        );
        client.stop().unwrap();
        return;
    }

    if funded {
        let timer = tokio::time::timeout(std::time::Duration::from_secs(2), time_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(AsyncRunner::handle_time_event(timer));
        let due_time = clock.borrow().timestamp_ns();
        let due = QuoteTick::new(
            instrument.id(),
            Price::from("4598.38"),
            Price::from("4598.39"),
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            due_time,
            due_time,
        );
        cache.borrow_mut().add_quote(due).unwrap();
        client.process_quote_tick(&due).unwrap();
        let mut funding_seen = false;

        while let Ok(event) = exec_rx.try_recv() {
            if matches!(event, ExecutionEvent::FundingBoundary(_)) {
                funding_seen = true;
            }

            if matches!(event, ExecutionEvent::SimulationBoundary(_)) {
                assert!(funding_seen);
            }
            AsyncRunner::handle_exec_event(event);
        }
        assert!(funding_seen);
        let original = client.funding_checkpoint().unwrap().unwrap();
        assert_eq!(original.receipts().len(), 1);
        assert_eq!(original.receipts()[0].progress, FundingProgress::Applied);
        assert_eq!(
            original.receipts()[0].adjustments[0].pnl_change,
            Some(Money::from("0.00400000 USDT"))
        );
        let account_id = AccountId::from("BINANCE-001");
        assert_eq!(
            cache
                .borrow()
                .account_owned(&account_id)
                .unwrap()
                .balance_total(Some(Currency::USDT())),
            Some(Money::from("9.98400000 USDT"))
        );
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            1
        );
        let next_time = clock.borrow().timestamp_ns();
        let next = FundingRateUpdate::new(
            instrument.id(),
            Decimal::new(1, 4),
            Some(480),
            Some(UnixNanos::from(boundary.as_u64() + 28_800_000_000_000)),
            next_time,
            next_time,
        );
        cache.borrow_mut().add_funding_rate(next).unwrap();
        client.process_funding_rate(next).unwrap();
        let healthy_time = clock.borrow().timestamp_ns();
        let healthy = QuoteTick::new(
            instrument.id(),
            Price::from("4598.38"),
            Price::from("4598.39"),
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            healthy_time,
            healthy_time,
        );
        cache.borrow_mut().add_quote(healthy).unwrap();
        client.process_quote_tick(&healthy).unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        let retained = client.maintenance_handle().checkpoint().unwrap();
        assert!(retained.receipts().is_empty());
        assert_eq!(
            client
                .maintenance_threshold(instrument.id())
                .unwrap()
                .unwrap()
                .price,
            Some(Price::from("4598.40"))
        );
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            1
        );
        let breach_time = clock.borrow().timestamp_ns();
        let breach = QuoteTick::new(
            instrument.id(),
            Price::from("4598.39"),
            Price::from("4598.40"),
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            breach_time,
            breach_time,
        );
        cache.borrow_mut().add_quote(breach).unwrap();
        client.process_quote_tick(&breach).unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        let maintenance = client.maintenance_handle().checkpoint().unwrap();
        assert_eq!(maintenance.receipts().len(), 1);
        assert_eq!(
            maintenance.receipts()[0].decision.exit_equity,
            Money::from("4.00000000 USDT")
        );
        assert_eq!(
            maintenance.receipts()[0]
                .account
                .balance_total(Some(Currency::USDT())),
            Some(Money::from("3.97700800 USDT"))
        );
        assert_eq!(
            maintenance.receipts()[0].positions[0].realized_pnl,
            Some(Money::from("-6.02299200 USDT"))
        );
        assert!(maintenance.receipts()[0].positions[0].is_closed());
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            2
        );
        assert_eq!(
            client
                .funding_checkpoint()
                .unwrap()
                .unwrap()
                .receipts()
                .len(),
            1
        );
        client.acknowledge_maintenance(1).unwrap();
        client.process_quote_tick(&breach).unwrap();
        assert!(exec_rx.try_recv().is_err());
        client.stop().unwrap();
        return;
    }
    let account_id = AccountId::from("BINANCE-001");
    if long || matches!(close_case, CloseCase::ZeroMaintenance) {
        let expected = Money::from(if long {
            "9999.98000000 USDT"
        } else {
            "9.98000000 USDT"
        });
        assert_eq!(
            cache
                .borrow()
                .account_owned(&account_id)
                .unwrap()
                .balance_total(Some(Currency::USDT())),
            Some(expected)
        );
        let threshold = client.maintenance_threshold(instrument.id()).unwrap();
        if long {
            assert!(threshold.unwrap().price.is_none());
        } else {
            assert!(threshold.is_none());
        }
        let adverse_time = clock.borrow().timestamp_ns();
        let adverse = QuoteTick::new(
            instrument.id(),
            Price::from(if long { "1.00" } else { "4597.99" }),
            Price::from(if long { "1.01" } else { "4598.00" }),
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            adverse_time,
            adverse_time,
        );
        cache.borrow_mut().add_quote(adverse).unwrap();
        client.process_quote_tick(&adverse).unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        assert_eq!(
            cache
                .borrow()
                .account_owned(&account_id)
                .unwrap()
                .balance_total(Some(Currency::USDT())),
            Some(expected)
        );
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            1
        );
        assert!(
            client
                .maintenance_checkpoint()
                .unwrap()
                .receipts()
                .is_empty()
        );
        client.stop().unwrap();
        return;
    }

    if delayed {
        assert_eq!(
            cache
                .borrow()
                .account_owned(&account_id)
                .unwrap()
                .balance_total(Some(Currency::USDT())),
            Some(Money::from("9.98000000 USDT"))
        );
        let adverse_time = clock.borrow().timestamp_ns();
        let adverse = QuoteTick::new(
            instrument.id(),
            Price::from("4597.99"),
            Price::from("4598.00"),
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            adverse_time,
            adverse_time,
        );
        cache.borrow_mut().add_quote(adverse).unwrap();
        client.process_quote_tick(&adverse).unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        let original = client.maintenance_checkpoint().unwrap();
        assert_eq!(original.receipts().len(), 1);
        assert_eq!(
            original.receipts()[0]
                .account
                .balance_total(Some(Currency::USDT())),
            Some(Money::from("3.97701000 USDT"))
        );
        assert!(client.maintenance_handle().checkpoint().is_err());
        client.stop().unwrap();
        return;
    }
    let query_before = serde_json::to_value(client.maintenance_checkpoint()).unwrap();
    client
        .query_account(QueryAccount::new(
            TraderId::from("TESTER-001"),
            Some(client.client_id()),
            account_id,
            UUID4::new(),
            clock.borrow().timestamp_ns(),
            None,
            None,
        ))
        .unwrap();
    SandboxExecutionClient::reset(&client);
    assert_eq!(
        serde_json::to_value(client.maintenance_checkpoint()).unwrap(),
        query_before
    );
    let account = cache.borrow().account_owned(&account_id).unwrap();
    assert_eq!(
        account.balance_total(Some(Currency::USDT())),
        Some(Money::from(opening_balance))
    );
    let position = cache
        .borrow()
        .positions_open(None, None, None, Some(&account_id), None)[0]
        .cloned();
    assert_eq!(position.quantity, Quantity::from(opening_quantity));
    assert_eq!(
        position
            .try_unrealized_pnl(if buy {
                healthy.bid_price
            } else {
                healthy.ask_price
            })
            .unwrap(),
        Money::from(initial_pnl)
    );
    assert_eq!(
        cache
            .borrow()
            .orders(None, None, None, Some(&account_id), None)
            .len(),
        1
    );
    let before = serde_json::to_value(client.maintenance_checkpoint()).unwrap();
    let threshold = client
        .maintenance_threshold(instrument.id())
        .unwrap()
        .unwrap();
    assert_eq!(threshold.price, Some(Price::from(threshold_price)));
    assert_eq!(
        serde_json::to_value(client.maintenance_threshold(instrument.id()).unwrap()).unwrap(),
        serde_json::to_value(Some(threshold)).unwrap()
    );
    assert_eq!(
        serde_json::to_value(client.maintenance_checkpoint()).unwrap(),
        before
    );
    assert_eq!(
        portfolio
            .as_mut()
            .unwrap()
            .equity(&Venue::from("BINANCE"), Some(&account_id))
            .get(&Currency::USDT()),
        Some(&Money::from(opening_balance))
    );
    let mut foreign = None;

    if matches!(close_case, CloseCase::ForeignAccount) {
        let other_id = AccountId::from("BINANCE-OTHER");
        let other_client_id = ClientId::from("OTHER");
        let config = SandboxExecutionClientConfig::builder()
            .account_id(other_id)
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
            other_client_id,
            config.venue,
            config.oms_type,
            other_id,
            config.account_type,
            config.base_currency,
            cache.clone(),
        );
        let mut other = SandboxExecutionClient::new(core, config, clock.clone(), cache.clone());
        other.connect().await.unwrap();
        other.start().unwrap();
        let other_time = clock.borrow().timestamp_ns();
        let other_quote = QuoteTick::new(
            instrument.id(),
            Price::from("3999.99"),
            Price::from("4000.00"),
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            other_time,
            other_time,
        );
        cache.borrow_mut().add_quote(other_quote).unwrap();
        other.process_quote_tick(&other_quote).unwrap();
        let other_order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from("TESTER-001"))
            .strategy_id("OTHER-001".into())
            .instrument_id(instrument.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.010"))
            .price(Price::from("4000.00"))
            .time_in_force(TimeInForce::Gtc)
            .client_order_id("OTHER-OPEN-001".into())
            .ts_init(other_time)
            .build();
        cache
            .borrow_mut()
            .add_order(other_order.clone(), None, Some(other_client_id), false)
            .unwrap();
        other
            .submit_order(SubmitOrder::from_order(
                &other_order,
                TraderId::from("TESTER-001"),
                Some(other_client_id),
                None,
                UUID4::new(),
                other_time,
            ))
            .unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        let account =
            serde_json::to_value(cache.borrow().account_owned(&other_id).unwrap()).unwrap();
        let positions = serde_json::to_value(
            cache
                .borrow()
                .positions(None, None, None, Some(&other_id), None)
                .into_iter()
                .map(|p| p.cloned())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        foreign = Some((other, account, positions));
    }

    if matches!(close_case, CloseCase::UserCancelAll) {
        client
            .cancel_all_orders(CancelAllOrders::new(
                order.trader_id(),
                Some(client.client_id()),
                order.strategy_id(),
                instrument.id(),
                None,
                UUID4::new(),
                clock.borrow().timestamp_ns(),
                None,
                None,
            ))
            .unwrap();
    } else if matches!(close_case, CloseCase::UserCancelBatch) {
        let cancel = CancelOrder::new(
            order.trader_id(),
            Some(client.client_id()),
            order.strategy_id(),
            instrument.id(),
            order.client_order_id(),
            None,
            UUID4::new(),
            clock.borrow().timestamp_ns(),
            None,
            None,
        );
        client
            .batch_cancel_orders(BatchCancelOrders::new(
                order.trader_id(),
                Some(client.client_id()),
                order.strategy_id(),
                instrument.id(),
                vec![cancel],
                UUID4::new(),
                clock.borrow().timestamp_ns(),
                None,
                None,
            ))
            .unwrap();
    }
    let adverse_time = clock.borrow().timestamp_ns();
    let adverse = QuoteTick::new(
        instrument.id(),
        Price::from(if remainder {
            "6097.99"
        } else if buy {
            "3402.00"
        } else {
            "4597.99"
        }),
        Price::from(if matches!(close_case, CloseCase::Above) {
            "4598.01"
        } else {
            threshold_price
        }),
        Quantity::from("0.100"),
        Quantity::from(if partial {
            "0.004"
        } else if matches!(
            close_case,
            CloseCase::NoBook
                | CloseCase::RemainderNoBook
                | CloseCase::MissingCancellationApplication
        ) {
            "0.000"
        } else {
            "0.100"
        }),
        adverse_time,
        adverse_time,
    );
    cache.borrow_mut().add_quote(adverse).unwrap();

    if matches!(close_case, CloseCase::DispatchFailure) {
        exec_rx.close();
        assert!(client.process_quote_tick(&adverse).is_err());
        assert!(client.maintenance_handle().checkpoint().is_err());
        let failed = serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap();
        assert!(failed["owner"]["uncertainty"].is_string());
        assert_eq!(failed["owner"]["receipts"].as_array().unwrap().len(), 0);
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            1
        );
        assert_eq!(
            cache
                .borrow()
                .account_owned(&account_id)
                .unwrap()
                .balance_total(Some(Currency::USDT())),
            Some(Money::from("9.98000000 USDT"))
        );
        SandboxExecutionClient::reset(&client);
        client.stop().unwrap();
        client.connect().await.unwrap();
        client.start().unwrap();
        assert_eq!(
            serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap(),
            failed
        );
        client.stop().unwrap();
        return;
    }
    client.process_quote_tick(&adverse).unwrap();
    let mut dispatched = false;

    while let Ok(event) = exec_rx.try_recv() {
        let boundary = matches!(event, ExecutionEvent::SimulationBoundary(_));
        AsyncRunner::handle_exec_event(event);

        if boundary {
            dispatched = true;
            break;
        }
    }
    assert!(dispatched);

    if matches!(close_case, CloseCase::DeadlinePending) {
        let remaining_ns =
            deadline.saturating_sub(clock.borrow().timestamp_ns().as_u64()) + 1_000_000;
        tokio::time::sleep(std::time::Duration::from_nanos(remaining_ns)).await;
    }
    assert_eq!(
        cache
            .borrow()
            .positions_open(None, None, None, Some(&account_id), None)[0]
            .quantity,
        Quantity::from(opening_quantity)
    );
    client.process_quote_tick(&adverse).unwrap();
    let extra = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(order.trader_id())
        .strategy_id(order.strategy_id())
        .instrument_id(instrument.id())
        .side(order.order_side())
        .quantity(Quantity::from("0.010"))
        .price(Price::from("4000.00"))
        .client_order_id("FORBIDDEN-EXTRA-OPEN".into())
        .ts_init(adverse_time)
        .build();
    cache
        .borrow_mut()
        .add_order(extra.clone(), None, Some(client.client_id()), false)
        .unwrap();
    assert!(
        client
            .submit_order(SubmitOrder::from_order(
                &extra,
                order.trader_id(),
                Some(client.client_id()),
                None,
                UUID4::new(),
                adverse_time
            ))
            .is_err()
    );
    assert!(
        client
            .submit_order_list(SubmitOrderList::new(
                order.trader_id(),
                Some(client.client_id()),
                order.strategy_id(),
                OrderList::new(
                    OrderListId::from("FORBIDDEN-EXTRA-LIST"),
                    instrument.id(),
                    order.strategy_id(),
                    vec![extra.client_order_id()],
                    adverse_time
                ),
                vec![extra.init_event().clone()],
                None,
                None,
                None,
                UUID4::new(),
                adverse_time,
                None
            ))
            .is_err()
    );

    if matches!(close_case, CloseCase::MissingCancellationApplication) {
        drop(portfolio.take());

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        let account = cache.borrow().account_owned(&account_id).unwrap();
        let AccountAny::Margin(margin) = &account else {
            panic!("native margin account required")
        };
        assert!(margin.total_initial_margin(Currency::USDT()).raw() > 0);
        assert!(
            cache
                .borrow()
                .order_owned(&order.client_order_id())
                .unwrap()
                .is_closed()
        );
        let unresolved = serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap();
        assert!(
            unresolved["owner"]["uncertainty"]
                .as_str()
                .is_some_and(|reason| reason.contains("post-order"))
        );
        assert_eq!(unresolved["owner"]["receipts"].as_array().unwrap().len(), 0);
        assert!(client.maintenance_handle().checkpoint().is_err());
        assert!(client.acknowledge_maintenance(1).is_err());
        client.stop().unwrap();
        client.connect().await.unwrap();
        client.start().unwrap();
        assert_eq!(
            serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap(),
            unresolved
        );
        client.stop().unwrap();
        return;
    }

    if matches!(close_case, CloseCase::MissingAccountApplication) {
        drop(portfolio.take());

        while let Ok(event) = exec_rx.try_recv() {
            if matches!(event, ExecutionEvent::SimulationBoundary(_)) {
                let mut unrelated = cache
                    .borrow()
                    .account_owned(&account_id)
                    .unwrap()
                    .last_event()
                    .unwrap();
                unrelated.event_id = UUID4::new();
                unrelated.ts_init = clock.borrow().timestamp_ns();
                cache.borrow_mut().update_account_state(&unrelated).unwrap();
            }
            AsyncRunner::handle_exec_event(event);
        }
        let unresolved = serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap();
        assert!(
            unresolved["owner"]["uncertainty"]
                .as_str()
                .unwrap()
                .contains("actual post-position")
        );
        assert_eq!(unresolved["owner"]["receipts"].as_array().unwrap().len(), 0);
        assert!(client.maintenance_threshold(instrument.id()).is_err());
        let event_count = cache
            .borrow()
            .account_owned(&account_id)
            .unwrap()
            .events()
            .len();
        assert!(
            event_count
                > unresolved["owner"]["pending"][0]["account"]["Margin"]["base"]["events"]
                    .as_array()
                    .unwrap()
                    .len()
        );
        SandboxExecutionClient::reset(&client);
        client.stop().unwrap();
        client.connect().await.unwrap();
        client.start().unwrap();
        client.process_quote_tick(&adverse).unwrap();
        assert!(exec_rx.try_recv().is_err());
        assert_eq!(
            serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap(),
            unresolved
        );
        client.stop().unwrap();
        return;
    }

    while let Ok(event) = exec_rx.try_recv() {
        AsyncRunner::handle_exec_event(event);
    }

    if matches!(
        close_case,
        CloseCase::NoBook | CloseCase::RemainderNoBook | CloseCase::MissingCancellationApplication
    ) {
        let no_book = serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap();
        assert_eq!(no_book["owner"]["pending"], serde_json::Value::Null);
        assert_eq!(no_book["owner"]["uncertainty"], serde_json::Value::Null);
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            1
        );
        assert_eq!(
            cache
                .borrow()
                .account_owned(&account_id)
                .unwrap()
                .balance_total(Some(Currency::USDT())),
            Some(Money::from(opening_balance))
        );
        assert_eq!(
            cache
                .borrow()
                .positions_open(None, None, None, Some(&account_id), None)[0]
                .quantity,
            Quantity::from(opening_quantity)
        );

        if remainder {
            let AccountAny::Margin(margin) = cache.borrow().account_owned(&account_id).unwrap()
            else {
                panic!("native margin account required")
            };
            assert_eq!(
                margin.total_initial_margin(Currency::USDT()),
                Money::zero(Currency::USDT())
            );
            assert_eq!(
                margin.total_maintenance_margin(Currency::USDT()),
                Money::from("1.60000000 USDT")
            );
            assert_eq!(no_book["owner"]["receipts"].as_array().unwrap().len(), 1);
            assert!(
                cache
                    .borrow()
                    .order_owned(&order.client_order_id())
                    .unwrap()
                    .is_closed()
            );
            client.acknowledge_maintenance(1).unwrap();
        } else {
            assert!(
                no_book["owner"]["deferral"]
                    .as_str()
                    .unwrap()
                    .contains("exit book")
            );
        }
        let fresh_time = clock.borrow().timestamp_ns();
        let executable = QuoteTick::new(
            instrument.id(),
            adverse.bid_price,
            adverse.ask_price,
            Quantity::from("0.100"),
            Quantity::from("0.100"),
            fresh_time,
            fresh_time,
        );
        cache.borrow_mut().add_quote(executable).unwrap();
        client.process_quote_tick(&executable).unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
    }
    let checkpoint = serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap();
    assert_eq!(
        checkpoint["owner"]["receipts"].as_array().unwrap().len(),
        if matches!(close_case, CloseCase::RemainderNoBook) {
            2
        } else {
            1
        },
        "{checkpoint}"
    );
    assert_eq!(checkpoint["owner"]["pending"], serde_json::Value::Null);
    assert_eq!(checkpoint["owner"]["uncertainty"], serde_json::Value::Null);

    if partial {
        let account = cache.borrow().account_owned(&account_id).unwrap();
        assert_eq!(
            account.balance_total(Some(Currency::USDT())),
            Some(Money::from("7.57880400 USDT"))
        );
        let position = cache
            .borrow()
            .positions_open(None, None, None, Some(&account_id), None)[0]
            .cloned();
        assert_eq!(position.quantity, Quantity::from("0.006"));
        assert_eq!(position.realized_pnl, Some(Money::from("-2.42119600 USDT")));
        assert_eq!(
            position.try_unrealized_pnl(adverse.ask_price).unwrap(),
            Money::from("-3.58800000 USDT")
        );
        let AccountAny::Margin(margin) = account else {
            panic!("native margin account required")
        };
        assert_eq!(
            margin.total_maintenance_margin(Currency::USDT()),
            Money::from("2.40000000 USDT")
        );
        client.process_quote_tick(&adverse).unwrap();
        assert!(exec_rx.try_recv().is_err());
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            2
        );
        client.acknowledge_maintenance(1).unwrap();
        if matches!(close_case, CloseCase::ResidualRestore) {
            let original = client.maintenance_checkpoint().unwrap();
            let original_json = serde_json::to_value(&original).unwrap();
            let restored_cache = Rc::new(RefCell::new(Cache::default()));
            restored_cache
                .borrow_mut()
                .add_instrument(instrument.clone())
                .unwrap();
            restored_cache
                .borrow_mut()
                .add_account(cache.borrow().account_owned(&account_id).unwrap())
                .unwrap();

            for position in cache
                .borrow()
                .positions(None, None, None, Some(&account_id), None)
            {
                restored_cache
                    .borrow_mut()
                    .add_position(&position, OmsType::Netting)
                    .unwrap();
            }

            for order in cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
            {
                restored_cache
                    .borrow_mut()
                    .add_order(
                        (*order).clone(),
                        cache
                            .borrow()
                            .position_id(&order.client_order_id())
                            .copied(),
                        Some(client.client_id()),
                        false,
                    )
                    .unwrap();
            }
            restored_cache
                .borrow_mut()
                .add_funding_rate(update)
                .unwrap();
            client.stop().unwrap();
            drop(portfolio.take());
            cache = restored_cache;
            portfolio = Some(Portfolio::new(clock.clone(), cache.clone(), None));
            let restored_engine = Rc::new(RefCell::new(ExecutionEngine::new(
                clock.clone(),
                cache.clone(),
                None,
            )));
            engine = restored_engine;
            ExecutionEngine::register_msgbus_handlers(&engine);
            client = make_window_client(clock.clone(), cache.clone(), window);
            client.restore_maintenance(original, Decimal::ONE).unwrap();
            client.restore_quote_only().unwrap();
            client.connect().await.unwrap();
            client.start().unwrap();
            assert_eq!(
                serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap(),
                original_json
            );
            assert!(exec_rx.try_recv().is_err());
        }
        client.process_quote_tick(&adverse).unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            2
        );
        let fresh_time = clock.borrow().timestamp_ns();
        let second = QuoteTick::new(
            instrument.id(),
            Price::from("4899.99"),
            Price::from("4900.00"),
            Quantity::from("0.100"),
            Quantity::from(if matches!(close_case, CloseCase::AttemptBudget) {
                "0.004"
            } else {
                "0.100"
            }),
            fresh_time,
            fresh_time,
        );
        cache.borrow_mut().add_quote(second).unwrap();
        client.process_quote_tick(&second).unwrap();

        while let Ok(event) = exec_rx.try_recv() {
            AsyncRunner::handle_exec_event(event);
        }
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            3
        );

        if matches!(close_case, CloseCase::AttemptBudget) {
            assert_eq!(
                cache
                    .borrow()
                    .account_owned(&account_id)
                    .unwrap()
                    .balance_total(Some(Currency::USDT())),
                Some(Money::from("3.96900400 USDT"))
            );
            assert_eq!(
                cache
                    .borrow()
                    .positions_open(None, None, None, Some(&account_id), None)[0]
                    .quantity,
                Quantity::from("0.002")
            );
            client.acknowledge_maintenance(2).unwrap();
            let now = clock.borrow().timestamp_ns();
            let third = QuoteTick::new(
                instrument.id(),
                Price::from("5999.99"),
                Price::from("6000.00"),
                Quantity::from("0.100"),
                Quantity::from("0.100"),
                now,
                now,
            );
            cache.borrow_mut().add_quote(third).unwrap();
            client.process_quote_tick(&third).unwrap();

            while let Ok(event) = exec_rx.try_recv() {
                AsyncRunner::handle_exec_event(event);
            }
            let cut = serde_json::to_value(client.maintenance_checkpoint().unwrap()).unwrap();
            assert!(
                cut["owner"]["deferral"]
                    .as_str()
                    .unwrap()
                    .contains("attempt budget")
            );
            assert_eq!(cut["owner"]["receipts"].as_array().unwrap().len(), 2);
            assert_eq!(
                cache
                    .borrow()
                    .orders(None, None, None, Some(&account_id), None)
                    .len(),
                3
            );
            assert_eq!(
                cache
                    .borrow()
                    .positions_open(None, None, None, Some(&account_id), None)[0]
                    .quantity,
                Quantity::from("0.002")
            );
            assert_eq!(
                cache
                    .borrow()
                    .account_owned(&account_id)
                    .unwrap()
                    .balance_total(Some(Currency::USDT())),
                Some(Money::from("3.96900400 USDT"))
            );
            client.stop().unwrap();
            return;
        }
        let account = cache.borrow().account_owned(&account_id).unwrap();
        assert_eq!(
            account.balance_total(Some(Currency::USDT())),
            Some(Money::from("2.16410400 USDT"))
        );
        let position = cache
            .borrow()
            .positions(None, None, None, Some(&account_id), None)[0]
            .cloned();
        assert!(position.is_closed());
        assert_eq!(position.realized_pnl, Some(Money::from("-7.83589600 USDT")));
        client.acknowledge_maintenance(2).unwrap();
    } else {
        let account = cache.borrow().account_owned(&account_id).unwrap();
        assert_eq!(
            account.balance_total(Some(Currency::USDT())),
            Some(Money::from(final_balance))
        );
        let positions: Vec<_> = cache
            .borrow()
            .positions(None, None, None, Some(&account_id), None)
            .into_iter()
            .map(|p| p.cloned())
            .collect();
        assert_eq!(positions.len(), 1);
        let position = &positions[0];
        assert!(position.is_closed());
        assert_eq!(position.realized_pnl, Some(Money::from(final_realized)));
        let AccountAny::Margin(margin) = account else {
            panic!("native margin account required")
        };
        assert_eq!(
            margin.total_maintenance_margin(Currency::USDT()),
            Money::zero(Currency::USDT())
        );
        assert_eq!(
            cache
                .borrow()
                .orders(None, None, None, Some(&account_id), None)
                .len(),
            2
        );
        client.process_quote_tick(&adverse).unwrap();
        assert!(exec_rx.try_recv().is_err());
        client
            .acknowledge_maintenance(if matches!(close_case, CloseCase::RemainderNoBook) {
                2
            } else {
                1
            })
            .unwrap();
    }
    drop(portfolio);
    assert!(
        client
            .maintenance_threshold(instrument.id())
            .unwrap()
            .is_none()
    );

    if let Some((mut other, account, positions)) = foreign {
        let other_id = AccountId::from("BINANCE-OTHER");
        assert_eq!(
            serde_json::to_value(cache.borrow().account_owned(&other_id).unwrap()).unwrap(),
            account
        );
        assert_eq!(
            serde_json::to_value(
                cache
                    .borrow()
                    .positions(None, None, None, Some(&other_id), None)
                    .into_iter()
                    .map(|p| p.cloned())
                    .collect::<Vec<_>>()
            )
            .unwrap(),
            positions
        );
        other.stop().unwrap();
    }

    if delayed {
        client.stop().unwrap();
        return;
    }
    let original_checkpoint = client.maintenance_checkpoint().unwrap();
    let original_json = serde_json::to_value(&original_checkpoint).unwrap();
    let original_account = cache.borrow().account_owned(&account_id).unwrap();
    let original_positions: Vec<_> = cache
        .borrow()
        .positions(None, None, None, Some(&account_id), None)
        .into_iter()
        .map(|p| p.cloned())
        .collect();
    let original_orders: Vec<_> = cache
        .borrow()
        .orders(None, None, None, Some(&account_id), None)
        .into_iter()
        .map(|o| (*o).clone())
        .collect();
    client.stop().unwrap();
    let restored_cache = Rc::new(RefCell::new(Cache::default()));
    restored_cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    restored_cache
        .borrow_mut()
        .add_account(original_account)
        .unwrap();

    for position in &original_positions {
        restored_cache
            .borrow_mut()
            .add_position(position, OmsType::Netting)
            .unwrap();
    }

    for order in &original_orders {
        let position_id = cache
            .borrow()
            .position_id(&order.client_order_id())
            .copied();
        restored_cache
            .borrow_mut()
            .add_order(
                order.clone(),
                position_id,
                Some(ClientId::from("SANDBOX")),
                false,
            )
            .unwrap();
    }
    restored_cache
        .borrow_mut()
        .add_funding_rate(update)
        .unwrap();

    if partial {
        let unchanged =
            serde_json::to_value(restored_cache.borrow().account_owned(&account_id).unwrap())
                .unwrap();
        let mut mutations = vec![
            (
                "/profile/execution_deadline_ns".to_string(),
                serde_json::json!(deadline + 1),
            ),
            ("/engines/0/1/order_count".to_string(), serde_json::json!(0)),
            (
                "/engines/0/1/position_count".to_string(),
                serde_json::json!(1),
            ),
            (
                "/engines/0/1/execution_count".to_string(),
                serde_json::json!(4),
            ),
            (
                "/owner/account/Margin/default_leverage".to_string(),
                serde_json::json!("2"),
            ),
            (
                "/owner/uncertainty".to_string(),
                serde_json::json!("unknown original close"),
            ),
            (
                "/owner/opening/0".to_string(),
                serde_json::json!("ALTERED-OPENING"),
            ),
        ];
        mutations.push(("/owner/receipts".to_string(), serde_json::json!([])));
        mutations.push((
            "/owner/receipts/1/orders".to_string(),
            original_json["owner"]["receipts"][0]["orders"].clone(),
        ));
        let variant = original_json["owner"]["receipts"][0]["orders"][0]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap();
        mutations.push((
            format!("/owner/receipts/0/orders/0/{variant}/core/events/0/Initialized/tags"),
            serde_json::json!(["ALTERED-LIQUIDATION-LINKAGE"]),
        ));
        let original_close =
            original_json["owner"]["receipts"][0]["orders"][0][variant]["core"]["client_order_id"]
                .clone();
        mutations.push((
            "/owner/pending".to_string(),
            serde_json::json!([
                original_json["owner"]["receipts"][0]["decision"],
                [original_close]
            ]),
        ));

        for (pointer, replacement) in mutations {
            let mut altered = original_json.clone();
            *altered
                .pointer_mut(&pointer)
                .unwrap_or_else(|| panic!("original checkpoint lacks {pointer}")) = replacement;
            if pointer == "/owner/receipts" {
                altered["owner"]["acknowledged"] = serde_json::json!(0);
            }
            let mut denied = make_window_client(clock.clone(), restored_cache.clone(), window);

            if let Ok(checkpoint) = serde_json::from_value::<SandboxMaintenanceCheckpoint>(altered)
            {
                assert!(
                    denied
                        .restore_maintenance(checkpoint, Decimal::ONE)
                        .is_err(),
                    "altered original accepted: {pointer}"
                );
            }
            assert!(
                denied.restore_quote_only().is_err(),
                "failed original restore bypassed: {pointer}"
            );
            assert!(
                denied.connect().await.is_err(),
                "failed original restore funded/connected: {pointer}"
            );
            assert_eq!(
                serde_json::to_value(restored_cache.borrow().account_owned(&account_id).unwrap())
                    .unwrap(),
                unchanged
            );
            assert!(exec_rx.try_recv().is_err());
        }
    }
    let mut restored = make_window_client(clock.clone(), restored_cache.clone(), window);
    restored
        .restore_maintenance(original_checkpoint, Decimal::ONE)
        .unwrap();
    restored.restore_quote_only().unwrap();
    assert_eq!(
        serde_json::to_value(restored.maintenance_checkpoint().unwrap()).unwrap(),
        original_json
    );
    restored.connect().await.unwrap();
    restored.start().unwrap();
    restored.process_quote_tick(&adverse).unwrap();
    assert!(exec_rx.try_recv().is_err());
    assert_eq!(
        serde_json::to_value(restored.maintenance_checkpoint().unwrap()).unwrap(),
        original_json
    );
    restored.stop().unwrap();

    if matches!(close_case, CloseCase::Full) {
        let expired_clock = Rc::new(RefCell::new(TestClock::new()));
        expired_clock
            .borrow_mut()
            .set_time(UnixNanos::from(deadline + 1));
        let original = client.maintenance_checkpoint().unwrap();
        let mut expired = make_window_client(expired_clock, restored_cache.clone(), window);
        expired
            .restore_maintenance(original.clone(), Decimal::ONE)
            .unwrap();
        expired.restore_quote_only().unwrap();
        expired.connect().await.unwrap();
        expired.start().unwrap();
        let _ = expired.process_quote_tick(&adverse);
        assert!(exec_rx.try_recv().is_err());
        assert_eq!(
            serde_json::to_value(expired.maintenance_checkpoint().unwrap()).unwrap(),
            serde_json::to_value(original).unwrap()
        );
        expired.stop().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_native_short_close_realizes_positive_control_without_cash_sale_credit() {
    let close_case = CloseCase::Full;
    let long = false;
    let buy = false;
    let delayed = false;
    let opening_size = "0.100";
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
    perpetual.margin_maint = if matches!(close_case, CloseCase::ZeroMaintenance) {
        Decimal::ZERO
    } else {
        Decimal::new(1, 1)
    };

    if matches!(close_case, CloseCase::NoBounds) {
        perpetual.min_price = None;
        perpetual.max_price = None;
    }
    let instrument = InstrumentAny::CryptoPerpetual(perpetual);
    cache
        .borrow_mut()
        .add_instrument(instrument.clone())
        .unwrap();
    let mut portfolio = Some(Portfolio::new(clock.clone(), cache.clone(), None));
    let engine = Rc::new(RefCell::new(ExecutionEngine::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    ExecutionEngine::register_msgbus_handlers(&engine);
    let (exec_tx, mut exec_rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(exec_tx);
    let now = clock.borrow().timestamp_ns();
    let boundary = UnixNanos::from(now.as_u64() + 60_000_000_000);
    let deadline = now.as_u64() + 20_000_000_000;
    let window = Some((boundary.as_u64(), deadline));
    let latency = delayed.then(|| {
        LatencyModelAny::Static(StaticLatencyModel::new(
            DurationNanos::from_millis(2),
            DurationNanos::ZERO,
            DurationNanos::ZERO,
            DurationNanos::ZERO,
        ))
    });
    let mut client = make_balance_client(
        clock.clone(),
        cache.clone(),
        window,
        latency,
        Money::from(if long { "10000 USDT" } else { "10 USDT" }),
    );
    let update = FundingRateUpdate::new(
        instrument.id(),
        Decimal::new(1, 4),
        Some(480),
        Some(boundary),
        now,
        now,
    );
    cache.borrow_mut().add_funding_rate(update).unwrap();
    client.connect().await.unwrap();
    client.start().unwrap();
    let mark = MarkPriceUpdate::new(instrument.id(), Price::from("4000.00"), now, now);
    cache.borrow_mut().add_mark_price(mark).unwrap();
    let quote = QuoteTick::new(
        instrument.id(),
        Price::from(if buy { "3999.99" } else { "4000.00" }),
        Price::from(if buy { "4000.00" } else { "4000.01" }),
        Quantity::from(opening_size),
        Quantity::from("0.100"),
        now,
        now,
    );
    cache.borrow_mut().add_quote(quote).unwrap();
    client.process_quote_tick(&quote).unwrap();

    while let Ok(event) = exec_rx.try_recv() {
        AsyncRunner::handle_exec_event(event);
    }
    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .instrument_id(instrument.id())
        .side(if buy { OrderSide::Buy } else { OrderSide::Sell })
        .quantity(Quantity::from("0.010"))
        .price(Price::from("4000.00"))
        .time_in_force(TimeInForce::Gtc)
        .client_order_id("MAINTENANCE-QUEUED-001".into())
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

    if delayed {
        let timer = tokio::time::timeout(std::time::Duration::from_secs(2), time_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(AsyncRunner::handle_time_event(timer));
        assert!(
            cache
                .borrow()
                .positions_open(None, None, None, None, None)
                .is_empty()
        );
    }

    while let Ok(event) = exec_rx.try_recv() {
        AsyncRunner::handle_exec_event(event);
    }
    let now = clock.borrow().timestamp_ns();
    let quote = QuoteTick::new(
        instrument.id(),
        Price::from("3899.99"),
        Price::from("3900.00"),
        Quantity::from("0.100"),
        Quantity::from("0.100"),
        now,
        now,
    );
    cache.borrow_mut().add_quote(quote).unwrap();
    client.process_quote_tick(&quote).unwrap();
    let close = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TESTER-001"))
        .instrument_id(instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.010"))
        .price(Price::from("3900.00"))
        .time_in_force(TimeInForce::Gtc)
        .reduce_only(true)
        .client_order_id("POSITIVE-NATIVE-CLOSE".into())
        .ts_init(now)
        .build();
    cache
        .borrow_mut()
        .add_order(close.clone(), None, Some(client.client_id()), false)
        .unwrap();
    client
        .submit_order(SubmitOrder::from_order(
            &close,
            TraderId::from("TESTER-001"),
            Some(client.client_id()),
            None,
            UUID4::new(),
            now,
        ))
        .unwrap();

    while let Ok(event) = exec_rx.try_recv() {
        AsyncRunner::handle_exec_event(event);
    }
    let account_id = AccountId::from("BINANCE-001");
    let account = cache.borrow().account_owned(&account_id).unwrap();
    assert_eq!(
        account.balance_total(Some(Currency::USDT())),
        Some(Money::from("10.96050000 USDT"))
    );
    let position = cache
        .borrow()
        .positions(None, None, None, Some(&account_id), None)[0]
        .cloned();
    assert!(position.is_closed());
    assert_eq!(position.realized_pnl, Some(Money::from("0.96050000 USDT")));
    assert_eq!(
        portfolio
            .as_mut()
            .unwrap()
            .equity(&Venue::from("BINANCE"), Some(&account_id))
            .get(&Currency::USDT()),
        Some(&Money::from("10.96050000 USDT"))
    );
    client.stop().unwrap();
}
