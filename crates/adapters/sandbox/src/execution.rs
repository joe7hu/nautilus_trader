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

//! Sandbox execution client implementation.

use std::{
    cell::{Cell, RefCell},
    collections::BinaryHeap,
    fmt::Debug,
    rc::Rc,
};

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::Clock,
    factories::OrderEventFactory,
    live::try_get_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder,
            GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
            GeneratePositionStatusReports, ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
            SubmitOrderList, TradingCommand,
        },
    },
    msgbus::{
        self, MStr, MessagingSwitchboard, Pattern, TypedHandler,
        typed_handler::ShareableMessageHandler,
    },
    runner::TimeEventMessage,
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{DurationNanos, Params, UUID4, UnixNanos, WeakCell};
use nautilus_execution::{
    client::core::ExecutionClientCore,
    funding::{
        FundingAccountPublisher, FundingCheckpoint, FundingOwner, FundingPriceCut, FundingProgress,
        FundingWindow,
    },
    maintenance::{
        MaintenanceCheckpoint, MaintenanceOwner, MaintenanceReceipt, MaintenanceThreshold,
    },
    matching_engine::{
        OrderMatchingEngine, ids_generator::IdsCheckpoint, inflight::InflightOrders,
    },
    models::{fee::FeeModelHandle, fill::FillModelHandle, latency::LatencyModel},
};
use nautilus_model::{
    accounts::{Account, AccountAny},
    data::{
        Bar, FundingRateUpdate, InstrumentClose, InstrumentStatus, OrderBookDeltas, QuoteTick,
        TradeTick,
    },
    enums::{AccountType, AggressorSide, BookType, OmsType, PositionAdjustmentType},
    events::{
        OrderCancelRejected, OrderEventAny, OrderModifyRejected, OrderRejected, PositionEvent,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, Venue, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    position::Position,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Quantity},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::config::SandboxExecutionClientConfig;

// Bounds retained state for quote-only instruments that expire without event-driven cleanup
const EXPIRED_ENGINE_SWEEP_INTERVAL: DurationNanos = DurationNanos::from_mins(1);

/// A sandbox execution client for paper trading against live market data.
///
/// The `SandboxExecutionClient` simulates order execution using the `OrderMatchingEngine`
/// to match orders against market data. This enables strategy testing in real-time
/// without actual order execution on exchanges.
pub struct SandboxExecutionClient {
    core: RefCell<ExecutionClientCore>,
    factory: OrderEventFactory,
    config: SandboxExecutionClientConfig,
    inner: Rc<RefCell<SandboxInner>>,
    handlers: RefCell<Option<RegisteredHandlers>>,
    restored: Cell<bool>,
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
}

/// Read-only access to the funding cut of one registered native sandbox client.
///
/// This handle stays on the client's native owner thread and cannot publish or settle events.
#[derive(Clone)]
pub struct SandboxFundingHandle {
    funding: Rc<SandboxFunding>,
    account_id: AccountId,
}

impl Debug for SandboxFundingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(SandboxFundingHandle))
            .field("account_id", &self.account_id)
            .finish_non_exhaustive()
    }
}

impl SandboxFundingHandle {
    /// Captures the original funding lifecycle against its current native account and positions.
    ///
    /// # Errors
    ///
    /// Returns an error while a boundary, account application or owner borrow is unresolved.
    pub fn checkpoint(&self) -> anyhow::Result<FundingCheckpoint> {
        anyhow::ensure!(
            self.funding.enabled.get() && !self.funding.in_flight.get(),
            "native funding capture is unavailable"
        );
        self.funding
            .owner
            .try_borrow()
            .map_err(|_| anyhow::anyhow!("native funding owner is borrowed"))?
            .checkpoint(self.account_id)
    }
}

/// Full original maintenance lifecycle and native matching ID state for dormant restoration.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SandboxMaintenanceCheckpoint {
    version: u8,
    profile: SandboxExecutionClientConfig,
    instruments: Vec<InstrumentAny>,
    owner: MaintenanceCheckpoint,
    engines: Vec<(InstrumentId, IdsCheckpoint)>,
}

/// Owner-thread access to coherent original maintenance capture and its durable acknowledgement.
#[derive(Clone)]
pub struct SandboxMaintenanceHandle {
    inner: Rc<RefCell<SandboxInner>>,
}

impl Debug for SandboxMaintenanceHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(SandboxMaintenanceHandle))
            .finish_non_exhaustive()
    }
}

impl SandboxMaintenanceHandle {
    /// Captures the complete original native account/position/order-generator lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an error while a native turn, account application or owner borrow is unresolved.
    pub fn checkpoint(&self) -> anyhow::Result<SandboxMaintenanceCheckpoint> {
        let inner = self
            .inner
            .try_borrow()
            .map_err(|_| anyhow::anyhow!("native maintenance owner borrowed"))?;
        anyhow::ensure!(
            !inner.maintenance_in_flight,
            "native maintenance FIFO turn pending"
        );
        anyhow::ensure!(
            inner.config.latency_model.is_none()
                && inner.config.fill_model.is_none()
                && inner.config.fee_model.is_none(),
            "runtime-only native models cannot be captured for restoration"
        );
        let owner = inner
            .maintenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("native maintenance disabled"))?;
        anyhow::ensure!(
            owner.checkpoint().account().is_some() && !owner.pending() && owner.can_capture(),
            "native maintenance original application unresolved"
        );
        Ok(SandboxMaintenanceCheckpoint {
            version: 1,
            profile: inner.config.clone(),
            instruments: inner
                .cache
                .borrow()
                .instruments(&inner.config.venue, None)
                .into_iter()
                .cloned()
                .collect(),
            owner: owner.checkpoint(),
            engines: inner
                .matching_engines
                .iter()
                .map(|(id, engine)| (*id, engine.ids_checkpoint()))
                .collect(),
        })
    }

    /// Reads the same native exit-quote threshold without changing money, orders or counters.
    ///
    /// # Errors
    ///
    /// Returns an error for unresolved application or unsupported native valuation.
    pub fn threshold(
        &self,
        instrument: InstrumentId,
    ) -> anyhow::Result<Option<MaintenanceThreshold>> {
        let inner = self
            .inner
            .try_borrow()
            .map_err(|_| anyhow::anyhow!("native maintenance owner borrowed"))?;
        anyhow::ensure!(
            !inner.maintenance_in_flight,
            "native maintenance FIFO turn pending"
        );
        let instrument = inner.cache.borrow().try_instrument(&instrument)?.clone();
        inner
            .maintenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("native maintenance disabled"))?
            .threshold(&instrument)
    }

    /// Releases only the captured exact original receipt prefix after durable acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error for unresolved or mismatched native receipts.
    pub fn acknowledge(&self, prefix: usize) -> anyhow::Result<()> {
        let mut inner = self
            .inner
            .try_borrow_mut()
            .map_err(|_| anyhow::anyhow!("native maintenance owner borrowed"))?;
        inner
            .maintenance
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("native maintenance disabled"))?
            .acknowledge(prefix)?;
        let now = inner.clock.borrow().timestamp_ns();
        inner.minimum_quote_init = Some(now);
        Ok(())
    }
}

impl SandboxMaintenanceCheckpoint {
    /// Returns the original full native account metadata and event history.
    pub fn account(&self) -> Option<&AccountAny> {
        self.owner.account()
    }
    /// Returns the original native positions without reconstructing their economics.
    pub fn positions(&self) -> &[Position] {
        self.owner.positions()
    }
    /// Returns the complete original native maintenance receipt prefix.
    pub fn receipts(&self) -> &[MaintenanceReceipt] {
        self.owner.receipts()
    }
}

struct RegisteredHandlers {
    deltas_pattern: MStr<Pattern>,
    deltas_handler: TypedHandler<OrderBookDeltas>,
    quote_pattern: MStr<Pattern>,
    quote_handler: TypedHandler<QuoteTick>,
    trade_pattern: MStr<Pattern>,
    trade_handler: TypedHandler<TradeTick>,
    bar_pattern: MStr<Pattern>,
    bar_handler: TypedHandler<Bar>,
    status_pattern: MStr<Pattern>,
    status_handler: ShareableMessageHandler,
    close_pattern: MStr<Pattern>,
    close_handler: ShareableMessageHandler,
    funding_pattern: MStr<Pattern>,
    funding_handler: TypedHandler<FundingRateUpdate>,
    position_pattern: MStr<Pattern>,
    position_handler: TypedHandler<PositionEvent>,
}

impl Debug for SandboxExecutionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(SandboxExecutionClient))
            .field("venue", &self.config.venue)
            .field("account_id", &self.core.borrow().account_id)
            .field("connected", &self.core.borrow().is_connected())
            .field(
                "matching_engines",
                &self.inner.borrow().matching_engines.len(),
            )
            .finish()
    }
}

impl SandboxExecutionClient {
    /// Creates a new [`SandboxExecutionClient`] instance.
    #[must_use]
    pub fn new(
        core: ExecutionClientCore,
        config: SandboxExecutionClientConfig,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
    ) -> Self {
        let mut balances = AHashMap::new();
        for money in &config.starting_balances {
            balances.insert(money.currency.code.to_string(), *money);
        }

        let funding = Rc::new(SandboxFunding {
            owner: RefCell::new(FundingOwner::new(
                config.venue,
                clock.clone(),
                cache.clone(),
                config.frozen_account,
            )),
            publisher: SandboxFundingPublisher {
                factory: OrderEventFactory::new(
                    core.trader_id,
                    core.account_id,
                    core.account_type,
                    core.base_currency,
                ),
                venue: config.venue,
                clock: clock.clone(),
            },
            initialized: Cell::new(false),
            enabled: Cell::new(false),
            active: Cell::new(false),
            in_flight: Cell::new(false),
            generation: Cell::new(0),
        });
        let fill_model = config
            .fill_model
            .clone()
            .map(FillModelHandle::from)
            .unwrap_or_default();
        let inner = Rc::new_cyclic(|weak: &std::rc::Weak<RefCell<SandboxInner>>| {
            RefCell::new(SandboxInner {
                clock: clock.clone(),
                cache: cache.clone(),
                config: config.clone(),
                fill_model,
                funding: funding.clone(),
                maintenance: None,
                maintenance_in_flight: false,
                maintenance_active: false,
                maintenance_quote: None,
                maintenance_restore: MaintenanceRestore::Fresh,
                maintenance_source_fault: false,
                matching_engines: AHashMap::new(),
                next_engine_raw_id: 0,
                balances,
                event_handler: None,
                inbound_queue: BinaryHeap::new(),
                inflight_orders: InflightOrders::default(),
                inbound_seq: 0,
                minimum_quote_init: None,
                client_id: core.client_id,
                account_id: core.account_id,
                self_weak: WeakCell::from(weak.clone()),
            })
        });

        let factory = OrderEventFactory::new(
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );

        Self {
            core: RefCell::new(core),
            factory,
            config,
            inner,
            handlers: RefCell::new(None),
            restored: Cell::new(false),
            clock,
            cache,
        }
    }

    /// Restores the original quote-only sandbox account and order state.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is active, original state is unavailable,
    /// or the account, instrument or order profile is unsupported.
    ///
    /// # Panics
    ///
    /// Panics if the matching engine is absent after it has been initialized.
    pub fn restore_quote_only(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.restored.get()
                && !self.core.borrow().is_connected()
                && (self.inner.borrow().matching_engines.is_empty()
                    || self.inner.borrow().maintenance.is_some())
                && (self.inner.borrow().maintenance.is_none()
                    || self.inner.borrow().maintenance_restore == MaintenanceRestore::Restored)
                && matches!(
                    self.config.account_type,
                    AccountType::Cash | AccountType::Margin
                )
                && self.config.oms_type == OmsType::Netting
                && self.config.book_type == BookType::L1_MBP
                && !self.config.bar_execution
                && !self.config.trade_execution
                && !self.config.queue_position
                && !self.config.liquidity_consumption
                && !self.config.frozen_account
                && self.config.latency_model.is_none()
                && self.config.fill_model.is_none(),
            "unsupported sandbox restoration state or configuration"
        );
        let account_id = self.config.account_id;
        let (account, orders, instrument) = {
            let cache = self.cache.borrow();
            anyhow::ensure!(
                cache.instruments(&self.config.venue, None).len() == 1,
                "restoration requires one original instrument engine"
            );
            let account = cache
                .account_owned(&account_id)
                .ok_or_else(|| anyhow::anyhow!("original cash account unavailable"))?;
            anyhow::ensure!(
                account.account_type() == self.config.account_type
                    && account.base_currency() == self.config.base_currency,
                "original account type mismatch"
            );
            let orders = cache
                .orders(Some(&self.config.venue), None, None, None, None)
                .iter()
                .map(|o| (*o).clone())
                .collect::<Vec<_>>();
            anyhow::ensure!(
                orders.len()
                    <= if self.inner.borrow().maintenance.is_some() {
                        3
                    } else {
                        1
                    },
                "restoration exceeds the original bounded order history"
            );
            let instrument = orders
                .first()
                .map(|order| cache.try_instrument(&order.instrument_id()).cloned())
                .transpose()?;
            (account, orders, instrument)
        };

        if self.config.account_type == AccountType::Margin {
            let has_funding = self
                .cache
                .borrow()
                .positions(
                    Some(&self.config.venue),
                    None,
                    None,
                    Some(&account_id),
                    None,
                )
                .iter()
                .any(|position| {
                    position.adjustments.iter().any(|adjustment| {
                        adjustment.adjustment_type == PositionAdjustmentType::Funding
                    })
                });
            anyhow::ensure!(
                !has_funding || self.funding_checkpoint()?.is_some(),
                "original funding checkpoint required for funded positions"
            );
            anyhow::ensure!(
                self.config.default_leverage == rust_decimal::Decimal::ONE
                    && self.config.leverages.is_empty()
                    && account.balances().keys().all(|currency| Some(*currency) == self.config.base_currency)
                    && self.cache.borrow().instruments(&self.config.venue, None).iter().all(|i| matches!(i, InstrumentAny::CryptoPerpetual(p) if
                        (self.config.base_currency == Some(Currency::USDT()) && !p.is_inverse && p.settlement_currency == Currency::USDT())
                        || (self.config.base_currency == Some(Currency::BTC()) && p.is_inverse && p.base_currency == Currency::BTC() && p.quote_currency == Currency::USD() && p.settlement_currency == Currency::BTC() && p.multiplier == Quantity::from("100"))
                    )),
                "unsupported margin restoration profile"
            );
        }

        if let Some(instrument) = instrument
            && self.inner.borrow().maintenance.is_none()
        {
            let mut inner = self.inner.borrow_mut();
            inner.ensure_matching_engine(&instrument);
            inner
                .matching_engines
                .get_mut(&instrument.id())
                .unwrap()
                .restore_quote_only_limit_orders(&orders, account_id)?;
        }
        self.inner.borrow_mut().balances = account
            .balances()
            .into_values()
            .map(|balance| (balance.currency.code.to_string(), balance.total))
            .collect();
        self.sync_cached_account_config()?;
        self.restored.set(true);
        Ok(())
    }

    /// Enables native funding settlement before the client starts.
    ///
    /// # Errors
    ///
    /// Returns an error for an active, frozen, cash or before-funding-only profile.
    pub fn enable_funding(&self) -> anyhow::Result<()> {
        let funding = self.inner.borrow().funding.clone();
        if funding.enabled.get() {
            return Ok(());
        }
        anyhow::ensure!(
            !self.core.borrow().is_connected()
                && self
                    .cache
                    .borrow()
                    .account(&self.config.account_id)
                    .is_none(),
            "an existing native funded account requires its original checkpoint"
        );
        anyhow::ensure!(
            !self.core.borrow().is_started()
                && self.config.account_type == AccountType::Margin
                && !self.config.frozen_account
                && matches!(
                    (
                        self.config.execution_deadline_ns,
                        self.config.expected_funding_ns
                    ),
                    (None, None) | (Some(_), Some(_))
                ),
            "unsupported native funding profile"
        );
        let window = self
            .config
            .execution_deadline_ns
            .zip(self.config.expected_funding_ns)
            .map(|(deadline, first_boundary)| FundingWindow {
                first_boundary,
                deadline,
            });
        let now = self.clock.borrow().timestamp_ns();
        anyhow::ensure!(
            window.is_none_or(|window| now < window.first_boundary
                && window.deadline.as_u64().saturating_sub(now.as_u64()) <= 300_000_000_000),
            "native funding window is expired or exceeds five minutes"
        );
        funding.owner.borrow_mut().bind_window(window)?;
        funding.enabled.set(true);
        Ok(())
    }

    /// Queues an original native funding update on an explicitly enabled client.
    ///
    /// # Errors
    ///
    /// Returns an error for disabled funding or conflicting original input.
    pub fn process_funding_rate(&self, update: FundingRateUpdate) -> anyhow::Result<()> {
        let funding = self.inner.borrow().funding.clone();
        anyhow::ensure!(funding.enabled.get(), "native funding is disabled");
        funding.owner.borrow_mut().queue_live(update)?;
        SandboxFunding::schedule(&self.inner);
        Ok(())
    }

    /// Captures the complete original native funding state at a coherent financial cut.
    ///
    /// # Errors
    ///
    /// Returns an error while settlement or account application is unresolved.
    pub fn funding_checkpoint(&self) -> anyhow::Result<Option<FundingCheckpoint>> {
        self.funding_handle()
            .map(|handle| handle.checkpoint())
            .transpose()
    }

    /// Returns a read-only funding capture handle for an explicitly enabled client.
    #[must_use]
    pub fn funding_handle(&self) -> Option<SandboxFundingHandle> {
        let funding = self.inner.borrow().funding.clone();
        funding.enabled.get().then_some(SandboxFundingHandle {
            funding,
            account_id: self.config.account_id,
        })
    }

    /// Restores funding against original cached native account and positions without arming work.
    ///
    /// # Errors
    ///
    /// Returns an error for an active client or a mismatched native financial cut.
    pub fn restore_funding(&self, checkpoint: FundingCheckpoint) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.core.borrow().is_started()
                && !self.core.borrow().is_connected()
                && self.config.account_type == AccountType::Margin
                && !self.config.frozen_account
                && matches!(
                    (
                        self.config.execution_deadline_ns,
                        self.config.expected_funding_ns
                    ),
                    (None, None) | (Some(_), Some(_))
                ),
            "unsupported native funding restoration profile"
        );
        let funding = self.inner.borrow().funding.clone();
        let window = self
            .config
            .execution_deadline_ns
            .zip(self.config.expected_funding_ns)
            .map(|(deadline, first_boundary)| FundingWindow {
                first_boundary,
                deadline,
            });
        funding.owner.borrow_mut().bind_window(window)?;
        funding
            .owner
            .borrow_mut()
            .restore(checkpoint, self.config.account_id)?;
        funding.enabled.set(true);
        Ok(())
    }

    /// Returns a reference to the configuration.
    #[must_use]
    pub const fn config(&self) -> &SandboxExecutionClientConfig {
        &self.config
    }

    /// Returns the number of active matching engines.
    #[must_use]
    pub fn matching_engine_count(&self) -> usize {
        self.inner.borrow().matching_engines.len()
    }

    fn dispatch_order_event(&self, event: OrderEventAny) {
        self.inner.borrow().dispatch_order_event(event);
    }

    fn register_message_handlers(&self) {
        if self.handlers.borrow().is_some() {
            log::warn!("Sandbox message handlers already registered");
            return;
        }

        let inner_weak = WeakCell::from(Rc::downgrade(&self.inner));
        let venue = self.config.venue;
        let account_id = self.core.borrow().account_id;

        let deltas_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |deltas: &OrderBookDeltas| {
                if deltas.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_order_book_deltas(&inner_rc, deltas);
                }
            })
        };

        let quote_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |quote: &QuoteTick| {
                if quote.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_quote_tick(&inner_rc, quote);
                }
            })
        };

        let trade_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |trade: &TradeTick| {
                if trade.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_trade_tick(&inner_rc, trade);
                }
            })
        };

        // Bar topics include the bar type, so filter by venue in the handler
        let bar_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |bar: &Bar| {
                if bar.bar_type.instrument_id().venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_bar(&inner_rc, bar);
                }
            })
        };

        let status_handler = {
            let inner = inner_weak.clone();
            ShareableMessageHandler::from_typed(move |status: &InstrumentStatus| {
                if status.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_instrument_status(&inner_rc, status);
                }
            })
        };

        let close_handler = {
            let inner = inner_weak.clone();
            ShareableMessageHandler::from_typed(move |close: &InstrumentClose| {
                if close.instrument_id.venue == venue
                    && let Some(inner_rc) = inner.upgrade()
                {
                    SandboxInner::on_instrument_close(&inner_rc, close);
                }
            })
        };

        let funding_handler = {
            let inner = inner_weak.clone();
            TypedHandler::from(move |update: &FundingRateUpdate| {
                if update.instrument_id.venue == venue
                    && let Some(inner) = inner.upgrade()
                {
                    let funding = inner.borrow().funding.clone();
                    if funding.enabled.get() && funding.active.get() {
                        if let Err(e) = funding.owner.borrow_mut().queue_live(*update) {
                            log::error!("Native funding update rejected: {e}");
                        }
                        SandboxFunding::schedule(&inner);
                    }
                }
            })
        };
        let position_handler = {
            TypedHandler::from(move |event: &PositionEvent| {
                let PositionEvent::PositionClosed(position_closed) = event else {
                    return;
                };

                if position_closed.instrument_id.venue == venue
                    && position_closed.account_id == account_id
                    && let Some(inner_rc) = inner_weak.upgrade()
                {
                    // ExecutionEngine updates the cached position state before publishing
                    // PositionClosed, so this retry observes the post-settlement cache view.
                    if let Ok(mut inner) = inner_rc.try_borrow_mut() {
                        inner.sync_expired_cleanup(position_closed.instrument_id);
                    } else {
                        log::debug!(
                            "Skipping immediate expired cleanup retry for {} due to active sandbox borrow",
                            position_closed.instrument_id,
                        );
                    }
                }
            })
        };

        let deltas_pattern: MStr<Pattern> = format!("data.book.deltas.{venue}.*").into();
        let quote_pattern: MStr<Pattern> = format!("data.quotes.{venue}.*").into();
        let trade_pattern: MStr<Pattern> = format!("data.trades.{venue}.*").into();
        let bar_pattern: MStr<Pattern> = "data.bars.*".into();
        let status_pattern: MStr<Pattern> = format!("data.status.{venue}.*").into();
        let close_pattern: MStr<Pattern> = format!("data.close.{venue}.*").into();
        let position_pattern: MStr<Pattern> = "events.position.*".into();
        let funding_pattern: MStr<Pattern> = format!("data.funding_rates.{venue}.*").into();

        msgbus::subscribe_book_deltas(deltas_pattern, deltas_handler.clone(), Some(10));
        msgbus::subscribe_quotes(quote_pattern, quote_handler.clone(), Some(10));
        msgbus::subscribe_trades(trade_pattern, trade_handler.clone(), Some(10));
        msgbus::subscribe_bars(bar_pattern, bar_handler.clone(), Some(10));
        msgbus::subscribe_any(status_pattern, status_handler.clone(), Some(10));
        msgbus::subscribe_instrument_close(close_pattern, close_handler.clone(), Some(10));
        msgbus::subscribe_position_events(position_pattern, position_handler.clone(), Some(10));
        msgbus::subscribe_funding_rates(funding_pattern, funding_handler.clone(), Some(10));

        *self.handlers.borrow_mut() = Some(RegisteredHandlers {
            deltas_pattern,
            deltas_handler,
            quote_pattern,
            quote_handler,
            trade_pattern,
            trade_handler,
            bar_pattern,
            bar_handler,
            status_pattern,
            status_handler,
            close_pattern,
            close_handler,
            funding_pattern,
            funding_handler,
            position_pattern,
            position_handler,
        });

        log::debug!(
            "Sandbox registered message handlers for venue={}",
            self.config.venue
        );
    }

    fn deregister_message_handlers(&self) {
        if let Some(handlers) = self.handlers.borrow_mut().take() {
            msgbus::unsubscribe_book_deltas(handlers.deltas_pattern, &handlers.deltas_handler);
            msgbus::unsubscribe_quotes(handlers.quote_pattern, &handlers.quote_handler);
            msgbus::unsubscribe_trades(handlers.trade_pattern, &handlers.trade_handler);
            msgbus::unsubscribe_bars(handlers.bar_pattern, &handlers.bar_handler);
            msgbus::unsubscribe_any(handlers.status_pattern, &handlers.status_handler);
            msgbus::unsubscribe_instrument_close(handlers.close_pattern, &handlers.close_handler);
            msgbus::unsubscribe_funding_rates(handlers.funding_pattern, &handlers.funding_handler);
            msgbus::unsubscribe_position_events(
                handlers.position_pattern,
                &handlers.position_handler,
            );

            log::debug!(
                "Sandbox deregistered message handlers for venue={}",
                self.config.venue
            );
        }
    }

    fn expiry_sweep_timer_name(&self) -> String {
        format!("{}-sandbox-expiry-sweep", self.core.borrow().client_id)
    }

    fn register_expiry_sweep_timer(&self) {
        let inner_weak = WeakCell::from(Rc::downgrade(&self.inner));
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event: TimeEvent| {
            let Some(inner_rc) = inner_weak.upgrade() else {
                return;
            };

            // The timer fires on the runner task, but a nested msgbus dispatch may already hold the
            // borrow; skipping is safe because the next interval retries.
            if let Ok(mut inner) = inner_rc.try_borrow_mut() {
                inner.sweep_expired_engines();
            } else {
                log::debug!("Skipping sandbox expiry sweep due to active borrow");
            }
        });

        let name = self.expiry_sweep_timer_name();

        if let Err(e) = self.clock.borrow_mut().set_timer_ns(
            &name,
            EXPIRED_ENGINE_SWEEP_INTERVAL,
            None,
            None,
            Some(TimeEventCallback::from(callback)),
            None,
            None,
        ) {
            log::error!("Failed to register sandbox expiry sweep timer: {e}");
        }
    }

    fn cancel_expiry_sweep_timer(&self) {
        self.clock
            .borrow_mut()
            .cancel_timer(&self.expiry_sweep_timer_name());
    }

    fn cancel_inbound_alert(&self) {
        let client_id = self.core.borrow().client_id;
        self.clock
            .borrow_mut()
            .cancel_timer(&inbound_alert_name(client_id));
    }

    fn get_current_account_balances(&self) -> Vec<AccountBalance> {
        let account_id = self.core.borrow().account_id;
        let cache = self.cache.borrow();

        if let Some(account) = cache.account(&account_id) {
            return account.balances().into_values().collect();
        }

        self.get_account_balances()
    }

    fn sync_cached_account_config(&self) -> anyhow::Result<()> {
        let Some(mut account) = self.get_account() else {
            return Ok(());
        };

        account.set_calculate_account_state(!self.config.frozen_account);

        if let AccountAny::Margin(margin_account) = &mut account {
            margin_account.set_default_leverage(self.config.default_leverage);
            for (instrument_id, leverage) in &self.config.leverages {
                margin_account.set_leverage(*instrument_id, *leverage);
            }
        }

        self.cache.borrow_mut().update_account(&account)
    }

    /// Enables bounded native maintenance before connection without changing legacy profiles.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported or already connected simulation profiles.
    pub fn enable_maintenance(&self, ratio: Decimal) -> anyhow::Result<()> {
        let now = self.clock.borrow().timestamp_ns();
        let deadline = self
            .config
            .execution_deadline_ns
            .ok_or_else(|| anyhow::anyhow!("native maintenance deadline missing"))?;
        let first = self
            .config
            .expected_funding_ns
            .ok_or_else(|| anyhow::anyhow!("native maintenance funding boundary missing"))?;
        anyhow::ensure!(
            deadline > now
                && deadline.as_u64().saturating_sub(now.as_u64()) <= 300_000_000_000
                && first > now
                && (self.inner.borrow().funding.enabled.get() || deadline < first),
            "native maintenance window must remain within five minutes and its original funding authority"
        );
        anyhow::ensure!(
            !self.core.borrow().is_connected()
                && !self.restored.get()
                && self.config.account_type == AccountType::Margin
                && self.config.oms_type == OmsType::Netting
                && self.config.book_type == BookType::L1_MBP
                && !self.config.frozen_account
                && !self.config.bar_execution
                && !self.config.trade_execution
                && self.config.execution_deadline_ns.is_some()
                && self.config.expected_funding_ns.is_some(),
            "unsupported bounded maintenance profile"
        );
        let mut inner = self.inner.borrow_mut();
        anyhow::ensure!(
            inner.maintenance.is_none(),
            "native maintenance already enabled"
        );
        inner.maintenance = Some(MaintenanceOwner::new(
            inner.account_id,
            self.cache.clone(),
            ratio,
            2,
        )?);
        Ok(())
    }

    /// Returns the retained original bounded maintenance cut.
    pub fn maintenance_checkpoint(&self) -> Option<SandboxMaintenanceCheckpoint> {
        let inner = self.inner.borrow();
        inner
            .maintenance
            .as_ref()
            .map(|owner| SandboxMaintenanceCheckpoint {
                version: 1,
                profile: inner.config.clone(),
                instruments: inner
                    .cache
                    .borrow()
                    .instruments(&inner.config.venue, None)
                    .into_iter()
                    .cloned()
                    .collect(),
                owner: owner.checkpoint(),
                engines: inner
                    .matching_engines
                    .iter()
                    .map(|(id, engine)| (*id, engine.ids_checkpoint()))
                    .collect(),
            })
    }

    /// Restores original maintenance/account/counter state without connecting or matching.
    ///
    /// # Errors
    ///
    /// Returns an error for changed original state or an unresolved native close.
    pub fn restore_maintenance(
        &self,
        original: SandboxMaintenanceCheckpoint,
        ratio: Decimal,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.restored.get() && !self.core.borrow().is_connected(),
            "native maintenance restore requires a fresh disarmed client"
        );
        let mut inner = self.inner.borrow_mut();
        anyhow::ensure!(
            inner.maintenance_restore == MaintenanceRestore::Fresh && inner.maintenance.is_none(),
            "native maintenance restore requires an unused original destination"
        );
        inner.maintenance_restore = MaintenanceRestore::Pending;
        anyhow::ensure!(
            inner.config.account_type == AccountType::Margin
                && inner.config.oms_type == OmsType::Netting
                && inner.config.book_type == BookType::L1_MBP
                && !inner.config.frozen_account
                && !inner.config.bar_execution
                && !inner.config.trade_execution
                && inner.config.execution_deadline_ns.is_some()
                && inner.config.expected_funding_ns.is_some(),
            "unsupported original native maintenance profile"
        );
        inner.maintenance = Some(MaintenanceOwner::new(
            inner.account_id,
            self.cache.clone(),
            ratio,
            2,
        )?);
        let current_instruments: Vec<_> = inner
            .cache
            .borrow()
            .instruments(&inner.config.venue, None)
            .into_iter()
            .cloned()
            .collect();
        anyhow::ensure!(
            original.version == 1
                && serde_json::to_value(&original.profile)? == serde_json::to_value(&inner.config)?
                && serde_json::to_value(&original.instruments)?
                    == serde_json::to_value(current_instruments)?,
            "native maintenance original profile/instrument mismatch"
        );
        anyhow::ensure!(
            inner.maintenance.is_some()
                && inner.matching_engines.is_empty()
                && original.engines.len() <= 1,
            "unsupported native maintenance destination"
        );

        for (instrument_id, ids) in &original.engines {
            let (instrument, orders) = {
                let cache = self.cache.borrow();
                let instrument = cache.try_instrument(instrument_id)?.clone();
                let orders = cache
                    .orders(
                        None,
                        Some(instrument_id),
                        None,
                        Some(&inner.account_id),
                        None,
                    )
                    .into_iter()
                    .map(|order| (*order).clone())
                    .collect::<Vec<_>>();
                (instrument, orders)
            };
            inner.ensure_matching_engine(&instrument);
            let account_id = inner.account_id;
            inner
                .matching_engines
                .get_mut(instrument_id)
                .ok_or_else(|| anyhow::anyhow!("native matcher absent"))?
                .restore_quote_only_history(&orders, account_id, ids)?;
        }
        inner
            .maintenance
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("native maintenance owner absent"))?
            .restore(original.owner)?;
        inner.maintenance_restore = MaintenanceRestore::Restored;
        Ok(())
    }

    /// Captures the read-only native exit-quote threshold for the supported position.
    ///
    /// # Errors
    ///
    /// Returns an error for unresolved application, unsupported basis or absent instrument.
    pub fn maintenance_threshold(
        &self,
        instrument: InstrumentId,
    ) -> anyhow::Result<Option<MaintenanceThreshold>> {
        let inner = self.inner.borrow();
        anyhow::ensure!(
            !inner.maintenance_in_flight,
            "native maintenance turn pending"
        );
        let owner = inner
            .maintenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("native maintenance disabled"))?;
        let instrument = self.cache.borrow().try_instrument(&instrument)?.clone();
        owner.threshold(&instrument)
    }

    /// Confirms durable retention of the exact original native maintenance receipt prefix.
    ///
    /// # Errors
    ///
    /// Returns an error for a mismatched prefix or unresolved original application.
    pub fn acknowledge_maintenance(&self, prefix: usize) -> anyhow::Result<()> {
        self.maintenance_handle().acknowledge(prefix)
    }

    /// Returns a same-thread handle to the original maintenance owner.
    pub fn maintenance_handle(&self) -> SandboxMaintenanceHandle {
        SandboxMaintenanceHandle {
            inner: self.inner.clone(),
        }
    }

    /// Processes a quote tick through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_quote_tick(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        if self.inner.borrow().maintenance.is_some() {
            return SandboxInner::schedule_maintenance_quote(&self.inner, *quote);
        }
        SandboxFunding::schedule(&self.inner);
        if !self
            .inner
            .borrow()
            .execution_open(quote.instrument_id, Some(quote.ts_init.max(quote.ts_event)))
        {
            SandboxInner::drain_inbound(&self.inner);
            return Ok(());
        }

        if self
            .inner
            .borrow()
            .minimum_quote_init
            .is_some_and(|cut| quote.ts_init < cut)
        {
            return Ok(());
        }
        SandboxInner::drain_inbound(&self.inner);

        let instrument_id = quote.instrument_id;
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        if !check_quote_or_drop("quote tick", quote, &instrument) {
            return Ok(());
        }

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_quote_tick(quote);
        }
        Ok(())
    }

    /// Processes a trade tick through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_trade_tick(&self, trade: &TradeTick) -> anyhow::Result<()> {
        SandboxFunding::schedule(&self.inner);
        if self.inner.borrow().funding.enabled.get()
            && !self
                .inner
                .borrow()
                .execution_open(trade.instrument_id, Some(trade.ts_event.max(trade.ts_init)))
        {
            return Ok(());
        }
        SandboxInner::drain_inbound(&self.inner);

        if !self.config.trade_execution {
            return Ok(());
        }

        let instrument_id = trade.instrument_id;
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        if !check_trade_or_drop("trade tick", trade, &instrument) {
            return Ok(());
        }

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_trade_tick(trade);
        }
        Ok(())
    }

    /// Processes a bar through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        SandboxFunding::schedule(&self.inner);
        if self.inner.borrow().funding.enabled.get()
            && !self.inner.borrow().execution_open(
                bar.bar_type.instrument_id(),
                Some(bar.ts_event.max(bar.ts_init)),
            )
        {
            return Ok(());
        }
        SandboxInner::drain_inbound(&self.inner);

        if !self.config.bar_execution {
            return Ok(());
        }

        let instrument_id = bar.bar_type.instrument_id();
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        if !check_bar_or_drop("bar", bar, &instrument) {
            return Ok(());
        }

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_bar(bar);
        }
        Ok(())
    }

    /// Processes order book deltas through the matching engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not found in the cache.
    pub fn process_order_book_deltas(&self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        if self.inner.borrow().maintenance.is_some() {
            return Ok(());
        }

        SandboxFunding::schedule(&self.inner);
        if self.inner.borrow().funding.enabled.get()
            && !self.inner.borrow().execution_open(
                deltas.instrument_id,
                Some(deltas.ts_event.max(deltas.ts_init)),
            )
        {
            return Ok(());
        }
        SandboxInner::drain_inbound(&self.inner);

        let instrument_id = deltas.instrument_id;
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        let mut inner = self.inner.borrow_mut();
        inner.ensure_matching_engine(&instrument);
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id) {
            engine.process_order_book_deltas(deltas)?;
        }
        Ok(())
    }

    /// Resets the sandbox to its initial state.
    pub fn reset(&self) {
        if self.inner.borrow().funding.enabled.get() || self.inner.borrow().maintenance.is_some() {
            log::error!("Retained sandbox reset requires a new isolated account and owner");
            return;
        }
        self.restored.set(false);
        let mut inner = self.inner.borrow_mut();
        inner.minimum_quote_init = None;
        inner.funding.owner.borrow_mut().reset();
        inner.funding.in_flight.set(false);
        inner
            .funding
            .generation
            .set(inner.funding.generation.get().wrapping_add(1));
        for engine in inner.matching_engines.values_mut() {
            engine.reset();
        }

        inner.balances.clear();
        for money in &self.config.starting_balances {
            inner
                .balances
                .insert(money.currency.code.to_string(), *money);
        }

        inner.clear_inbound_queue();
        self.cancel_inbound_alert();
        self.clock
            .borrow_mut()
            .cancel_timer(&funding_timer_name(self.config.account_id));

        log::info!(
            "Sandbox execution client reset: venue={}",
            self.config.venue
        );
    }

    fn get_account_balances(&self) -> Vec<AccountBalance> {
        self.inner
            .borrow()
            .balances
            .values()
            .map(|money| AccountBalance::new(*money, Money::zero(money.currency), *money))
            .collect()
    }

    fn get_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<OrderAny> {
        Ok(self.cache.borrow().try_order_owned(client_order_id)?)
    }
}

#[async_trait(?Send)]
impl ExecutionClient for SandboxExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.borrow().is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.borrow().client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.borrow().account_id
    }

    fn venue(&self) -> Venue {
        self.core.borrow().venue
    }

    fn oms_type(&self) -> OmsType {
        self.config.oms_type
    }

    fn on_instrument(&mut self, instrument: InstrumentAny) {
        let instrument_id = instrument.id();
        let mut inner = self.inner.borrow_mut();
        if let Some(engine) = inner.matching_engines.get_mut(&instrument_id)
            && let Err(e) = engine.update_instrument(instrument)
        {
            log::error!("Failed to update instrument {instrument_id} in sandbox engine: {e}");
        }
    }

    fn get_account(&self) -> Option<AccountAny> {
        let account_id = self.core.borrow().account_id;
        self.cache.borrow().account_owned(&account_id)
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.inner.borrow().funding.enabled.get() && self.inner.borrow().maintenance.is_none(),
            "cannot reset a retained native funding owner"
        );
        Self::reset(self);
        Ok(())
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        let ts_init = self.clock.borrow().timestamp_ns();
        let state = self
            .factory
            .generate_account_state(balances, margins, reported, ts_event, ts_init, info);
        let endpoint = MessagingSwitchboard::portfolio_update_account();
        msgbus::send_account_state(endpoint, &state);
        self.sync_cached_account_config()?;
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.borrow().is_started() {
            return Ok(());
        }

        let funding = self.inner.borrow().funding.clone();
        let now = self.clock.borrow().timestamp_ns();
        if funding.enabled.get() {
            let owner = funding.owner.borrow();
            anyhow::ensure!(
                owner.window().is_none_or(|window| now < window.deadline
                    && (if self.restored.get() {
                        owner
                            .due(now)
                            .iter()
                            .all(|(boundary, _)| *boundary >= window.deadline)
                    } else {
                        now < window.first_boundary
                            && owner.next_boundary() == Some(window.first_boundary)
                    })),
                "expired or unresolved restored native funding window"
            );
        }
        anyhow::ensure!(
            !funding.enabled.get() || try_get_exec_event_sender().is_some(),
            "funding requires the native execution FIFO channel"
        );
        funding.active.set(true);
        self.inner.borrow_mut().maintenance_active = true;

        if let Some(sender) = try_get_exec_event_sender() {
            let handler: Rc<dyn Fn(OrderEventAny)> = Rc::new(move |event: OrderEventAny| {
                if let Err(e) = sender.send(ExecutionEvent::Order(event)) {
                    log::warn!("Failed to send order event: {e}");
                }
            });
            let mut inner = self.inner.borrow_mut();
            inner.event_handler = Some(handler.clone());
            for engine in inner.matching_engines.values_mut() {
                engine.set_event_handler(handler.clone());
            }
        }

        if self.restored.get() {
            self.inner.borrow_mut().minimum_quote_init = Some(self.clock.borrow().timestamp_ns());
        }
        self.register_message_handlers();
        self.register_expiry_sweep_timer();
        SandboxFunding::schedule(&self.inner);

        anyhow::ensure!(
            self.inner.borrow().maintenance.is_none() || try_get_exec_event_sender().is_some(),
            "maintenance requires the native execution FIFO channel"
        );
        self.core.borrow().set_started();
        let core = self.core.borrow();
        log::info!(
            "Sandbox execution client started: venue={}, account_id={}, oms_type={:?}, account_type={:?}",
            self.config.venue,
            core.account_id,
            self.config.oms_type,
            self.config.account_type,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.borrow().is_stopped() {
            return Ok(());
        }

        let funding = self.inner.borrow().funding.clone();
        {
            let mut inner = self.inner.borrow_mut();
            inner.maintenance_active = false;
            if inner.maintenance_in_flight
                && let Some(owner) = inner.maintenance.as_mut()
            {
                owner.fail("native maintenance stopped during an unresolved turn".into());
            }
        }
        funding.active.set(false);
        funding
            .generation
            .set(funding.generation.get().wrapping_add(1));
        funding.in_flight.set(false);
        funding.owner.borrow_mut().cancel_barrier();
        self.clock
            .borrow_mut()
            .cancel_timer(&funding_timer_name(self.config.account_id));
        self.deregister_message_handlers();
        self.cancel_expiry_sweep_timer();
        self.cancel_inbound_alert();

        // Rejections take the execution channel like every other event, so the engine stopping
        // this client processes them once its own borrow is released. Without a runner sender they
        // would dispatch synchronously into an engine `ExecutionEngine::stop` still holds mutably,
        // which is unsupported
        {
            let mut inner = self.inner.borrow_mut();
            let discarded = inner.take_inbound_queue();
            inner.reject_discarded(discarded);
        }

        self.core.borrow().set_stopped();
        self.core.borrow().set_disconnected();
        log::info!(
            "Sandbox execution client stopped: venue={}",
            self.config.venue
        );
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.borrow().is_connected() {
            return Ok(());
        }

        anyhow::ensure!(
            self.inner.borrow().maintenance_restore == MaintenanceRestore::Fresh
                || (self.inner.borrow().maintenance_restore == MaintenanceRestore::Restored
                    && self.restored.get()),
            "original native maintenance restoration incomplete"
        );
        let funding = self.inner.borrow().funding.clone();
        let publish_initial = !self.restored.get()
            && ((!funding.enabled.get() && self.inner.borrow().maintenance.is_none())
                || !funding.initialized.get());
        if publish_initial {
            if funding.enabled.get() || self.inner.borrow().maintenance.is_some() {
                anyhow::ensure!(
                    self.cache
                        .borrow()
                        .account(&self.config.account_id)
                        .is_none(),
                    "native funded account already exists"
                );
                funding.initialized.set(true);
            }
            let balances = self.get_account_balances();
            let ts_event = self.clock.borrow().timestamp_ns();
            self.generate_account_state(balances, vec![], false, ts_event, None)?;
        }
        anyhow::ensure!(
            !funding.enabled.get()
                || self
                    .cache
                    .borrow()
                    .account(&self.config.account_id)
                    .is_some(),
            "original native funding initialization unavailable"
        );

        self.core.borrow().set_connected();
        log::info!(
            "Sandbox execution client connected: venue={}",
            self.config.venue
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.core.borrow().is_disconnected() {
            return Ok(());
        }

        self.core.borrow().set_disconnected();
        log::info!(
            "Sandbox execution client disconnected: venue={}",
            self.config.venue
        );
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.inner.borrow().execution_open(cmd.instrument_id, None),
            "sandbox execution window expired"
        );
        let order = self.get_order(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        if let Some(owner) = self.inner.borrow_mut().maintenance.as_mut() {
            owner.claim_opening(&order)?;
        }
        let ts_init = self.clock.borrow().timestamp_ns();
        let event = self.factory.generate_order_submitted(&order, ts_init);
        self.dispatch_order_event(event);

        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            inner.apply_submit_order(&cmd)?;
            inner.maintenance_after_command()?;
        } else {
            inner.defer_or_apply(TradingCommand::SubmitOrder(cmd));
        }
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.inner.borrow().execution_open(cmd.instrument_id, None),
            "sandbox execution window expired"
        );
        let ts_init = self.clock.borrow().timestamp_ns();

        let orders: Vec<OrderAny> = self
            .cache
            .borrow()
            .orders_for_ids(&cmd.order_list.client_order_ids, &cmd);

        if let Some(owner) = self.inner.borrow_mut().maintenance.as_mut() {
            anyhow::ensure!(
                orders.len() == 1,
                "native maintenance list requires one original opening"
            );
            owner.claim_opening(&orders[0])?;
        }

        for order in &orders {
            if order.is_closed() {
                log::warn!("Cannot submit closed order {}", order.client_order_id());
                continue;
            }

            let event = self.factory.generate_order_submitted(order, ts_init);
            self.dispatch_order_event(event);
        }

        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() {
            let _ = inner.apply_submit_order_list(&cmd);
            inner.maintenance_after_command()?;
        } else {
            inner.defer_or_apply(TradingCommand::SubmitOrderList(cmd));
        }
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.inner.borrow().execution_open(cmd.instrument_id, None),
            "sandbox execution window expired"
        );
        let mut inner = self.inner.borrow_mut();
        anyhow::ensure!(
            inner
                .maintenance
                .as_ref()
                .is_none_or(|owner| owner.modification_allowed(cmd.client_order_id, cmd.quantity)),
            "native maintenance modification exceeds original exposure"
        );

        if inner.config.latency_model.is_none() {
            inner.apply_modify_order(&cmd);
            inner.maintenance_after_command()?;
        } else {
            inner.defer_or_apply(TradingCommand::ModifyOrder(cmd));
        }
        Ok(())
    }

    fn batch_modify_orders(&self, cmd: BatchModifyOrders) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.inner.borrow().execution_open(cmd.instrument_id, None),
            "sandbox execution window expired"
        );
        let mut inner = self.inner.borrow_mut();
        anyhow::ensure!(
            inner.maintenance.as_ref().is_none_or(|owner| cmd
                .modifies
                .iter()
                .all(|modify| owner.modification_allowed(modify.client_order_id, modify.quantity))),
            "native maintenance batch modification exceeds original exposure"
        );

        if inner.config.latency_model.is_none() {
            inner.apply_batch_modify_orders(&cmd);
            inner.maintenance_after_command()?;
        } else {
            inner.defer_or_apply(TradingCommand::ModifyOrders(cmd));
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() && !inner.funding.enabled.get() {
            inner.apply_cancel_order(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::CancelOrder(cmd));
        }
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() && !inner.funding.enabled.get() {
            inner.apply_cancel_all_orders(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::CancelAllOrders(cmd));
        }
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.config.latency_model.is_none() && !inner.funding.enabled.get() {
            inner.apply_batch_cancel_orders(&cmd);
        } else {
            inner.defer_or_apply(TradingCommand::CancelOrders(cmd));
        }
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        if self.inner.borrow().funding.enabled.get() || self.inner.borrow().maintenance.is_some() {
            let state = self
                .get_account()
                .and_then(|account| account.last_event())
                .ok_or_else(|| anyhow::anyhow!("native funded account unavailable"))?;
            msgbus::publish_account_state(
                format!("events.account.{}", state.account_id).into(),
                &state,
            );
            return Ok(());
        }
        let balances = self.get_current_account_balances();
        let ts_event = self.clock.borrow().timestamp_ns();
        self.generate_account_state(balances, vec![], false, ts_event, None)?;
        Ok(())
    }

    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        // Orders are tracked in the cache, no external query needed for sandbox
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        _cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        // Sandbox orders are tracked internally
        Ok(None)
    }

    async fn generate_order_status_reports(
        &self,
        _cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        // Sandbox orders are tracked internally
        Ok(Vec::new())
    }

    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        // Sandbox fills are tracked internally
        Ok(Vec::new())
    }

    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        // Sandbox positions are tracked internally
        Ok(Vec::new())
    }

    async fn generate_mass_status(
        &self,
        _lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let core = self.core.borrow();
        let ts_init = self.clock.borrow().timestamp_ns();
        Ok(Some(ExecutionMassStatus::new(
            core.client_id,
            core.account_id,
            core.venue,
            ts_init,
            None,
        )))
    }
}

struct SandboxFundingPublisher {
    factory: OrderEventFactory,
    venue: Venue,
    clock: Rc<RefCell<dyn Clock>>,
}

impl FundingAccountPublisher for SandboxFundingPublisher {
    fn account_id(&self) -> AccountId {
        self.factory.account_id()
    }
    fn venue(&self) -> Venue {
        self.venue
    }
    fn publish(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        let state = self.factory.generate_account_state(
            balances,
            margins,
            true,
            ts_event,
            self.clock.borrow().timestamp_ns(),
            None,
        );
        msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state);
        Ok(())
    }
}

struct SandboxFunding {
    owner: RefCell<FundingOwner>,
    publisher: SandboxFundingPublisher,
    initialized: Cell<bool>,
    enabled: Cell<bool>,
    active: Cell<bool>,
    in_flight: Cell<bool>,
    generation: Cell<u64>,
}

impl SandboxFunding {
    fn schedule(inner: &Rc<RefCell<SandboxInner>>) {
        let funding = inner.borrow().funding.clone();
        if !funding.enabled.get() || !funding.active.get() || funding.in_flight.get() {
            return;
        }
        let now = funding.publisher.clock.borrow().timestamp_ns();
        let due = funding.owner.borrow_mut().seal_due(now);
        if due.is_empty() {
            Self::arm_next(inner);
            return;
        }
        let cuts: Vec<_> = {
            let this = inner.borrow();
            let cache = this.cache.borrow();
            due.into_iter()
                .map(|(boundary, instrument)| {
                    let price = cache
                        .mark_price(&instrument)
                        .copied()
                        .map(FundingPriceCut::Mark)
                        .or_else(|| {
                            let engine = this.matching_engines.get(&instrument)?;
                            Some(FundingPriceCut::Midpoint {
                                bid: engine.best_bid_price()?,
                                ask: engine.best_ask_price()?,
                                observed_ns: now,
                            })
                        });
                    (boundary, instrument, price)
                })
                .collect()
        };
        funding.in_flight.set(true);
        let generation = funding.generation.get();
        let weak = Rc::downgrade(inner);
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_| {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let funding = inner.borrow().funding.clone();
            if !funding.active.get() || funding.generation.get() != generation {
                return;
            }

            for (boundary, instrument, price) in &cuts {
                let progress = funding.owner.borrow_mut().process_live(
                    *instrument,
                    *boundary,
                    price.clone(),
                    &funding.publisher,
                );

                if !matches!(progress, FundingProgress::Applied | FundingProgress::Empty) {
                    break;
                }
            }
            funding.in_flight.set(false);
            Self::arm_next(&inner);
        });
        let event = TimeEvent::new(
            funding_timer_name(funding.publisher.account_id()).into(),
            UUID4::new(),
            now,
            now,
        );
        let message = ExecutionEvent::FundingBoundary(TimeEventMessage::new(
            event,
            TimeEventCallback::from(callback),
        ));

        if let Some(sender) = try_get_exec_event_sender()
            && let Err(e) = sender.send(message)
        {
            log::error!("Native funding barrier unavailable: {e}");
        }
    }

    fn arm_next(inner: &Rc<RefCell<SandboxInner>>) {
        let funding = inner.borrow().funding.clone();
        if !funding.active.get() || funding.in_flight.get() {
            return;
        }
        let Some(next) = funding.owner.borrow().next_boundary() else {
            return;
        };

        if funding
            .owner
            .borrow()
            .window()
            .is_some_and(|window| next >= window.deadline)
        {
            return;
        }
        let now = funding.publisher.clock.borrow().timestamp_ns();
        if next <= now {
            return;
        }
        let clock = funding.publisher.clock.clone();
        let name = funding_timer_name(funding.publisher.account_id());
        if clock.borrow().next_time_ns(&name) == Some(next) {
            return;
        }
        clock.borrow_mut().cancel_timer(&name);
        let weak = Rc::downgrade(inner);
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_| {
            if let Some(inner) = weak.upgrade() {
                Self::schedule(&inner);
            }
        });

        if let Err(e) = clock.borrow_mut().set_time_alert_ns(
            &name,
            next,
            Some(TimeEventCallback::from(callback)),
            Some(true),
        ) {
            log::error!("Native funding timer unavailable: {e}");
        }
    }
}

fn funding_timer_name(account: AccountId) -> String {
    format!("{account}-sandbox-funding")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaintenanceRestore {
    Fresh,
    Pending,
    Restored,
}

// Wrapped in `Rc<RefCell<>>` so message handlers can hold weak references
struct SandboxInner {
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    config: SandboxExecutionClientConfig,
    fill_model: FillModelHandle,
    funding: Rc<SandboxFunding>,
    maintenance: Option<MaintenanceOwner>,
    maintenance_in_flight: bool,
    maintenance_active: bool,
    maintenance_quote: Option<QuoteTick>,
    maintenance_restore: MaintenanceRestore,
    maintenance_source_fault: bool,
    matching_engines: AHashMap<InstrumentId, OrderMatchingEngine>,
    next_engine_raw_id: u32,
    balances: AHashMap<String, Money>,
    /// Forwards order events to the runner's execution channel once `start` finds a sender; shared
    /// with every matching engine this client owns.
    event_handler: Option<Rc<dyn Fn(OrderEventAny)>>,
    /// Inbound commands deferred by latency, ordered as a min-heap by due time.
    inbound_queue: BinaryHeap<DelayedCommand>,
    inflight_orders: InflightOrders,
    /// Monotonic sequence providing FIFO tie-breaking for deferred commands sharing a due time, so
    /// no queued command can be overtaken by one enqueued after it.
    inbound_seq: u64,
    minimum_quote_init: Option<UnixNanos>,
    client_id: ClientId,
    account_id: AccountId,
    self_weak: WeakCell<Self>,
}

/// A [`TradingCommand`] deferred by inbound latency, ordered by `due_ns` then `seq` so the
/// `BinaryHeap` behaves as a min-heap for FIFO draining. Equality follows the same key.
#[derive(Debug)]
struct DelayedCommand {
    due_ns: UnixNanos,
    seq: u64,
    command: TradingCommand,
}

impl Ord for DelayedCommand {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .due_ns
            .cmp(&self.due_ns)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for DelayedCommand {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DelayedCommand {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for DelayedCommand {}

impl SandboxInner {
    fn validate_maintenance_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        let now = self.clock.borrow().timestamp_ns();
        anyhow::ensure!(
            quote.ts_event <= now
                && quote.ts_init <= now
                && now.as_u64().saturating_sub(quote.ts_event.as_u64()) <= 5_000_000_000
                && now.as_u64().saturating_sub(quote.ts_init.as_u64()) <= 5_000_000_000,
            "stale native maintenance quote"
        );
        let cache = self.cache.borrow();
        let instrument = cache.try_instrument(&quote.instrument_id)?;
        anyhow::ensure!(
            check_quote_or_drop("maintenance quote", quote, instrument)
                && quote.bid_price <= quote.ask_price,
            "invalid native maintenance quote"
        );
        let tick = instrument.price_increment();
        anyhow::ensure!(
            tick.raw() > 0
                && [quote.bid_price, quote.ask_price]
                    .iter()
                    .all(|price| !price.is_error()
                        && !price.is_undefined()
                        && price.raw() >= 0
                        && (!instrument.is_inverse() || price.raw() > 0)
                        && price.raw() % tick.raw() == 0
                        && instrument.min_price().is_none_or(|min| *price >= min)
                        && instrument.max_price().is_none_or(|max| *price <= max)),
            "native maintenance quote outside instrument domain"
        );
        Ok(())
    }

    fn schedule_maintenance_quote(
        inner: &Rc<RefCell<Self>>,
        quote: QuoteTick,
    ) -> anyhow::Result<()> {
        {
            let this = inner.borrow();
            if this
                .config
                .execution_deadline_ns
                .is_some_and(|deadline| this.clock.borrow().timestamp_ns() >= deadline)
            {
                return Ok(());
            }
        }
        SandboxFunding::schedule(inner);
        {
            let mut this = inner.borrow_mut();
            if let Err(e) = this.validate_maintenance_quote(&quote) {
                this.maintenance_source_fault = true;
                if let Some(owner) = this.maintenance.as_mut() {
                    owner.defer(e.to_string());
                }
                return Err(e);
            }
        }
        Self::drain_inbound(inner);
        {
            let mut this = inner.borrow_mut();
            if !this.maintenance_active
                || !this
                    .maintenance
                    .as_ref()
                    .is_some_and(MaintenanceOwner::can_observe)
                || !this.execution_authorized(
                    quote.instrument_id,
                    Some(quote.ts_init.max(quote.ts_event)),
                )
            {
                return Ok(());
            }

            if this
                .minimum_quote_init
                .is_some_and(|cut| quote.ts_init < cut)
            {
                return Ok(());
            }

            if let Some(previous) = this.maintenance.as_ref().and_then(MaintenanceOwner::quote) {
                if previous == quote {
                    return Ok(());
                }

                if quote.ts_init <= previous.ts_init || quote.ts_event < previous.ts_event {
                    this.maintenance_source_fault = true;
                    if let Some(owner) = this.maintenance.as_mut() {
                        owner.defer("reordered or conflicting original maintenance source".into());
                    }
                    anyhow::bail!("reordered or conflicting original maintenance source");
                }
            }
            let instrument = this
                .cache
                .borrow()
                .try_instrument(&quote.instrument_id)?
                .clone();
            this.ensure_matching_engine(&instrument);
            if this.maintenance_in_flight {
                if this.maintenance_quote.is_none_or(|old| {
                    quote.ts_init >= old.ts_init && quote.ts_event >= old.ts_event
                }) {
                    this.maintenance_quote = Some(quote);
                }
                return Ok(());
            }
            this.maintenance_source_fault = false;
            this.maintenance_in_flight = true;
        }
        Self::enqueue_maintenance_turn(inner, quote, 0)
    }

    fn enqueue_maintenance_turn(
        inner: &Rc<RefCell<Self>>,
        quote: QuoteTick,
        stage: u8,
    ) -> anyhow::Result<()> {
        let now = inner.borrow().clock.borrow().timestamp_ns();
        let result =
            Self::send_maintenance_turn(WeakCell::from(Rc::downgrade(inner)), quote, stage, now);
        if let Err(e) = &result
            && let Some(owner) = inner.borrow_mut().maintenance.as_mut()
        {
            owner.fail(e.to_string());
        }
        result
    }

    fn send_maintenance_turn(
        weak: WeakCell<Self>,
        quote: QuoteTick,
        stage: u8,
        now: UnixNanos,
    ) -> anyhow::Result<()> {
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_| {
            let Some(inner) = weak.upgrade() else { return };
            if let Err(e) = Self::run_maintenance_turn(&inner, quote, stage) {
                let mut this = inner.borrow_mut();
                if let Some(owner) = this.maintenance.as_mut() {
                    if owner.pending() {
                        owner.fail(e.to_string());
                    } else {
                        owner.defer(e.to_string());
                    }
                }
                this.maintenance_in_flight = false;
                log::error!("Native maintenance application unavailable: {e}");
            }
        });
        let event = TimeEvent::new("sandbox-maintenance".into(), UUID4::new(), now, now);
        try_get_exec_event_sender()
            .ok_or_else(|| anyhow::anyhow!("native maintenance execution FIFO unavailable"))?
            .send(ExecutionEvent::SimulationBoundary(TimeEventMessage::new(
                event,
                TimeEventCallback::from(callback),
            )))
            .map_err(|e| anyhow::anyhow!("native maintenance dispatch: {e}"))
    }

    fn maintenance_after_command(&mut self) -> anyhow::Result<()> {
        let Some(owner) = self.maintenance.as_ref() else {
            return Ok(());
        };
        let quote = owner
            .quote()
            .ok_or_else(|| anyhow::anyhow!("native command source cut unavailable"))?;
        self.maintenance_in_flight = true;
        let now = self.clock.borrow().timestamp_ns();
        let result = Self::send_maintenance_turn(self.self_weak.clone(), quote, 1, now);
        if let Err(e) = &result
            && let Some(owner) = self.maintenance.as_mut()
        {
            owner.fail(e.to_string());
        }
        result
    }

    fn run_maintenance_turn(
        inner: &Rc<RefCell<Self>>,
        quote: QuoteTick,
        stage: u8,
    ) -> anyhow::Result<()> {
        let mut this = inner.borrow_mut();
        anyhow::ensure!(
            stage == 2
                || (this.maintenance_active
                    && this.execution_authorized(
                        quote.instrument_id,
                        Some(quote.ts_event.max(quote.ts_init))
                    )),
            "native maintenance authority expired"
        );

        if stage != 2 {
            anyhow::ensure!(
                !this.maintenance_source_fault,
                "native maintenance source invalidated during FIFO application"
            );
            this.validate_maintenance_quote(&quote)?;
        }
        let mut owner = this
            .maintenance
            .take()
            .ok_or_else(|| anyhow::anyhow!("native maintenance owner missing"))?;
        let result = (|| {
            if stage == 2 {
                owner.complete()?;
                return Ok(None);
            }

            if stage == 0 {
                owner.observe_quote(quote)?;
                anyhow::ensure!(
                    this.matching_engines
                        .get_mut(&quote.instrument_id)
                        .is_some_and(|engine| engine.install_quote_tick(&quote)),
                    "native maintenance quote installation failed"
                );
            }
            let currency = this
                .cache
                .borrow()
                .try_instrument(&quote.instrument_id)?
                .settlement_currency();
            let now = this.clock.borrow().timestamp_ns();
            if owner.process(now, currency, true, this.matching_engines.values_mut())? {
                return Ok(Some(2));
            }

            if !owner.ready() {
                return Ok(None);
            }

            if stage == 0 {
                this.matching_engines
                    .get_mut(&quote.instrument_id)
                    .ok_or_else(|| anyhow::anyhow!("native maintenance matcher missing"))?
                    .iterate(quote.ts_init, AggressorSide::NoAggressor);
                return Ok(Some(1));
            }
            Ok(None)
        })();

        if let Err(e) = &result {
            if owner.pending() {
                owner.fail(e.to_string());
            } else {
                owner.defer(e.to_string());
            }
        }
        this.maintenance = Some(owner);
        match result {
            Ok(Some(next)) => {
                drop(this);
                Self::enqueue_maintenance_turn(inner, quote, next)
            }
            Ok(None) => {
                this.maintenance_in_flight = false;
                let pending_quote = this.maintenance_quote.take();
                let ready = this
                    .maintenance
                    .as_ref()
                    .is_some_and(MaintenanceOwner::can_observe);
                drop(this);
                Self::drain_inbound(inner);
                if ready && let Some(quote) = pending_quote {
                    Self::schedule_maintenance_quote(inner, quote)?;
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn execution_open(&self, instrument: InstrumentId, event_ns: Option<UnixNanos>) -> bool {
        if self.maintenance.is_some()
            && (!self.maintenance_active
                || self.maintenance_in_flight
                || !self.maintenance.as_ref().is_some_and(|owner| {
                    owner.opening_ready(instrument, self.clock.borrow().timestamp_ns())
                }))
        {
            return false;
        }
        self.execution_authorized(instrument, event_ns)
    }

    fn execution_authorized(&self, instrument: InstrumentId, event_ns: Option<UnixNanos>) -> bool {
        if self.funding.enabled.get() {
            return self.funding.active.get()
                && !self.funding.in_flight.get()
                && self.funding.owner.try_borrow().is_ok_and(|owner| {
                    owner.matching_ready(instrument, self.clock.borrow().timestamp_ns(), event_ns)
                });
        }

        match (
            self.config.execution_deadline_ns,
            self.config.expected_funding_ns,
        ) {
            (None, None) => true,
            (Some(deadline), Some(_)) => {
                self.clock.borrow().timestamp_ns() < deadline
                    && event_ns.is_none_or(|event| event < deadline)
                    && self
                        .cache
                        .borrow()
                        .funding_rate(&instrument)
                        .is_some_and(|funding| {
                            funding.next_funding_ns == self.config.expected_funding_ns
                                && funding.next_funding_ns.is_some_and(|next| next > deadline)
                                && funding.ts_init <= self.clock.borrow().timestamp_ns()
                                && funding.ts_event <= self.clock.borrow().timestamp_ns()
                                && self
                                    .clock
                                    .borrow()
                                    .timestamp_ns()
                                    .as_u64()
                                    .saturating_sub(funding.ts_init.as_u64())
                                    <= 5_000_000_000
                                && self
                                    .clock
                                    .borrow()
                                    .timestamp_ns()
                                    .as_u64()
                                    .saturating_sub(funding.ts_event.as_u64())
                                    <= 5_000_000_000
                        })
            }
            _ => false,
        }
    }

    fn ensure_matching_engine(&mut self, instrument: &InstrumentAny) {
        let instrument_id = instrument.id();

        if !self.matching_engines.contains_key(&instrument_id) {
            let engine_config = self.config.to_matching_engine_config();
            let fill_model = self.fill_model.clone();
            let fee_model = self
                .config
                .fee_model
                .clone()
                .map(FeeModelHandle::from)
                .unwrap_or_default();
            let raw_id = self.next_engine_raw_id;
            self.next_engine_raw_id = self.next_engine_raw_id.wrapping_add(1);

            let mut engine = OrderMatchingEngine::new(
                instrument.clone(),
                raw_id,
                fill_model,
                fee_model,
                self.config.book_type,
                self.config.oms_type,
                self.config.account_type,
                self.clock.clone(),
                self.cache.clone(),
                engine_config,
            );

            if let Some(handler) = &self.event_handler {
                engine.set_event_handler(handler.clone());
            }

            engine.set_inflight_orders(self.inflight_orders.clone());
            self.matching_engines.insert(instrument_id, engine);
        }
    }

    fn process_quote_tick(&mut self, quote: &QuoteTick) {
        let instrument_id = quote.instrument_id;

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            if !check_quote_or_drop("quote tick", quote, &instrument) {
                return;
            }

            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_quote_tick(quote);
            }
        }
    }

    fn process_trade_tick(&mut self, trade: &TradeTick) {
        if self.funding.enabled.get()
            && !self.execution_open(trade.instrument_id, Some(trade.ts_event.max(trade.ts_init)))
        {
            return;
        }

        if !self.config.trade_execution {
            return;
        }

        let instrument_id = trade.instrument_id;

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            if !check_trade_or_drop("trade tick", trade, &instrument) {
                return;
            }

            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_trade_tick(trade);
            }
        }
    }

    fn process_bar(&mut self, bar: &Bar) {
        if self.funding.enabled.get()
            && !self.execution_open(
                bar.bar_type.instrument_id(),
                Some(bar.ts_event.max(bar.ts_init)),
            )
        {
            return;
        }

        if !self.config.bar_execution {
            return;
        }

        let instrument_id = bar.bar_type.instrument_id();

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            if !check_bar_or_drop("bar", bar, &instrument) {
                return;
            }

            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_bar(bar);
            }
        }
    }

    fn process_order_book_deltas(&mut self, deltas: &OrderBookDeltas) {
        if self.maintenance.is_some() {
            return;
        }

        if self.funding.enabled.get()
            && !self.execution_open(
                deltas.instrument_id,
                Some(deltas.ts_event.max(deltas.ts_init)),
            )
        {
            return;
        }
        let instrument_id = deltas.instrument_id;

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id)
                && let Err(e) = engine.process_order_book_deltas(deltas)
            {
                log::error!("Error processing order book deltas: {e}");
            }
        }
    }

    fn process_instrument_status(&mut self, status: &InstrumentStatus) {
        if self.maintenance.is_some() {
            return;
        }

        if self.funding.enabled.get()
            && !self.execution_open(
                status.instrument_id,
                Some(status.ts_event.max(status.ts_init)),
            )
        {
            return;
        }
        let instrument_id = status.instrument_id;

        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            engine.process_status(status.action);
            return;
        }

        let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
        if let Some(instrument) = instrument {
            self.ensure_matching_engine(&instrument);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                engine.process_status(status.action);
            }
        } else {
            log::warn!(
                "Ignoring instrument status for {instrument_id}: instrument missing from cache",
            );
        }
    }

    fn process_instrument_close(&mut self, close: &InstrumentClose) {
        if self.maintenance.is_some() {
            return;
        }

        if self.funding.enabled.get()
            && !self.execution_open(close.instrument_id, Some(close.ts_event.max(close.ts_init)))
        {
            return;
        }
        let instrument_id = close.instrument_id;

        // A delayed close belongs to an existing exposure lifecycle. Unlike an
        // instrument status update, it must not recreate execution state from
        // cache after rotation/unsubscribe; pending-settlement ownership stays
        // with the already-initialized matching engine.
        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            engine.process_instrument_close(*close);
            self.sync_expired_cleanup(instrument_id);
        } else {
            log::warn!(
                "Ignoring instrument close for {instrument_id}: no existing matching engine",
            );
        }
    }

    fn is_expired_now(&self, instrument_id: InstrumentId) -> bool {
        let Some(engine) = self.matching_engines.get(&instrument_id) else {
            return false;
        };

        let now_ns = self.clock.borrow().timestamp_ns();
        engine
            .instrument
            .expiration_ns()
            .is_some_and(|ns| now_ns >= ns)
    }

    fn has_open_orders(&self, instrument_id: InstrumentId) -> bool {
        self.cache.borrow().has_orders_open(
            Some(&self.config.venue),
            Some(&instrument_id),
            None,
            None,
            None,
        )
    }

    fn sync_expired_cleanup(&mut self, instrument_id: InstrumentId) {
        if !self.is_expired_now(instrument_id) {
            return;
        }

        let has_open_positions = self.cache.borrow().has_positions_open(
            Some(&self.config.venue),
            Some(&instrument_id),
            None,
            None,
            None,
        );

        if has_open_positions {
            return;
        }

        self.matching_engines.remove(&instrument_id);
        self.cache
            .borrow_mut()
            .purge_instrument_skip_order_guard(instrument_id);
    }

    fn sync_expired_cleanup_many(&mut self, instrument_ids: &[InstrumentId]) {
        for &instrument_id in instrument_ids {
            self.sync_expired_cleanup(instrument_id);
        }
    }

    // Quote-only instruments may never receive event-driven cleanup. Retain expired engines with
    // open positions because this path cannot settle them. Open orders must also remain because
    // `Cache::purge_instrument_skip_order_guard` requires callers to terminalize order state first
    fn sweep_expired_engines(&mut self) {
        let expired_ids: Vec<InstrumentId> = self
            .matching_engines
            .keys()
            .copied()
            .filter(|instrument_id| {
                self.is_expired_now(*instrument_id) && !self.has_open_orders(*instrument_id)
            })
            .collect();

        self.sync_expired_cleanup_many(&expired_ids);
    }

    /// Routes a deferred [`TradingCommand`] to its venue-side apply helper.
    fn apply_trading_command(&mut self, cmd: &TradingCommand) -> anyhow::Result<()> {
        self.inflight_orders.remove(cmd);

        if (self.funding.enabled.get()
            || matches!(
                cmd,
                TradingCommand::SubmitOrder(_)
                    | TradingCommand::SubmitOrderList(_)
                    | TradingCommand::ModifyOrder(_)
                    | TradingCommand::ModifyOrders(_)
            ))
            && !self.execution_open(cmd.instrument_id(), None)
        {
            self.reject_command(cmd, "Sandbox execution window expired");
            return Ok(());
        }

        // Only a deferred command can overtake the submit that would have created the engine, so
        // build it here and let the venue raise the rejection.
        if matches!(
            cmd,
            TradingCommand::ModifyOrder(_)
                | TradingCommand::ModifyOrders(_)
                | TradingCommand::CancelOrder(_)
                | TradingCommand::CancelOrders(_)
        ) && !self.ensure_engine_for(cmd.instrument_id())
        {
            self.reject_command(cmd, "No matching engine for instrument");
            return Ok(());
        }

        match cmd {
            TradingCommand::SubmitOrder(cmd) => self.apply_submit_order(cmd)?,
            TradingCommand::SubmitOrderList(cmd) => {
                // Only a deferred leg has an `OrderSubmitted` out that nothing else would resolve
                for order in self.apply_submit_order_list(cmd) {
                    self.reject_submit_leg(cmd, &order, "No instrument for order");
                }
            }
            TradingCommand::ModifyOrder(cmd) => self.apply_modify_order(cmd),
            TradingCommand::ModifyOrders(cmd) => self.apply_batch_modify_orders(cmd),
            TradingCommand::CancelOrder(cmd) => self.apply_cancel_order(cmd),
            TradingCommand::CancelOrders(cmd) => self.apply_batch_cancel_orders(cmd),
            TradingCommand::CancelAllOrders(cmd) => self.apply_cancel_all_orders(cmd),
            TradingCommand::QueryOrder(_) | TradingCommand::QueryAccount(_) => {}
        }
        Ok(())
    }

    fn reject_command(&self, command: &TradingCommand, reason: &str) {
        self.reject_command_deduped(command, reason, &mut AHashSet::new());
    }

    /// Dispatches the rejection for `command`, skipping any order that has already received a
    /// modify or cancel rejection recorded in `pending_rejected`.
    fn reject_command_deduped(
        &self,
        command: &TradingCommand,
        reason: &str,
        pending_rejected: &mut AHashSet<ClientOrderId>,
    ) {
        let ts_now = self.clock.borrow().timestamp_ns();
        let account_id = self.account_id;
        let reason = Ustr::from(reason);

        let reject_submit = |trader_id, strategy_id, instrument_id, client_order_id| {
            self.dispatch_order_event(OrderEventAny::Rejected(OrderRejected::new(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                account_id,
                reason,
                UUID4::new(),
                ts_now,
                ts_now,
                false,
                false,
            )));
        };

        match command {
            TradingCommand::SubmitOrder(cmd) => reject_submit(
                cmd.trader_id,
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
            ),
            TradingCommand::SubmitOrderList(cmd) => {
                // Keyed per leg: the list's `instrument_id` is only representative
                let in_flight: Vec<(ClientOrderId, InstrumentId)> = self
                    .cache
                    .borrow()
                    .orders_for_ids(&cmd.order_list.client_order_ids, cmd)
                    .iter()
                    .filter(|order| !order.is_closed())
                    .map(|order| (order.client_order_id(), order.instrument_id()))
                    .collect();

                for (client_order_id, instrument_id) in in_flight {
                    reject_submit(
                        cmd.trader_id,
                        cmd.strategy_id,
                        instrument_id,
                        client_order_id,
                    );
                }
            }
            TradingCommand::ModifyOrder(cmd) => {
                if self.needs_rejection(cmd.client_order_id, pending_rejected) {
                    self.reject_modify(cmd, reason, ts_now);
                }
            }
            TradingCommand::ModifyOrders(cmd) => {
                for modify in &cmd.modifies {
                    if self.needs_rejection(modify.client_order_id, pending_rejected) {
                        self.reject_modify(modify, reason, ts_now);
                    }
                }
            }
            TradingCommand::CancelOrder(cmd) => {
                if self.needs_rejection(cmd.client_order_id, pending_rejected) {
                    self.reject_cancel(
                        cmd.trader_id,
                        cmd.strategy_id,
                        cmd.instrument_id,
                        cmd.client_order_id,
                        cmd.venue_order_id,
                        reason,
                        ts_now,
                    );
                }
            }
            TradingCommand::CancelOrders(cmd) => {
                for cancel in &cmd.cancels {
                    if self.needs_rejection(cancel.client_order_id, pending_rejected) {
                        self.reject_cancel(
                            cancel.trader_id,
                            cancel.strategy_id,
                            cancel.instrument_id,
                            cancel.client_order_id,
                            cancel.venue_order_id,
                            reason,
                            ts_now,
                        );
                    }
                }
            }
            // `CancelAllOrders` names no orders and the strategy marks none `PENDING_CANCEL` for
            // it, so there is no pending state to release and the FSM would refuse a rejection.
            TradingCommand::CancelAllOrders(_) => {}
            TradingCommand::QueryOrder(_) | TradingCommand::QueryAccount(_) => {}
        }
    }

    /// Returns whether a modify or cancel that will not reach the venue still has a rejection to
    /// raise for `client_order_id`, recording it in `pending_rejected`.
    ///
    /// An order closed while the command was in flight has none: the event that closed it already
    /// resolved the `PENDING_UPDATE` or `PENDING_CANCEL` a rejection would release, and the FSM
    /// has no transition from a closed status to a rejection.
    fn needs_rejection(
        &self,
        client_order_id: ClientOrderId,
        pending_rejected: &mut AHashSet<ClientOrderId>,
    ) -> bool {
        let is_closed = self
            .cache
            .borrow()
            .order(&client_order_id)
            .is_some_and(|order| order.is_closed());

        !is_closed && pending_rejected.insert(client_order_id)
    }

    fn reject_modify(&self, cmd: &ModifyOrder, reason: Ustr, ts_now: UnixNanos) {
        self.dispatch_order_event(OrderEventAny::ModifyRejected(OrderModifyRejected::new(
            cmd.trader_id,
            cmd.strategy_id,
            cmd.instrument_id,
            cmd.client_order_id,
            reason,
            UUID4::new(),
            ts_now,
            ts_now,
            false,
            cmd.venue_order_id,
            Some(self.account_id),
        )));
    }

    #[expect(clippy::too_many_arguments, reason = "mirrors the event's own fields")]
    fn reject_cancel(
        &self,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
        reason: Ustr,
        ts_now: UnixNanos,
    ) {
        self.dispatch_order_event(OrderEventAny::CancelRejected(OrderCancelRejected::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            reason,
            UUID4::new(),
            ts_now,
            ts_now,
            false,
            venue_order_id,
            Some(self.account_id),
        )));
    }

    fn dispatch_order_event(&self, event: OrderEventAny) {
        if let Some(handler) = &self.event_handler {
            handler(event);
        } else {
            msgbus::send_order_event(MessagingSwitchboard::exec_engine_process(), event);
        }
    }

    /// Creates the matching engine from the cached instrument when it does not exist yet, so it can
    /// answer for an order it has never seen. Returns whether an engine now exists.
    fn ensure_engine_for(&mut self, instrument_id: InstrumentId) -> bool {
        if !self.matching_engines.contains_key(&instrument_id) {
            let instrument = self.cache.borrow().instrument(&instrument_id).cloned();
            let Some(instrument) = instrument else {
                log::warn!(
                    "Cannot process command for {instrument_id}: instrument missing from cache",
                );
                return false;
            };
            self.ensure_matching_engine(&instrument);
        }

        self.matching_engines.contains_key(&instrument_id)
    }

    fn apply_submit_order(&mut self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.execution_open(cmd.instrument_id, None),
            "sandbox execution window expired"
        );
        let mut order = self.cache.borrow().try_order_owned(&cmd.client_order_id)?;

        let instrument_id = order.instrument_id();
        let instrument = self.cache.borrow().try_instrument(&instrument_id)?.clone();

        self.ensure_matching_engine(&instrument);

        let cache = self.cache.borrow();

        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            if self.maintenance.is_none()
                && let Some(quote) = cache.quote(&instrument_id)
                && check_quote_or_drop("cached quote tick", quote, &instrument)
            {
                engine.process_quote_tick(quote);
            }

            if self.config.trade_execution
                && let Some(trade) = cache.trade(&instrument_id)
                && check_trade_or_drop("cached trade tick", trade, &instrument)
            {
                engine.process_trade_tick(trade);
            }
        }
        drop(cache);

        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            engine.process_order(&mut order, self.account_id);
            self.sync_expired_cleanup(instrument_id);
        }

        Ok(())
    }

    /// Applies a submit-order-list command to the matching engines (venue-side), less the per-order
    /// `OrderSubmitted` dispatch kept by the client handler, returning the legs that could not
    /// reach a matching engine.
    fn apply_submit_order_list(&mut self, cmd: &SubmitOrderList) -> Vec<OrderAny> {
        if !self.execution_open(cmd.instrument_id, None) {
            return self
                .cache
                .borrow()
                .orders_for_ids(&cmd.order_list.client_order_ids, cmd);
        }
        let orders: Vec<OrderAny> = self
            .cache
            .borrow()
            .orders_for_ids(&cmd.order_list.client_order_ids, cmd);

        let mut cleanup_instrument_ids = Vec::new();
        let mut unresolved: Vec<OrderAny> = Vec::new();

        for order in &orders {
            if order.is_closed() {
                continue;
            }

            let instrument_id = order.instrument_id();
            if !cleanup_instrument_ids.contains(&instrument_id) {
                cleanup_instrument_ids.push(instrument_id);
            }
            let instrument = self.cache.borrow().instrument(&instrument_id).cloned();

            let Some(instrument) = instrument else {
                // Skipped per leg rather than failing the whole command: the legs that did reach
                // the venue are live orders.
                unresolved.push(order.clone());
                continue;
            };

            self.ensure_matching_engine(&instrument);

            let cache = self.cache.borrow();

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                if self.maintenance.is_none()
                    && let Some(quote) = cache.quote(&instrument_id)
                    && check_quote_or_drop("cached quote tick", quote, &instrument)
                {
                    engine.process_quote_tick(quote);
                }

                if self.config.trade_execution
                    && let Some(trade) = cache.trade(&instrument_id)
                    && check_trade_or_drop("cached trade tick", trade, &instrument)
                {
                    engine.process_trade_tick(trade);
                }
            }
            drop(cache);

            if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
                let mut order_clone = order.clone();
                engine.process_order(&mut order_clone, self.account_id);
            }
        }

        if !cleanup_instrument_ids.is_empty() {
            self.sync_expired_cleanup_many(&cleanup_instrument_ids);
        }

        unresolved
    }

    /// Rejects a single leg of `cmd` the venue could not accept, leaving its siblings untouched.
    fn reject_submit_leg(&self, cmd: &SubmitOrderList, order: &OrderAny, reason: &str) {
        let ts_now = self.clock.borrow().timestamp_ns();
        self.dispatch_order_event(OrderEventAny::Rejected(OrderRejected::new(
            cmd.trader_id,
            cmd.strategy_id,
            order.instrument_id(),
            order.client_order_id(),
            self.account_id,
            Ustr::from(reason),
            UUID4::new(),
            ts_now,
            ts_now,
            false,
            false,
        )));
    }

    fn apply_modify_order(&mut self, cmd: &ModifyOrder) {
        if !self.execution_open(cmd.instrument_id, None) {
            self.reject_command(
                &TradingCommand::ModifyOrder(cmd.clone()),
                "Sandbox execution window expired",
            );
            return;
        }
        let account_id = self.account_id;
        if let Some(engine) = self.matching_engines.get_mut(&cmd.instrument_id) {
            engine.process_modify(cmd, account_id);
        }
    }

    fn apply_batch_modify_orders(&mut self, cmd: &BatchModifyOrders) {
        let account_id = self.account_id;
        if let Some(engine) = self.matching_engines.get_mut(&cmd.instrument_id) {
            engine.process_batch_modify(cmd, account_id);
        }
    }

    fn apply_cancel_order(&mut self, cmd: &CancelOrder) {
        let account_id = self.account_id;
        if let Some(engine) = self.matching_engines.get_mut(&cmd.instrument_id) {
            engine.process_cancel(cmd, account_id);
        }
    }

    fn apply_cancel_all_orders(&mut self, cmd: &CancelAllOrders) {
        let instrument_id = cmd.instrument_id;
        if let Some(engine) = self.matching_engines.get_mut(&instrument_id) {
            engine.process_cancel_all(cmd, self.account_id);
        } else {
            log::debug!("No open orders to cancel for {instrument_id}: no matching engine");
        }
    }

    fn apply_batch_cancel_orders(&mut self, cmd: &BatchCancelOrders) {
        let account_id = self.account_id;
        if let Some(engine) = self.matching_engines.get_mut(&cmd.instrument_id) {
            engine.process_batch_cancel(cmd, account_id);
        }
    }

    /// Enqueues a trading command to be applied after its inbound latency elapses, as backtest
    /// `generate_inflight_command` does, but keyed off arrival rather than `command.ts_init()`.
    fn enqueue(&mut self, command: TradingCommand, now_ns: UnixNanos) {
        let leg_latency = self.command_leg_latency(&command);
        let due_ns = now_ns + leg_latency;

        let seq = self.inbound_seq;
        self.inbound_seq += 1;

        self.inflight_orders.insert(&command);
        self.inbound_queue.push(DelayedCommand {
            due_ns,
            seq,
            command,
        });

        self.arm_inbound_alert(now_ns);
    }

    /// Defers `command` by its inbound latency leg, or applies it inline when that leg is zero.
    ///
    /// A zero-leg command is still queued behind a head already due, so the two apply in
    /// `(due_ns, seq)` order.
    fn defer_or_apply(&mut self, command: TradingCommand) {
        if self.funding.enabled.get() && !self.execution_open(command.instrument_id(), None) {
            self.reject_command(&command, "Native funding boundary is unresolved");
            return;
        }
        let now_ns = self.clock.borrow().timestamp_ns();
        let head_is_due = self
            .inbound_queue
            .peek()
            .is_some_and(|delayed| delayed.due_ns <= now_ns);

        if head_is_due || self.command_leg_latency(&command) > DurationNanos::ZERO {
            self.enqueue(command, now_ns);
            return;
        }

        if let Err(e) = self.apply_trading_command(&command) {
            log::error!("Error applying command: {e}");
            self.reject_command(&command, "Command could not be applied at the venue");
        }
    }

    /// Returns the inbound latency leg for `command`, or zero when no model is set.
    fn command_leg_latency(&self, command: &TradingCommand) -> DurationNanos {
        let Some(latency_model) = self.config.latency_model.as_ref() else {
            return DurationNanos::ZERO;
        };

        match command {
            TradingCommand::SubmitOrder(_) | TradingCommand::SubmitOrderList(_) => {
                latency_model.get_insert_latency()
            }
            TradingCommand::ModifyOrder(_) | TradingCommand::ModifyOrders(_) => {
                latency_model.get_update_latency()
            }
            TradingCommand::CancelOrder(_)
            | TradingCommand::CancelOrders(_)
            | TradingCommand::CancelAllOrders(_) => latency_model.get_delete_latency(),
            TradingCommand::QueryOrder(_) | TradingCommand::QueryAccount(_) => DurationNanos::ZERO,
        }
    }

    fn pop_due(&mut self, now_ns: UnixNanos) -> Option<DelayedCommand> {
        self.inbound_queue
            .peek()
            .is_some_and(|delayed| delayed.due_ns <= now_ns)
            .then(|| self.inbound_queue.pop().expect("peek returned Some"))
    }

    fn on_quote_tick(inner: &Rc<RefCell<Self>>, quote: &QuoteTick) {
        if inner.borrow().maintenance.is_some() {
            if let Err(e) = Self::schedule_maintenance_quote(inner, *quote) {
                log::error!("Native maintenance quote failed: {e}");
            }
            return;
        }
        SandboxFunding::schedule(inner);
        if !inner
            .borrow()
            .execution_open(quote.instrument_id, Some(quote.ts_init.max(quote.ts_event)))
        {
            Self::drain_inbound(inner);
            return;
        }

        if inner
            .borrow()
            .minimum_quote_init
            .is_some_and(|cut| quote.ts_init < cut)
        {
            return;
        }
        Self::drain_inbound(inner);
        inner.borrow_mut().process_quote_tick(quote);
    }

    fn on_trade_tick(inner: &Rc<RefCell<Self>>, trade: &TradeTick) {
        SandboxFunding::schedule(inner);
        Self::drain_inbound(inner);
        inner.borrow_mut().process_trade_tick(trade);
    }

    fn on_bar(inner: &Rc<RefCell<Self>>, bar: &Bar) {
        SandboxFunding::schedule(inner);
        Self::drain_inbound(inner);
        inner.borrow_mut().process_bar(bar);
    }

    fn on_order_book_deltas(inner: &Rc<RefCell<Self>>, deltas: &OrderBookDeltas) {
        SandboxFunding::schedule(inner);
        Self::drain_inbound(inner);
        inner.borrow_mut().process_order_book_deltas(deltas);
    }

    fn on_instrument_status(inner: &Rc<RefCell<Self>>, status: &InstrumentStatus) {
        SandboxFunding::schedule(inner);
        Self::drain_inbound(inner);
        inner.borrow_mut().process_instrument_status(status);
    }

    fn on_instrument_close(inner: &Rc<RefCell<Self>>, close: &InstrumentClose) {
        SandboxFunding::schedule(inner);
        Self::drain_inbound(inner);
        inner.borrow_mut().process_instrument_close(close);
    }

    /// Applies every inbound command whose latency has elapsed, in `(due_ns, seq)` order, then
    /// arms the alert for the earliest command still queued.
    ///
    /// Called at the top of each data handler and public `process_*` method, so a command due
    /// before a tick is processed is applied before that tick, and by the alert when no data is
    /// flowing.
    fn drain_inbound(inner: &Rc<RefCell<Self>>) {
        SandboxFunding::schedule(inner);
        // The alert fires on the runner task, where a nested msgbus dispatch may already hold the
        // borrow; the next data tick or public `process_*` call releases the queue instead.
        let Ok(mut this) = inner.try_borrow_mut() else {
            log::debug!("Skipping sandbox inbound drain due to active borrow");
            return;
        };

        if this.maintenance_in_flight
            || this.funding.enabled.get()
                && (this.funding.in_flight.get()
                    || this
                        .funding
                        .owner
                        .borrow()
                        .blocks_execution(this.clock.borrow().timestamp_ns()))
        {
            return;
        }

        if this.config.latency_model.is_none() {
            return;
        }

        let now_ns = this.clock.borrow().timestamp_ns();

        while let Some(delayed) = this.pop_due(now_ns) {
            if let Err(e) = this.apply_trading_command(&delayed.command) {
                log::error!("Error applying deferred command: {e}");
                this.reject_command(
                    &delayed.command,
                    "Command could not be applied at the venue",
                );
            }

            if this.maintenance.is_some()
                && matches!(
                    delayed.command,
                    TradingCommand::SubmitOrder(_)
                        | TradingCommand::SubmitOrderList(_)
                        | TradingCommand::ModifyOrder(_)
                        | TradingCommand::ModifyOrders(_)
                )
            {
                if let Err(e) = this.maintenance_after_command() {
                    log::error!("Native delayed maintenance command failed: {e}");
                }
                break;
            }
        }

        this.arm_inbound_alert(now_ns);
    }

    /// Takes every command still deferred by inbound latency, ordered for [`Self::reject_discarded`]
    /// to unwind last-issued-first.
    fn take_inbound_queue(&mut self) -> Vec<DelayedCommand> {
        self.inflight_orders.clear();

        if self.inbound_queue.is_empty() {
            return Vec::new();
        }

        log::warn!(
            "Discarding {} command(s) still in flight at stop",
            self.inbound_queue.len(),
        );

        // Unwind last-issued-first by `seq`, so each rejection restores the state the command
        // before it established.
        let mut discarded = std::mem::take(&mut self.inbound_queue).into_vec();
        discarded.sort_unstable_by_key(|delayed| std::cmp::Reverse(delayed.seq));
        discarded
    }

    /// Rejects every command [`Self::take_inbound_queue`] discarded.
    fn reject_discarded(&self, discarded: Vec<DelayedCommand>) {
        // Shared across the whole unwind: one order can have several pending commands in flight,
        // but only the first rejection it receives has a valid FSM transition.
        let mut pending_rejected = AHashSet::new();

        for delayed in discarded {
            self.reject_command_deduped(
                &delayed.command,
                "Client stopped before the command was sent",
                &mut pending_rejected,
            );
        }
    }

    /// Clears every command still deferred by inbound latency, without rejecting any of them:
    /// `reset` discards all client state, so there is nothing to release.
    fn clear_inbound_queue(&mut self) {
        self.inbound_queue.clear();
        self.inflight_orders.clear();
    }

    /// (Re)arms the `LiveClock` alert for the earliest queued `due_ns` while that is still ahead
    /// of `now_ns`.
    ///
    /// A due time already reached is not armed for: the drain releasing it is already pending on
    /// the runner, and an alert at a time the clock has passed only asks the clock to warn. The
    /// clock reads its own time again when arming, so a leg shorter than that gap can still warn.
    fn arm_inbound_alert(&self, now_ns: UnixNanos) {
        let Some(earliest_due) = self.inbound_queue.peek().map(|delayed| delayed.due_ns) else {
            return;
        };

        if earliest_due <= now_ns {
            return;
        }

        let name = inbound_alert_name(self.client_id);
        let armed_ns = self.clock.borrow().next_time_ns(&name);

        match armed_ns {
            // Already armed no later than the new earliest due, so that alert still wakes the drain
            Some(armed_ns) if armed_ns <= earliest_due => return,
            // Canceling first avoids the warning `replace_existing_timer` would log
            Some(_) => self.clock.borrow_mut().cancel_timer(&name),
            None => {}
        }

        let inner_weak = self.self_weak.clone();
        let alert_name = name.clone();

        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_event: TimeEvent| {
            let Some(inner_rc) = inner_weak.upgrade() else {
                return;
            };

            // Retire the spent one-shot before the drain's exit path reads it back as still armed
            if let Ok(this) = inner_rc.try_borrow() {
                this.clock.borrow_mut().cancel_timer(&alert_name);
            }

            // The pass arms for whatever it leaves queued
            Self::drain_inbound(&inner_rc);
        });

        if let Err(e) = self.clock.borrow_mut().set_time_alert_ns(
            &name,
            earliest_due,
            Some(TimeEventCallback::from(callback)),
            Some(true),
        ) {
            log::error!("Failed to arm sandbox inbound alert '{name}': {e}");
        }
    }
}

fn inbound_alert_name(client_id: ClientId) -> String {
    format!("{client_id}-sandbox-inbound-alert")
}

fn check_quote_or_drop(context: &str, quote: &QuoteTick, instrument: &InstrumentAny) -> bool {
    let price_domain = !instrument.is_inverse()
        || (quote.bid_price <= quote.ask_price
            && [quote.bid_price, quote.ask_price].into_iter().all(|price| {
                instrument.try_normalize_price(price).is_ok()
                    && price.raw() > 0
                    && instrument.min_price().is_none_or(|min| price >= min)
                    && instrument.max_price().is_none_or(|max| price <= max)
            }));

    if quote_matches_instrument_precision(quote, instrument) && price_domain {
        return true;
    }

    log::warn!(
        "Dropping {context} for {} due to invalid precision or inverse price domain \
         (bid_px={}, ask_px={}, bid_sz={}, ask_sz={}, expected_price={}, expected_size={})",
        instrument.id(),
        quote.bid_price.precision,
        quote.ask_price.precision,
        quote.bid_size.precision,
        quote.ask_size.precision,
        instrument.price_precision(),
        instrument.size_precision(),
    );
    false
}

fn check_trade_or_drop(context: &str, trade: &TradeTick, instrument: &InstrumentAny) -> bool {
    if trade_matches_instrument_precision(trade, instrument) {
        return true;
    }

    log::warn!(
        "Dropping {context} for {} due to precision mismatch \
         (px={}, sz={}, expected_price={}, expected_size={})",
        instrument.id(),
        trade.price.precision,
        trade.size.precision,
        instrument.price_precision(),
        instrument.size_precision(),
    );
    false
}

fn check_bar_or_drop(context: &str, bar: &Bar, instrument: &InstrumentAny) -> bool {
    if bar_matches_instrument_precision(bar, instrument) {
        return true;
    }

    log::warn!(
        "Dropping {context} for {} due to precision mismatch \
         (open={}, high={}, low={}, close={}, volume={}, expected_price={}, expected_size={})",
        instrument.id(),
        bar.open.precision,
        bar.high.precision,
        bar.low.precision,
        bar.close.precision,
        bar.volume.precision,
        instrument.price_precision(),
        instrument.size_precision(),
    );
    false
}

fn quote_matches_instrument_precision(quote: &QuoteTick, instrument: &InstrumentAny) -> bool {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    quote.bid_price.precision == price_precision
        && quote.ask_price.precision == price_precision
        && quote.bid_size.precision == size_precision
        && quote.ask_size.precision == size_precision
}

fn trade_matches_instrument_precision(trade: &TradeTick, instrument: &InstrumentAny) -> bool {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    trade.price.precision == price_precision && trade.size.precision == size_precision
}

fn bar_matches_instrument_precision(bar: &Bar, instrument: &InstrumentAny) -> bool {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();

    bar.open.precision == price_precision
        && bar.high.precision == price_precision
        && bar.low.precision == price_precision
        && bar.close.precision == price_precision
        && bar.volume.precision == size_precision
}
