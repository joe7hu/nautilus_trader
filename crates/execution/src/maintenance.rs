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

//! Native account-scoped maintenance decisions and original liquidation application.

use std::{cell::RefCell, collections::HashSet, rc::Rc};

use nautilus_common::cache::Cache;
use nautilus_core::UnixNanos;
use nautilus_model::{
    accounts::{Account, AccountAny},
    data::QuoteTick,
    enums::{OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce},
    events::OrderEventAny,
    identifiers::{AccountId, ClientOrderId, InstrumentId, PositionId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    position::Position,
    types::{Currency, Money, Price, Quantity, price::PRICE_RAW_MAX},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::matching_engine::OrderMatchingEngine;

/// The native account, positions and exit-side valuation which caused an attempt.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaintenanceDecision {
    pub account: AccountAny,
    pub positions: Vec<Position>,
    pub quotes: Vec<QuoteTick>,
    pub original_orders: Vec<OrderAny>,
    pub currency: Currency,
    pub exit_equity: Money,
    pub maintenance: Money,
    pub threshold: Decimal,
    pub observed_ns: UnixNanos,
}

/// Native application outcome of an original maintenance attempt.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MaintenanceOutcome {
    /// The original fills closed the account's affected exposure.
    Flat,
    /// Original fills applied while some native exposure remains.
    Residual,
    /// Original terminal cancellations applied without another fill.
    NoExecution,
}

/// Original terminal native actions and the coherent resulting financial cut.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaintenanceReceipt {
    pub outcome: MaintenanceOutcome,
    pub decision: MaintenanceDecision,
    pub orders: Vec<OrderAny>,
    pub account: AccountAny,
    pub positions: Vec<Position>,
}

/// Read-only tick-lattice boundary of the same native maintenance predicate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaintenanceThreshold {
    pub price: Option<Price>,
    pub exit_side: PositionSide,
    pub account: AccountAny,
    pub position: Position,
    pub quote: QuoteTick,
    pub minimum: Price,
    pub maximum: Price,
}

/// One original explicit close and its native application, distinct from liquidation attempts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorCloseCheckpoint {
    intent: OrderAny,
    before_account: AccountAny,
    before_position: Position,
    submitted: bool,
    receipt: Option<OperatorCloseReceipt>,
    acknowledged: bool,
}

/// The original terminal operator order and its actual native financial cut.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorCloseReceipt {
    /// Original terminal native order, including denied or unfilled outcomes.
    pub order: OrderAny,
    /// Actual original native account after the order settled.
    pub account: AccountAny,
    /// Actual original native position, including any residual quantity.
    pub position: Position,
}

impl OperatorCloseCheckpoint {
    /// Returns the one original operator command identity.
    pub fn client_order_id(&self) -> ClientOrderId {
        self.intent.client_order_id()
    }

    /// Returns only an actual complete native application.
    pub fn receipt(&self) -> Option<&OperatorCloseReceipt> {
        self.receipt.as_ref()
    }

    /// Returns whether the exact original outcome has a durable acknowledgement.
    pub fn acknowledged(&self) -> bool {
        self.acknowledged
    }
}

/// Retained bounded original actions; incomplete application never permits another attempt.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MaintenanceCheckpoint {
    receipts: Vec<MaintenanceReceipt>,
    pending: Option<(MaintenanceDecision, Vec<ClientOrderId>)>,
    acknowledged: usize,
    uncertainty: Option<String>,
    account: Option<AccountAny>,
    positions: Vec<Position>,
    quote: Option<QuoteTick>,
    deferral: Option<String>,
    ratio: Option<Decimal>,
    max_attempts: Option<usize>,
    opening: Option<(ClientOrderId, InstrumentId, OrderSide, Quantity)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operator_close: Option<Box<OperatorCloseCheckpoint>>,
}

/// Owns native maintenance decisions, once-only dispatch and application confirmation.
#[derive(Debug)]
pub struct MaintenanceOwner {
    account_id: AccountId,
    cache: Rc<RefCell<Cache>>,
    ratio: Decimal,
    max_attempts: usize,
    state: MaintenanceCheckpoint,
    quote: Option<QuoteTick>,
    baseline_orders: Vec<OrderAny>,
}

impl MaintenanceCheckpoint {
    /// Returns the original full native account when initialization has applied.
    pub fn account(&self) -> Option<&AccountAny> {
        self.account.as_ref()
    }
    /// Returns original native positions at this coherent cut.
    pub fn positions(&self) -> &[Position] {
        &self.positions
    }
    /// Returns the bounded complete original maintenance receipts.
    pub fn receipts(&self) -> &[MaintenanceReceipt] {
        &self.receipts
    }

    /// Returns the original separately approved operator-close lifecycle.
    pub fn operator_close(&self) -> Option<&OperatorCloseCheckpoint> {
        self.operator_close.as_deref()
    }
}

impl MaintenanceOwner {
    /// Creates one native account owner with a positive ratio and bounded close attempts.
    ///
    /// # Errors
    ///
    /// Returns an error for a nonpositive ratio or an empty attempt budget.
    pub fn new(
        account_id: AccountId,
        cache: Rc<RefCell<Cache>>,
        ratio: Decimal,
        max_attempts: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            ratio > Decimal::ZERO && max_attempts > 0,
            "invalid maintenance policy"
        );
        let state = {
            let initial = cache.borrow();
            MaintenanceCheckpoint {
                account: initial.account_owned(&account_id),
                positions: initial
                    .positions(None, None, None, Some(&account_id), None)
                    .into_iter()
                    .map(|position| position.cloned())
                    .collect(),
                ..MaintenanceCheckpoint::default()
            }
        };
        let baseline_orders = cache
            .borrow()
            .orders(None, None, None, Some(&account_id), None)
            .into_iter()
            .map(|order| (*order).clone())
            .collect();
        Ok(Self {
            baseline_orders,
            account_id,
            cache,
            ratio,
            max_attempts,
            state,
            quote: None,
        })
    }

    /// Returns whether native account application is settled even if capture is not acknowledged.
    pub fn can_capture(&self) -> bool {
        self.state.pending.is_none()
            && self.state.uncertainty.is_none()
            && self
                .state
                .operator_close
                .as_ref()
                .is_none_or(|close| close.receipt.is_some())
            && self.positions_applied()
            && self.orders_applied()
    }

    fn orders_applied(&self) -> bool {
        let Ok(cache) = self.cache.try_borrow() else {
            return false;
        };
        cache
            .orders(None, None, None, Some(&self.account_id), None)
            .iter()
            .all(|order| {
                order.order_type() == OrderType::Market
                    || !matches!(
                        order.status(),
                        OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected
                    )
                    || self.baseline_orders.iter().any(|original| {
                        serde_json::to_value(original).is_ok_and(|original| {
                            serde_json::to_value(&**order).is_ok_and(|current| original == current)
                        })
                    })
                    || cache.order_account_applied(order, None)
            })
    }

    fn positions_applied(&self) -> bool {
        let Ok(cache) = self.cache.try_borrow() else {
            return false;
        };
        let current = cache.positions(None, None, None, Some(&self.account_id), None);
        current.iter().all(|position| {
            self.state.positions.iter().any(|original| {
                original.id == position.id
                    && serde_json::to_value(original).is_ok_and(|original| {
                        serde_json::to_value(&**position).is_ok_and(|current| original == current)
                    })
            }) || cache.position_account_applied(position, self.state.account.as_ref())
        })
    }

    /// Returns whether no original action awaits application or durable acknowledgement.
    pub fn ready(&self) -> bool {
        self.can_capture()
            && self.state.deferral.is_none()
            && self.state.acknowledged == self.state.receipts.len()
            && self
                .state
                .operator_close
                .as_ref()
                .is_none_or(|close| close.acknowledged)
    }

    /// Returns whether a fresh source may resolve a no-effect deferral.
    pub fn can_observe(&self) -> bool {
        self.can_capture()
            && self.state.acknowledged == self.state.receipts.len()
            && self
                .state
                .operator_close
                .as_ref()
                .is_none_or(|close| close.acknowledged)
    }

    /// Closes exposure ingress until another valid original source cut is observed.
    pub fn defer(&mut self, reason: String) {
        self.state.deferral = Some(reason);
    }

    /// Checks native balance identity and current original-source freshness before opening exposure.
    pub fn opening_ready(&self, instrument: InstrumentId, now: UnixNanos) -> bool {
        self.ready()
            && self.quote.is_some_and(|quote| {
                quote.instrument_id == instrument
                    && quote.ts_event <= now
                    && quote.ts_init <= now
                    && now.as_u64().saturating_sub(quote.ts_event.as_u64()) <= 5_000_000_000
                    && now.as_u64().saturating_sub(quote.ts_init.as_u64()) <= 5_000_000_000
            })
            && self.cache.try_borrow().is_ok_and(|cache| {
                cache
                    .account_owned(&self.account_id)
                    .is_some_and(|account| {
                        matches!(account, AccountAny::Margin(_))
                            && cache.instrument(&instrument).is_some_and(|instrument| {
                                account
                                    .balance_total(Some(instrument.settlement_currency()))
                                    .is_some()
                            })
                    })
            })
    }

    /// Claims the one original external opening before any submission event is emitted.
    ///
    /// # Errors
    ///
    /// Returns an error for repeated/new exposure or unsupported original order terms.
    pub fn claim_opening(&mut self, order: &OrderAny) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.ready()
                && self.state.opening.is_none()
                && order
                    .account_id()
                    .is_none_or(|account| account == self.account_id)
                && order.order_type() == OrderType::Limit
                && order.time_in_force() == TimeInForce::Gtc
                && !order.is_reduce_only()
                && !order.is_emulated()
                && order.contingency_type().is_none(),
            "native maintenance permits one original limit opening"
        );
        let baseline = self.checkpoint();
        self.state.account = baseline.account;
        self.state.positions = baseline.positions;
        self.state.opening = Some((
            order.client_order_id(),
            order.instrument_id(),
            order.order_side(),
            order.quantity(),
        ));
        Ok(())
    }

    /// Retains one original operator close before it is handed to native risk.
    ///
    /// # Errors
    ///
    /// Returns an error for repeated intent, unresolved maintenance or a changed original scope.
    pub fn prepare_operator_close(
        &mut self,
        order: &OrderAny,
        position_id: PositionId,
        now: UnixNanos,
    ) -> anyhow::Result<()> {
        let cache = self.cache.borrow();
        let position = cache
            .position(&position_id)
            .map(|position| position.cloned())
            .ok_or_else(|| anyhow::anyhow!("original operator position unavailable"))?;
        let account = cache
            .account_owned(&self.account_id)
            .ok_or_else(|| anyhow::anyhow!("original operator account unavailable"))?;
        let instrument = cache.try_instrument(&position.instrument_id)?;
        let originals = cache.orders(None, None, None, Some(&self.account_id), None);
        anyhow::ensure!(
            self.state.operator_close.is_none()
                && self.opening_ready(position.instrument_id, now)
                && self
                    .state
                    .opening
                    .is_some_and(|(id, instrument, side, _)| id == position.opening_order_id
                        && instrument == position.instrument_id
                        && side == position.entry
                        && cache.position_id(&id).copied() == Some(position.id)
                        && cache.order(&id).is_some_and(|opening| opening.is_closed()
                            && opening.filled_qty() >= position.quantity
                            && opening.strategy_id() == position.strategy_id
                            && opening.trader_id() == position.trader_id))
                && position.account_id == self.account_id
                && position.is_open()
                && cache
                    .positions(None, None, None, Some(&self.account_id), None)
                    .len()
                    == 1
                && cache.order(&order.client_order_id()).is_none()
                && order.status() == OrderStatus::Initialized
                && order.account_id().is_none()
                && order.instrument_id() == position.instrument_id
                && order.strategy_id() == position.strategy_id
                && order.trader_id() == position.trader_id
                && Some(order.order_side()) == position.closing_order_side()
                && order.quantity() == position.quantity
                && order.quantity().precision == instrument.size_precision()
                && (order.quantity().as_decimal() % instrument.size_increment().as_decimal())
                    .is_zero()
                && order.order_type() == OrderType::Market
                && order.time_in_force() == TimeInForce::Ioc
                && order.is_reduce_only()
                && !order.is_quote_quantity()
                && !order.is_emulated()
                && order.contingency_type().is_none()
                && originals.len() <= 3
                && originals.iter().all(|original| original.is_closed()),
            "native operator close scope or lifecycle unavailable"
        );
        let quantum = Decimal::new(1, u32::from(position.size_precision));
        let lots = position
            .quantity
            .as_decimal()
            .checked_div(quantum)
            .ok_or_else(|| anyhow::anyhow!("native operator close headroom overflow"))?;
        anyhow::ensure!(
            lots.fract().is_zero() && lots <= Decimal::from(256),
            "native operator close fill headroom exceeds bound"
        );
        let fills = usize::try_from(lots.normalize().mantissa())?;
        let order_events = originals
            .iter()
            .map(|original| original.events().len())
            .sum::<usize>();
        anyhow::ensure!(
            order_events + 4 + fills <= 256 && account.events().len() + 4 + 2 * fills <= 256,
            "native operator close event headroom exhausted"
        );
        self.state.operator_close = Some(Box::new(OperatorCloseCheckpoint {
            intent: order.clone(),
            before_account: account,
            before_position: position,
            submitted: false,
            receipt: None,
            acknowledged: false,
        }));
        Ok(())
    }

    /// Identifies only the prepared original which may pass the closed ingress barrier.
    fn operator_submission_ready(&self, order: &OrderAny, position: Option<PositionId>) -> bool {
        self.state.operator_close.as_ref().is_some_and(|close| {
            !close.submitted
                && position == Some(close.before_position.id)
                && self.cache.try_borrow().is_ok_and(|cache| {
                    cache.position_id(&order.client_order_id()).copied() == position
                })
                && close.receipt.is_none()
                && serde_json::to_value(order).is_ok_and(|value| {
                    serde_json::to_value(&close.intent).is_ok_and(|intent| value == intent)
                })
        }) && self.state.uncertainty.is_none()
            && self.state.pending.is_none()
            && self.state.acknowledged == self.state.receipts.len()
    }

    /// Returns only the original prepared close which can still reach its terminal outcome.
    pub fn operator_pending(&self) -> Option<ClientOrderId> {
        self.state
            .operator_close
            .as_ref()
            .filter(|close| close.receipt.is_none())
            .map(|close| close.intent.client_order_id())
    }

    /// Rechecks the original execution source while allowing only its already prepared identity.
    pub fn operator_execution_ready(
        &self,
        original: ClientOrderId,
        instrument: InstrumentId,
        now: UnixNanos,
    ) -> bool {
        self.operator_pending() == Some(original)
            && self
                .state
                .operator_close
                .as_ref()
                .is_some_and(|close| close.intent.instrument_id() == instrument)
            && self.state.uncertainty.is_none()
            && self.state.pending.is_none()
            && self.state.deferral.is_none()
            && self.state.acknowledged == self.state.receipts.len()
            && self.quote.is_some_and(|quote| {
                quote.ts_init <= now
                    && quote.ts_event <= now
                    && now.as_u64().saturating_sub(quote.ts_init.as_u64()) <= 5_000_000_000
                    && now.as_u64().saturating_sub(quote.ts_event.as_u64()) <= 5_000_000_000
            })
    }

    /// Claims the exact prepared original once, before any submitted event.
    ///
    /// # Errors
    ///
    /// Returns an error for a different or already claimed native order.
    pub fn claim_operator_submission(
        &mut self,
        order: &OrderAny,
        position: Option<PositionId>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.operator_submission_ready(order, position),
            "native operator close already claimed or changed"
        );
        self.state
            .operator_close
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("native operator close missing"))?
            .submitted = true;
        Ok(())
    }

    /// Completes only actual terminal original order and account/position application.
    ///
    /// # Errors
    ///
    /// Returns an error for changed scope or an inconsistent native application.
    pub fn complete_operator_close(&mut self) -> anyhow::Result<bool> {
        let Some(close) = self.state.operator_close.as_ref() else {
            return Ok(false);
        };

        if close.receipt.is_some() {
            return Ok(true);
        }
        let cache = self.cache.borrow();
        let Some(order) = cache
            .order(&close.intent.client_order_id())
            .map(|order| order.clone())
        else {
            return Ok(false);
        };

        if !order.is_closed() {
            return Ok(false);
        }
        let account = cache
            .account_owned(&self.account_id)
            .ok_or_else(|| anyhow::anyhow!("native operator result account missing"))?;
        let position = cache
            .position(&close.before_position.id)
            .map(|position| position.cloned())
            .ok_or_else(|| anyhow::anyhow!("native operator result position missing"))?;
        let receipt = OperatorCloseReceipt {
            order,
            account,
            position,
        };
        Self::validate_operator_close(close, &receipt, &cache, true)?;
        drop(cache);
        self.state
            .operator_close
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("native operator intent missing"))?
            .receipt = Some(receipt);
        Ok(true)
    }

    fn validate_operator_close(
        close: &OperatorCloseCheckpoint,
        receipt: &OperatorCloseReceipt,
        cache: &Cache,
        require_application: bool,
    ) -> anyhow::Result<()> {
        let order = &receipt.order;
        let before = &close.before_position;
        let position = &receipt.position;
        let fills = order
            .events()
            .iter()
            .filter_map(|event| {
                if let OrderEventAny::Filled(fill) = event {
                    Some(fill)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            order.is_closed()
                && close.intent.status() == OrderStatus::Initialized
                && close.intent.account_id().is_none()
                && close.intent.venue_order_id().is_none()
                && close.intent.events().len() == 1
                && close.intent.filled_qty().is_zero()
                && before.is_open()
                && close.before_account.id() == before.account_id
                && !close.before_account.events().is_empty()
                && (fills.is_empty() || close.submitted)
                && order.client_order_id() == close.intent.client_order_id()
                && order.events().starts_with(&close.intent.events())
                && order.instrument_id() == before.instrument_id
                && order.strategy_id() == before.strategy_id
                && order.trader_id() == before.trader_id
                && Some(order.order_side()) == before.closing_order_side()
                && order.quantity() == before.quantity
                && order.filled_qty() <= order.quantity()
                && order.order_type() == OrderType::Market
                && order.time_in_force() == TimeInForce::Ioc
                && order.is_reduce_only()
                && !order.is_quote_quantity()
                && !order.is_emulated()
                && order.contingency_type().is_none()
                && (order.account_id() == Some(before.account_id)
                    || (order.status() == OrderStatus::Denied && order.account_id().is_none()))
                && (order.venue_order_id().is_some()
                    || matches!(order.status(), OrderStatus::Denied | OrderStatus::Rejected))
                && cache.position_id(&order.client_order_id()).copied() == Some(before.id)
                && position.id == before.id
                && position.account_id == before.account_id
                && position.instrument_id == before.instrument_id
                && position.strategy_id == before.strategy_id
                && position.entry == before.entry
                && position.quantity <= before.quantity
                && position.events.starts_with(&before.events)
                && position.events.len() == before.events.len() + fills.len()
                && position.events[before.events.len()..]
                    .iter()
                    .all(|event| fills.contains(&event))
                && serde_json::to_value(&position.adjustments)?
                    == serde_json::to_value(&before.adjustments)?
                && receipt.account.id() == before.account_id
                && receipt
                    .account
                    .events()
                    .starts_with(&close.before_account.events())
                && serde_json::to_value(cache.try_order_owned(&order.client_order_id())?)?
                    == serde_json::to_value(order)?
                && cache
                    .position(&before.id)
                    .is_some_and(|current| current.events.starts_with(&position.events))
                && cache
                    .account_owned(&before.account_id)
                    .is_some_and(|current| current.events().starts_with(&receipt.account.events())),
            "native operator original result scope mismatch"
        );

        if require_application {
            anyhow::ensure!(
                serde_json::to_value(cache.position(&before.id).map(|position| position.cloned()))?
                    == serde_json::to_value(Some(position))?
                    && serde_json::to_value(cache.account_owned(&before.account_id))?
                        == serde_json::to_value(Some(&receipt.account))?,
                "native operator completion cut changed"
            );
        }

        if fills.is_empty() {
            anyhow::ensure!(
                serde_json::to_value(position)? == serde_json::to_value(before)?,
                "unfilled native operator close changed position"
            );
            anyhow::ensure!(
                serde_json::to_value(&receipt.account)?
                    == serde_json::to_value(&close.before_account)?
                    || !require_application
                    || cache.order_account_applied(order, Some(&close.before_account)),
                "unfilled native operator account application missing"
            );
        } else if require_application {
            anyhow::ensure!(
                cache.position_account_applied(position, Some(&close.before_account)),
                "native operator position account application missing"
            );
        }
        Ok(())
    }

    /// Returns whether this same original operator receipt already has its acknowledgement.
    pub fn operator_acknowledged(&self, original: ClientOrderId) -> bool {
        self.state
            .operator_close
            .as_ref()
            .is_some_and(|close| close.client_order_id() == original && close.acknowledged)
    }

    /// Acknowledges the exact operator original after its durable financial capture.
    ///
    /// # Errors
    ///
    /// Returns an error before a terminal application or for a different original identity.
    pub fn acknowledge_operator_close(&mut self, original: ClientOrderId) -> anyhow::Result<()> {
        let close = self
            .state
            .operator_close
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("native operator close absent"))?;
        anyhow::ensure!(
            close.intent.client_order_id() == original
                && close.receipt.is_some()
                && self.state.pending.is_none()
                && self.state.uncertainty.is_none()
                && self.state.acknowledged == self.state.receipts.len(),
            "native operator acknowledgement mismatch"
        );
        close.acknowledged = true;
        Ok(())
    }

    /// Checks that a modification cannot increase the one originally admitted exposure.
    pub fn modification_allowed(&self, id: ClientOrderId, quantity: Option<Quantity>) -> bool {
        self.ready()
            && self.state.opening.is_some_and(|(original, _, _, cap)| {
                original == id && quantity.is_none_or(|quantity| quantity <= cap)
            })
    }

    /// Retains an uncertain dispatch permanently until explicit original-state recovery.
    pub fn fail(&mut self, reason: String) {
        self.state.uncertainty.get_or_insert(reason);
    }

    /// Returns the complete bounded owner checkpoint without changing native state.
    pub fn checkpoint(&self) -> MaintenanceCheckpoint {
        let mut original = self.state.clone();
        let cache = self.cache.borrow();
        original.account = cache.account_owned(&self.account_id);
        original.positions = cache
            .positions(None, None, None, Some(&self.account_id), None)
            .into_iter()
            .map(|p| p.cloned())
            .collect();
        original.quote = self.quote;
        original.ratio = Some(self.ratio);
        original.max_attempts = Some(self.max_attempts);
        original
    }

    /// Restores an exact settled original cut while leaving the destination disarmed.
    ///
    /// # Errors
    ///
    /// Returns an error for pending, uncertain or inconsistent native originals.
    pub fn restore(&mut self, original: MaintenanceCheckpoint) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state.receipts.is_empty()
                && self.state.operator_close.is_none()
                && self.state.pending.is_none()
                && original.pending.is_none()
                && original.uncertainty.is_none()
                && original.receipts.len() <= self.max_attempts
                && original.acknowledged <= original.receipts.len()
                && original.ratio == Some(self.ratio)
                && original.max_attempts == Some(self.max_attempts),
            "unsupported native maintenance checkpoint"
        );
        let opening: Vec<_> = self
            .cache
            .borrow()
            .orders(None, None, None, Some(&self.account_id), None)
            .into_iter()
            .filter(|order| order.order_type() == OrderType::Limit)
            .map(|order| (*order).clone())
            .collect();
        anyhow::ensure!(
            match original.opening {
                Some((id, instrument, side, quantity)) =>
                    opening.len() == 1
                        && opening[0].client_order_id() == id
                        && opening[0].instrument_id() == instrument
                        && opening[0].order_side() == side
                        && opening[0].quantity() <= quantity,
                None => opening.is_empty(),
            },
            "native maintenance opening lineage mismatch"
        );
        let current = self.checkpoint();
        anyhow::ensure!(
            serde_json::to_value(&current.account)? == serde_json::to_value(&original.account)?
                && serde_json::to_value(&current.positions)?
                    == serde_json::to_value(&original.positions)?,
            "native maintenance restored account/position mismatch"
        );
        let closes: HashSet<_> = self
            .cache
            .borrow()
            .orders(None, None, None, Some(&self.account_id), None)
            .into_iter()
            .filter(|order| order.order_type() == OrderType::Market)
            .map(|order| order.client_order_id())
            .collect();
        let mut actions = HashSet::new();

        for receipt in &original.receipts {
            anyhow::ensure!(
                receipt.decision.account.id() == self.account_id
                    && receipt.account.id() == self.account_id,
                "foreign native maintenance receipt"
            );

            for order in &receipt.orders {
                anyhow::ensure!(
                    actions.insert(order.client_order_id()),
                    "duplicate original native maintenance action"
                );
                let cached = self
                    .cache
                    .borrow()
                    .try_order_owned(&order.client_order_id())?;
                anyhow::ensure!(
                    serde_json::to_value(order)? == serde_json::to_value(cached)?
                        && order.is_closed(),
                    "native maintenance original close mismatch"
                );
            }
        }

        if let Some(close) = &original.operator_close {
            let receipt = close
                .receipt
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("native operator close unresolved"))?;
            Self::validate_operator_close(close, receipt, &self.cache.borrow(), false)?;
            anyhow::ensure!(
                actions.insert(close.intent.client_order_id()),
                "native operator close duplicates liquidation"
            );
        }
        anyhow::ensure!(
            closes.iter().all(|id| actions.contains(id)),
            "original native close receipt missing"
        );
        self.quote = original.quote;
        self.state = original;
        Ok(())
    }

    /// Returns the last original quote admitted to the native matcher.
    pub fn quote(&self) -> Option<QuoteTick> {
        self.quote
    }

    /// Returns whether an original close still awaits native application.
    pub fn pending(&self) -> bool {
        self.state.pending.is_some()
    }

    /// Returns the number of original completed attempts awaiting prefix capture.
    pub fn completed(&self) -> usize {
        self.state.receipts.len()
    }

    /// Acknowledges capture of the exact complete original receipt prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if an action is unresolved or the supplied prefix differs.
    pub fn acknowledge(&mut self, prefix: usize) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state.pending.is_none()
                && self.state.uncertainty.is_none()
                && prefix == self.state.receipts.len(),
            "maintenance receipt acknowledgement mismatch"
        );
        self.state.acknowledged = prefix;
        Ok(())
    }

    /// Selects the original quote installed in the matcher for this native turn.
    ///
    /// # Errors
    ///
    /// Returns an error for a reordered original source cut.
    pub fn observe_quote(&mut self, quote: QuoteTick) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.quote
                .is_none_or(|old| quote.ts_init > old.ts_init && quote.ts_event >= old.ts_event),
            "reordered native maintenance source"
        );
        self.quote = Some(quote);
        self.state.deferral = None;
        Ok(())
    }

    fn decision(
        &self,
        now: UnixNanos,
        currency: Currency,
    ) -> anyhow::Result<Option<MaintenanceDecision>> {
        let cache = self.cache.borrow();
        let account = cache
            .account_owned(&self.account_id)
            .ok_or_else(|| anyhow::anyhow!("missing native maintenance account"))?;
        let AccountAny::Margin(margin) = &account else {
            anyhow::bail!("maintenance requires a native margin account")
        };
        let positions: Vec<_> = cache
            .positions_open(None, None, None, Some(&self.account_id), None)
            .into_iter()
            .filter(|p| p.settlement_currency == currency)
            .map(|p| p.cloned())
            .collect();
        let mut equity = margin
            .balance(Some(currency))
            .ok_or_else(|| anyhow::anyhow!("missing native settlement balance"))?
            .total;

        if positions.is_empty() {
            return Ok(None);
        }
        let mut quotes = Vec::new();

        for position in &positions {
            anyhow::ensure!(
                position.account_id == self.account_id,
                "foreign native maintenance position"
            );
            let quote = self
                .quote
                .filter(|q| q.instrument_id == position.instrument_id)
                .or_else(|| cache.quote(&position.instrument_id).copied())
                .ok_or_else(|| anyhow::anyhow!("missing native exit quote"))?;
            let price = match position.side {
                PositionSide::Long => quote.bid_price,
                PositionSide::Short => quote.ask_price,
                PositionSide::Flat => anyhow::bail!("flat open position"),
            };
            equity = Self::equity_at(equity, position, price)?;
            quotes.push(quote);
        }
        let maintenance = margin.total_maintenance_margin(currency);
        let threshold = maintenance
            .as_decimal()
            .checked_mul(self.ratio)
            .ok_or_else(|| anyhow::anyhow!("native maintenance threshold overflow"))?;
        let original_orders = cache
            .orders(None, None, None, Some(&self.account_id), None)
            .into_iter()
            .map(|order| (*order).clone())
            .collect();
        Ok(Some(MaintenanceDecision {
            account,
            positions,
            quotes,
            original_orders,
            currency,
            exit_equity: equity,
            maintenance,
            threshold,
            observed_ns: now,
        }))
    }

    fn equity_at(balance: Money, position: &Position, price: Price) -> anyhow::Result<Money> {
        anyhow::ensure!(
            !price.is_undefined()
                && !price.is_error()
                && price.precision != u8::MAX
                && price.raw() >= 0,
            "invalid native exit price"
        );
        let pnl = position.try_unrealized_pnl(price)?;
        anyhow::ensure!(
            balance.currency == pnl.currency,
            "native maintenance currency mismatch"
        );
        balance
            .checked_add(pnl)
            .ok_or_else(|| anyhow::anyhow!("native maintenance equity overflow"))
    }

    /// Evaluates the native account and dispatches original normal matcher closes at equality.
    ///
    /// Returns true only if this turn issued an action requiring FIFO application.
    ///
    /// # Errors
    ///
    /// Returns an error for unavailable valuation, unresolved originals, exhausted authority or dispatch.
    pub fn process<'a>(
        &mut self,
        now: UnixNanos,
        currency: Currency,
        cancel_orders: bool,
        matchers: impl IntoIterator<Item = &'a mut OrderMatchingEngine>,
    ) -> anyhow::Result<bool> {
        if !self.orders_applied() {
            self.fail("actual post-order native account application missing".into());
            anyhow::bail!("actual post-order native account application missing");
        }

        if !self.positions_applied() {
            self.fail("actual post-position native account application missing".into());
            anyhow::bail!("actual post-position native account application missing");
        }
        anyhow::ensure!(
            self.can_observe(),
            "maintenance original application or acknowledgement unresolved"
        );
        self.state.deferral = None;
        let Some(decision) = self.decision(now, currency)? else {
            return Ok(false);
        };

        if decision.maintenance.raw() == 0 || decision.exit_equity.as_decimal() > decision.threshold
        {
            return Ok(false);
        }
        anyhow::ensure!(
            self.state.receipts.len() < self.max_attempts,
            "native maintenance attempt budget exhausted"
        );

        if let Some(last) = self.state.receipts.last() {
            anyhow::ensure!(
                decision.quotes.iter().all(|q| last
                    .decision
                    .quotes
                    .iter()
                    .filter(|p| p.instrument_id == q.instrument_id)
                    .all(|p| q.ts_init > p.ts_init)),
                "maintenance requires a fresh executable source cut"
            );
        }

        if self.max_attempts == 2 {
            let order_events: usize = decision
                .original_orders
                .iter()
                .map(|order| order.events().len())
                .sum();
            let cancellations = decision
                .original_orders
                .iter()
                .filter(|order| order.is_open())
                .count();
            let mut reserved_orders = cancellations;
            let mut reserved_accounts = cancellations;

            for position in &decision.positions {
                let quantum = Decimal::new(1, u32::from(position.size_precision));
                let lots = position
                    .quantity
                    .as_decimal()
                    .checked_div(quantum)
                    .ok_or_else(|| anyhow::anyhow!("native fill headroom overflow"))?;
                anyhow::ensure!(
                    lots.fract().is_zero() && lots <= Decimal::from(256),
                    "native fill headroom exceeds frame bound"
                );
                let fills = usize::try_from(lots.normalize().mantissa())?;
                reserved_orders = reserved_orders
                    .checked_add(4 + fills)
                    .ok_or_else(|| anyhow::anyhow!("native event headroom overflow"))?;
                reserved_accounts = reserved_accounts
                    .checked_add(4 + 2 * fills)
                    .ok_or_else(|| anyhow::anyhow!("native account headroom overflow"))?;
            }
            anyhow::ensure!(
                decision.original_orders.len()
                    < 3 + usize::from(self.state.operator_close.is_some())
                    && order_events
                        .checked_add(reserved_orders)
                        .is_some_and(|events| events <= 256)
                    && decision
                        .account
                        .events()
                        .len()
                        .checked_add(reserved_accounts)
                        .is_some_and(|events| events <= 256),
                "native maintenance frame headroom exhausted"
            );
        }
        self.state.pending = Some((decision.clone(), Vec::new()));
        let result = (|| {
            let mut matchers: Vec<_> = matchers.into_iter().collect();

            for position in &decision.positions {
                anyhow::ensure!(
                    matchers
                        .iter()
                        .filter(|m| m.instrument.id() == position.instrument_id)
                        .count()
                        == 1,
                    "missing or duplicate native liquidation matcher"
                );
            }
            let mut ids = Vec::new();

            for matcher in &mut matchers {
                if decision
                    .positions
                    .iter()
                    .any(|p| p.instrument_id == matcher.instrument.id())
                {
                    ids.extend(matcher.liquidate_account_positions(
                        now,
                        cancel_orders,
                        currency,
                        self.account_id,
                    )?);
                }
            }
            Ok::<_, anyhow::Error>(ids)
        })();

        match result {
            Ok(ids) => {
                if ids.is_empty() {
                    self.state.pending = None;
                    self.defer("native executable exit book unavailable".into());
                    return Ok(false);
                }
                self.state
                    .pending
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("pending native maintenance decision missing"))?
                    .1 = ids;
                Ok(true)
            }
            Err(e) => {
                self.fail(e.to_string());
                Err(e)
            }
        }
    }

    /// Confirms the actual terminal orders and post-position native account behind the FIFO barrier.
    ///
    /// # Errors
    ///
    /// Returns an error while originals or the coherent account application are missing.
    pub fn complete(&mut self) -> anyhow::Result<()> {
        let Some((decision, ids)) = self.state.pending.clone() else {
            if !self.orders_applied() {
                self.fail("actual post-order native account application missing".into());
                anyhow::bail!("actual post-order native account application missing");
            }

            if !self.positions_applied() {
                self.fail("actual post-position native account application missing".into());
                anyhow::bail!("actual post-position native account application missing");
            }
            return Ok(());
        };
        let result = (|| {
            anyhow::ensure!(!ids.is_empty(), "native liquidation has no original close");
            let cache = self.cache.borrow();
            let orders: Vec<_> = ids
                .iter()
                .map(|id| cache.try_order_owned(id))
                .collect::<Result<Vec<_>, _>>()?;
            anyhow::ensure!(
                orders
                    .iter()
                    .all(|o| o.account_id() == Some(self.account_id) && o.is_closed()),
                "original native liquidation is not terminal"
            );
            let account = cache
                .account_owned(&self.account_id)
                .ok_or_else(|| anyhow::anyhow!("native liquidation account missing"))?;
            let mut filled = false;

            for order in &orders {
                let previous_qty = decision
                    .original_orders
                    .iter()
                    .find(|old| old.client_order_id() == order.client_order_id())
                    .map_or(
                        Quantity::zero(order.quantity().precision),
                        Order::filled_qty,
                    );

                if order.filled_qty() > previous_qty {
                    filled = true;
                    let position_id = cache
                        .position_id(&order.client_order_id())
                        .ok_or_else(|| anyhow::anyhow!("native close position linkage missing"))?;
                    let position = cache
                        .position(position_id)
                        .ok_or_else(|| anyhow::anyhow!("native close position missing"))?;
                    anyhow::ensure!(
                        cache.position_account_applied(&position, Some(&decision.account)),
                        "actual post-position native account application missing"
                    );
                }
            }

            for order in &orders {
                if let Some(previous) = decision
                    .original_orders
                    .iter()
                    .find(|old| old.client_order_id() == order.client_order_id() && old.is_open())
                    && order.filled_qty() == previous.filled_qty()
                {
                    anyhow::ensure!(
                        cache.order_account_applied(order, Some(&decision.account)),
                        "actual post-order native account application missing"
                    );
                }
            }
            let positions: Vec<Position> = cache
                .positions(None, None, None, Some(&self.account_id), None)
                .into_iter()
                .map(|p| p.cloned())
                .collect();
            let outcome = if !filled {
                MaintenanceOutcome::NoExecution
            } else if positions.iter().any(Position::is_open) {
                MaintenanceOutcome::Residual
            } else {
                MaintenanceOutcome::Flat
            };
            Ok(MaintenanceReceipt {
                outcome,
                decision,
                orders,
                account,
                positions,
            })
        })();

        match result {
            Ok(receipt) => {
                self.state.receipts.push(receipt);
                self.state.pending = None;
                Ok(())
            }
            Err(e) => {
                self.fail(e.to_string());
                Err(e)
            }
        }
    }

    /// Finds the tick boundary using the identical native rounded exit-equity predicate.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported multi-position/inverse bases, invalid metadata or arithmetic.
    pub fn threshold(
        &self,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<Option<MaintenanceThreshold>> {
        anyhow::ensure!(
            self.state.pending.is_none() && self.state.uncertainty.is_none(),
            "maintenance cut unresolved"
        );
        anyhow::ensure!(
            !instrument.is_inverse(),
            "inverse threshold basis unsupported"
        );
        anyhow::ensure!(
            instrument.tick_scheme().is_none(),
            "variable native tick scheme threshold unsupported"
        );
        let Some(decision) =
            self.decision(UnixNanos::default(), instrument.settlement_currency())?
        else {
            return Ok(None);
        };
        anyhow::ensure!(
            decision.positions.len() == 1 && decision.positions[0].instrument_id == instrument.id(),
            "threshold requires one native position"
        );

        if decision.maintenance.raw() == 0 {
            return Ok(None);
        }
        let position = &decision.positions[0];
        let balance = decision
            .account
            .balance_total(Some(decision.currency))
            .ok_or_else(|| anyhow::anyhow!("missing native settlement balance"))?;
        let tick = instrument.price_increment();
        anyhow::ensure!(
            !tick.is_undefined() && !tick.is_error() && tick.precision != u8::MAX && tick.raw() > 0,
            "unsupported native price increment"
        );
        let min_raw = instrument.min_price().map_or(0, |p| p.raw());
        let max_raw = instrument.max_price().map_or(PRICE_RAW_MAX, |p| p.raw());
        anyhow::ensure!(
            min_raw >= 0 && max_raw >= min_raw && max_raw <= PRICE_RAW_MAX,
            "invalid native threshold domain"
        );
        let mut low = min_raw
            .checked_add(tick.raw() - 1)
            .ok_or_else(|| anyhow::anyhow!("native threshold domain overflow"))?
            / tick.raw();
        let mut high = max_raw / tick.raw();
        anyhow::ensure!(low <= high, "empty native threshold domain");
        let price_at =
            |index| Price::from_raw_checked(index * tick.raw(), instrument.price_precision());
        let minimum = price_at(low)?;
        let maximum = price_at(high)?;
        let breached = |index| -> anyhow::Result<bool> {
            Ok(
                Self::equity_at(balance, position, price_at(index)?)?.as_decimal()
                    <= decision.threshold,
            )
        };
        let is_short = position.side == PositionSide::Short;
        let adverse = if is_short { high } else { low };
        let price = if breached(adverse)? {
            while low < high {
                let middle = if is_short {
                    low + (high - low) / 2
                } else {
                    low + (high - low + 1) / 2
                };

                if breached(middle)? {
                    if is_short {
                        high = middle;
                    } else {
                        low = middle;
                    }
                } else if is_short {
                    low = middle + 1;
                } else {
                    high = middle - 1;
                }
            }
            Some(price_at(low)?)
        } else {
            None
        };
        Ok(Some(MaintenanceThreshold {
            price,
            exit_side: position.side,
            account: decision.account,
            position: position.clone(),
            quote: decision.quotes[0],
            minimum,
            maximum,
        }))
    }
}
