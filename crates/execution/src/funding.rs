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

//! Shared native simulated funding lifecycle.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    rc::Rc,
};

use ahash::AHashMap;
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::Clock,
    msgbus::{self, switchboard},
};
use nautilus_core::{DurationNanos, UUID4, UnixNanos};
use nautilus_model::{
    accounts::AccountAny,
    data::{FundingRateUpdate, MarkPriceUpdate},
    enums::PositionAdjustmentType,
    events::{AccountState, FundingSettlement, PositionAdjusted, PositionEvent},
    identifiers::{AccountId, InstrumentId, Venue},
    position::Position,
    types::{AccountBalance, Currency, MarginBalance, Money, Price},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use ustr::Ustr;

/// Publishes native funding balance changes through the existing account owner.
pub trait FundingAccountPublisher {
    /// Returns the original account identity.
    fn account_id(&self) -> AccountId;
    /// Returns the account venue.
    fn venue(&self) -> Venue;
    /// Publishes the original native account event.
    ///
    /// # Errors
    ///
    /// Returns an error when account publication may be incomplete.
    fn publish(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()>;
}
impl<T: ExecutionClient + ?Sized> FundingAccountPublisher for T {
    fn account_id(&self) -> AccountId {
        ExecutionClient::account_id(self)
    }
    fn venue(&self) -> Venue {
        ExecutionClient::venue(self)
    }
    fn publish(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        self.generate_account_state(balances, margins, true, ts_event, None)
    }
}

/// Original native price input selected before the execution barrier.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FundingPriceCut {
    /// Original mark update.
    Mark(MarkPriceUpdate),
    /// Matching-engine bid and ask at the boundary cut.
    Midpoint {
        bid: Price,
        ask: Price,
        observed_ns: UnixNanos,
    },
    /// Price supplied by the historical exchange's existing price selector.
    Replay(Price),
}

impl FundingPriceCut {
    /// Returns the exact native settlement price.
    #[must_use]
    pub fn price(&self) -> Option<Price> {
        match self {
            Self::Mark(mark) => Some(mark.value),
            Self::Midpoint { bid, ask, .. } => {
                let value = bid
                    .as_decimal()
                    .checked_add(ask.as_decimal())?
                    .checked_div(Decimal::TWO)?;
                Price::from_decimal(value).ok()
            }
            Self::Replay(price) => Some(*price),
        }
    }
}

/// Native progress at a single funding boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FundingProgress {
    /// Original account publication is verified in the native cache.
    Applied,
    /// Legacy replay publication completed without a cache application acknowledgment.
    Published,
    /// The original boundary had no open positions.
    Empty,
    /// No financial change occurred; a required input is unavailable.
    Deferred,
    /// A mutation may have happened; further settlement is prohibited.
    Uncertain,
}

/// Immutable execution limits for a finite native funding simulation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingWindow {
    /// Original first settlement selected for this simulation.
    pub first_boundary: UnixNanos,
    /// Exclusive cutoff for new matching and later settlements.
    pub deadline: UnixNanos,
}

/// A coherent native funding cut, restorable only against its original native state.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingCheckpoint {
    version: u32,
    venue: Venue,
    frozen_account: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    window: Option<FundingWindow>,
    account: AccountAny,
    positions: Vec<Position>,
    pending: Vec<FundingRateUpdate>,
    receipts: Vec<FundingReceipt>,
}

impl FundingCheckpoint {
    /// Returns the original native account identity.
    #[must_use]
    pub fn account_id(&self) -> AccountId {
        self.account.id()
    }

    /// Returns immutable native execution limits, if this is a finite simulation.
    #[must_use]
    pub const fn window(&self) -> Option<FundingWindow> {
        self.window
    }

    /// Returns original pending native observations.
    #[must_use]
    pub fn pending(&self) -> &[FundingRateUpdate] {
        &self.pending
    }

    /// Returns original consumed boundary receipts.
    #[must_use]
    pub fn receipts(&self) -> &[FundingReceipt] {
        &self.receipts
    }
}

/// Retains original native results for a consumed settlement boundary.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingReceipt {
    /// Preserves the original selected rate update.
    pub original_rate: FundingRateUpdate,
    /// Identifies the consumed settlement clock.
    pub boundary: UnixNanos,
    /// Original selected price input, absent for an empty boundary.
    pub price_cut: Option<FundingPriceCut>,
    /// Distinguishes verified application from legacy publication.
    pub progress: FundingProgress,
    /// Preserves the native settlement, absent for a boundary without positions.
    pub settlement: Option<FundingSettlement>,
    /// Preserves original native position adjustments.
    pub adjustments: Vec<PositionAdjusted>,
    /// Preserves observed original account events.
    pub account_events: Vec<AccountState>,
}

/// Owns pending and consumed simulated funding boundaries.
pub struct FundingOwner {
    venue: Venue,
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
    frozen_account: bool,
    window: Option<FundingWindow>,
    pending: BTreeMap<(UnixNanos, InstrumentId), FundingRateUpdate>,
    applied: BTreeMap<(UnixNanos, InstrumentId), FundingReceipt>,
    poisoned: Option<String>,
    sealed: BTreeSet<(UnixNanos, InstrumentId)>,
}
impl Debug for FundingOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(FundingOwner))
            .field("venue", &self.venue)
            .field("pending", &self.pending.len())
            .field("applied", &self.applied.len())
            .field("poisoned", &self.poisoned)
            .finish()
    }
}
impl FundingOwner {
    /// Creates the native venue funding owner.
    pub fn new(
        venue: Venue,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
        frozen_account: bool,
    ) -> Self {
        Self {
            venue,
            clock,
            cache,
            frozen_account,
            window: None,
            pending: BTreeMap::new(),
            applied: BTreeMap::new(),
            poisoned: None,
            sealed: BTreeSet::new(),
        }
    }
    /// Binds immutable finite execution limits before any lifecycle state is installed.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid limits or an already initialized owner.
    pub fn bind_window(&mut self, window: Option<FundingWindow>) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.pending.is_empty()
                && self.applied.is_empty()
                && self.sealed.is_empty()
                && self.poisoned.is_none(),
            "funding window requires an uninitialized owner"
        );
        anyhow::ensure!(
            window.is_none_or(|window| !window.first_boundary.is_zero()
                && window.first_boundary < window.deadline),
            "invalid native funding window"
        );
        self.window = window;
        Ok(())
    }

    /// Returns the bound immutable execution limits.
    #[must_use]
    pub const fn window(&self) -> Option<FundingWindow> {
        self.window
    }

    /// Checks a matching ingress against original pending input and native clock limits.
    #[must_use]
    pub fn matching_ready(
        &self,
        instrument: InstrumentId,
        now: UnixNanos,
        event: Option<UnixNanos>,
    ) -> bool {
        if self.blocks_execution(now) {
            return false;
        }
        let Some(((boundary, _), rate)) =
            self.pending.iter().find(|((_, id), _)| *id == instrument)
        else {
            return false;
        };
        event.is_none_or(|event| event < *boundary)
            && self.window.is_none_or(|window| {
                now < window.deadline
                    && event.is_none_or(|event| event < window.deadline)
                    && Self::observation_fresh(rate.ts_event, rate.ts_init, now)
            })
    }

    fn observation_fresh(ts_event: UnixNanos, ts_init: UnixNanos, cut: UnixNanos) -> bool {
        ts_event <= cut
            && ts_init <= cut
            && cut.as_u64().saturating_sub(ts_event.as_u64()) <= 5_000_000_000
            && cut.as_u64().saturating_sub(ts_init.as_u64()) <= 5_000_000_000
    }

    /// Queues an original rate update without repeating a consumed boundary.
    pub fn queue(&mut self, update: FundingRateUpdate) -> Option<UnixNanos> {
        let boundary = Self::boundary(&update)?;
        if !self.applied.contains_key(&(boundary, update.instrument_id)) {
            self.pending
                .insert((boundary, update.instrument_id), update);
        }
        Some(boundary)
    }
    /// Returns pending keys due at the supplied native clock.
    pub fn due(&self, now: UnixNanos) -> Vec<(UnixNanos, InstrumentId)> {
        self.pending
            .keys()
            .copied()
            .take_while(|(boundary, _)| *boundary <= now)
            .collect()
    }
    /// Returns the next pending boundary.
    pub fn next_boundary(&self) -> Option<UnixNanos> {
        self.pending
            .first_key_value()
            .map(|((boundary, _), _)| *boundary)
    }
    /// Returns the original pending rate for a key.
    pub fn pending_rate(
        &self,
        boundary: UnixNanos,
        instrument: InstrumentId,
    ) -> Option<FundingRateUpdate> {
        self.pending.get(&(boundary, instrument)).copied()
    }
    /// Settles a queued replay boundary through the original native account publisher.
    pub fn process_replay<P: FundingAccountPublisher + ?Sized>(
        &mut self,
        instrument: InstrumentId,
        boundary: UnixNanos,
        price: Option<Price>,
        publisher: Option<&P>,
        require_application: bool,
    ) -> bool {
        let Some(rate) = self.pending.get(&(boundary, instrument)).copied() else {
            return self.poisoned.is_none()
                && (!require_application
                    || self
                        .applied
                        .get(&(boundary, instrument))
                        .is_some_and(|receipt| {
                            matches!(
                                receipt.progress,
                                FundingProgress::Applied | FundingProgress::Empty
                            )
                        }));
        };

        if self.settle_replay(
            &rate,
            boundary,
            price.map(FundingPriceCut::Replay),
            publisher,
            require_application,
        ) {
            self.pending.remove(&(boundary, instrument));
            true
        } else {
            false
        }
    }
    /// Clears native lifecycle state during an explicit simulation reset.
    pub fn reset(&mut self) {
        self.pending.clear();
        self.applied.clear();
        self.sealed.clear();
        self.poisoned = None;
    }
    /// Resolves the original explicit or interval-aligned native boundary.
    pub fn boundary(rate: &FundingRateUpdate) -> Option<UnixNanos> {
        rate.next_funding_ns.or_else(|| {
            let interval = DurationNanos::try_from_mins(u64::from(rate.interval?)).ok()?;
            (!interval.is_zero() && rate.ts_event.floor(interval) == rate.ts_event)
                .then_some(rate.ts_event)
        })
    }
    fn settle_replay<P: FundingAccountPublisher + ?Sized>(
        &mut self,
        funding_rate: &FundingRateUpdate,
        ts_event: UnixNanos,
        price_cut: Option<FundingPriceCut>,
        publisher: Option<&P>,
        require_application: bool,
    ) -> bool {
        let settlement_key = (ts_event, funding_rate.instrument_id);
        if let Some(receipt) = self.applied.get(&settlement_key) {
            return !require_application
                || matches!(
                    receipt.progress,
                    FundingProgress::Applied | FundingProgress::Empty
                );
        }

        if self.poisoned.is_some() {
            return false;
        }
        let Some(exec_client) = publisher else {
            log::warn!(
                "Cannot settle funding for {}: execution client is not registered",
                funding_rate.instrument_id
            );
            return false;
        };
        let account_id = exec_client.account_id();
        let account_venue = exec_client.venue();

        if self
            .cache
            .borrow()
            .instrument(&funding_rate.instrument_id)
            .is_none()
            || account_venue != self.venue
        {
            return false;
        }

        if require_application
            && self
                .cache
                .borrow()
                .account_for_venue(&self.venue)
                .is_none_or(|account| account.id() != account_id)
        {
            return false;
        }

        let open_positions: Vec<Position> = {
            let cache = self.cache.borrow();
            cache
                .positions_open(
                    Some(&self.venue),
                    Some(&funding_rate.instrument_id),
                    None,
                    Some(&account_id),
                    None,
                )
                .into_iter()
                .map(|position| position.cloned())
                .collect()
        };

        if open_positions.is_empty() {
            self.applied.insert(
                settlement_key,
                FundingReceipt {
                    original_rate: *funding_rate,
                    boundary: ts_event,
                    price_cut: None,
                    progress: FundingProgress::Empty,
                    settlement: None,
                    adjustments: Vec::new(),
                    account_events: Vec::new(),
                },
            );
            return true;
        }

        let Some(settlement_price) = price_cut.as_ref().and_then(FundingPriceCut::price) else {
            log::warn!(
                "Cannot settle funding for {}: no mark price or top-of-book price",
                funding_rate.instrument_id
            );
            return false;
        };

        let settlement_currency = open_positions[0].settlement_currency;
        let mut valued_positions = Vec::with_capacity(open_positions.len());
        let mut account_adjustments: AHashMap<Currency, Money> = AHashMap::new();

        for position in open_positions {
            if position.settlement_currency != settlement_currency {
                log::error!(
                    "Cannot settle funding for {}: position settlement currencies differ",
                    funding_rate.instrument_id
                );
                return false;
            }

            let notional = match position.try_notional_value(settlement_price) {
                Ok(notional) => notional,
                Err(e) => {
                    log::error!(
                        "Cannot settle funding for position {}: invalid notional value: {e}",
                        position.id
                    );
                    return false;
                }
            };
            let side = if position.signed_qty > 0.0 {
                -Decimal::ONE
            } else {
                Decimal::ONE
            };
            let Some(amount) = notional
                .as_decimal()
                .checked_mul(funding_rate.rate)
                .and_then(|value| value.checked_mul(side))
            else {
                log::error!(
                    "Cannot settle funding for position {}: funding amount overflow",
                    position.id
                );
                return false;
            };
            let pnl_change = match Money::from_decimal(amount, notional.currency) {
                Ok(money) => money,
                Err(e) => {
                    log::error!(
                        "Cannot settle funding for position {}: invalid funding amount: {e}",
                        position.id
                    );
                    return false;
                }
            };

            if pnl_change.currency != settlement_currency {
                log::error!(
                    "Cannot settle funding for position {}: settlement currency {} differs from funding currency {}",
                    position.id,
                    settlement_currency,
                    pnl_change.currency
                );
                return false;
            }

            if let Some(realized) = position.realized_pnl {
                if realized.currency != pnl_change.currency {
                    log::error!(
                        "Cannot settle funding for position {}: realized PnL currency {} differs from funding currency {}",
                        position.id,
                        realized.currency,
                        pnl_change.currency
                    );
                    return false;
                }

                if realized.checked_add(pnl_change).is_none() {
                    log::error!(
                        "Cannot settle funding for position {}: realized PnL overflow",
                        position.id
                    );
                    return false;
                }
            }
            let total_adjustment =
                if let Some(current) = account_adjustments.get(&pnl_change.currency).copied() {
                    let Some(total) = current.checked_add(pnl_change) else {
                        log::error!(
                            "Cannot settle funding for {}: aggregate account adjustment overflow",
                            funding_rate.instrument_id
                        );
                        return false;
                    };
                    total
                } else {
                    pnl_change
                };
            account_adjustments.insert(pnl_change.currency, total_adjustment);
            valued_positions.push((position, pnl_change));
        }

        let mut account_adjustments = account_adjustments.into_values().collect::<Vec<_>>();
        account_adjustments.sort_unstable_by_key(|adjustment| adjustment.currency.code);

        if !self.frozen_account {
            let cache = self.cache.borrow();
            let Some(account) = cache.account_for_venue(&account_venue) else {
                log::error!("Cannot settle funding: no account for venue {account_venue}");
                return false;
            };

            if account.id() != account_id {
                return false;
            }

            for adjustment in &account_adjustments {
                let Some(balance) = account.balance(Some(adjustment.currency)) else {
                    log::error!(
                        "Cannot settle funding: no account balance for currency {}",
                        adjustment.currency
                    );
                    return false;
                };

                if balance.total.checked_add(*adjustment).is_none()
                    || balance.free.checked_add(*adjustment).is_none()
                {
                    log::error!(
                        "Cannot settle funding: {} account adjustment exceeds Money bounds",
                        adjustment.currency
                    );
                    return false;
                }
            }
        }

        let ts_init = self.clock.borrow().timestamp_ns();
        let settlement = FundingSettlement::new(
            msgbus::get_message_bus().borrow().trader_id,
            funding_rate.instrument_id,
            account_id,
            funding_rate.rate,
            settlement_price,
            settlement_currency,
            UUID4::new(),
            ts_event,
            ts_init,
        );
        let mut adjusted_positions = Vec::with_capacity(valued_positions.len());
        for (original, pnl_change) in valued_positions {
            let mut adjusted = original.clone();
            let adjustment = PositionAdjusted::new(
                settlement.trader_id,
                adjusted.strategy_id,
                adjusted.instrument_id,
                adjusted.id,
                adjusted.account_id,
                PositionAdjustmentType::Funding,
                None,
                Some(pnl_change),
                Some(Ustr::from(&format!(
                    "funding_settlement:{}",
                    settlement.event_id
                ))),
                UUID4::new(),
                settlement.ts_event,
                settlement.ts_init,
            );
            adjusted.apply_adjustment(adjustment);
            adjusted_positions.push((original, adjusted, adjustment));
        }

        if require_application {
            self.poisoned = Some("funding_mutation_in_progress".into());
        }
        {
            let mut cache = self.cache.borrow_mut();

            for (index, (_, adjusted, _)) in adjusted_positions.iter().enumerate() {
                if let Err(e) = cache.update_position(adjusted) {
                    log::error!(
                        "Cannot update position {} after funding settlement: {e}",
                        adjusted.id
                    );

                    // Inclusive of `index`: the failed update commits the adjusted position
                    // to the cache before attempting to persist it, so the position whose
                    // update returned the error also needs restoring.
                    let mut rollback_verified = true;

                    for (original, _, _) in adjusted_positions[..=index].iter().rev() {
                        if let Err(rollback_error) = cache.update_position(original) {
                            log::error!(
                                "Cannot roll back position {} after failed funding settlement: {rollback_error}",
                                original.id
                            );
                            rollback_verified = false;

                            if require_application {
                                self.poisoned = Some("funding_position_rollback_uncertain".into());
                            }
                        }
                    }

                    if require_application && rollback_verified {
                        self.poisoned = None;
                    }
                    return false;
                }
            }
        }

        let account_event_count = self
            .cache
            .borrow()
            .account_for_venue(&account_venue)
            .map(|a| a.events().len())
            .unwrap_or_default();
        let mut verified = !self.frozen_account;

        for adjustment in &account_adjustments {
            let Some(applied) = self.publish_adjustment(*adjustment, exec_client, ts_event) else {
                self.poisoned = Some("funding_account_publication_uncertain".into());
                return false;
            };
            verified &= applied;
        }

        if require_application && !verified {
            self.poisoned = Some("funding_account_application_unconfirmed".into());
            return false;
        }

        self.applied.insert(
            settlement_key,
            FundingReceipt {
                original_rate: *funding_rate,
                boundary: ts_event,
                price_cut,
                progress: if verified {
                    FundingProgress::Applied
                } else {
                    FundingProgress::Published
                },
                settlement: Some(settlement),
                adjustments: adjusted_positions
                    .iter()
                    .map(|(_, _, adjustment)| *adjustment)
                    .collect(),
                account_events: self
                    .cache
                    .borrow()
                    .account_for_venue(&account_venue)
                    .map(|a| a.events().into_iter().skip(account_event_count).collect())
                    .unwrap_or_default(),
            },
        );
        let settlement_topic = switchboard::get_funding_settlement_topic(settlement.instrument_id);
        msgbus::publish_any(settlement_topic, &settlement);

        for (_, _, adjustment) in adjusted_positions {
            let event = PositionEvent::PositionAdjusted(adjustment);
            let PositionEvent::PositionAdjusted(adjustment) = &event else {
                continue;
            };
            let topic = switchboard::get_event_position_topic(adjustment.strategy_id);
            msgbus::publish_position_event(topic, &event);
        }

        if require_application {
            self.poisoned = None;
        }
        true
    }

    fn publish_adjustment<P: FundingAccountPublisher + ?Sized>(
        &self,
        adjustment: Money,
        publisher: &P,
        ts_event: UnixNanos,
    ) -> Option<bool> {
        if self.frozen_account {
            return Some(false);
        }
        let (balances, margins, prior_events) = {
            let cache = self.cache.borrow();
            let account = cache.account_for_venue(&publisher.venue())?;
            if account.id() != publisher.account_id() {
                return None;
            }
            let mut balance = *account.balance(Some(adjustment.currency))?;
            balance.total = balance.total.checked_add(adjustment)?;
            balance.free = balance.free.checked_add(adjustment)?;
            let margins = match &*account {
                AccountAny::Margin(account) => {
                    let mut margins: Vec<_> = account.margins.values().copied().collect();
                    margins.extend(account.account_margins.values().copied());
                    margins
                }
                _ => Vec::new(),
            };
            (vec![balance], margins, account.events().len())
        };
        publisher
            .publish(balances.clone(), margins.clone(), ts_event)
            .ok()?;
        let cache = self.cache.borrow();
        let account = cache.account_for_venue(&publisher.venue())?;
        let Some(event) = account.last_event() else {
            return Some(false);
        };
        Some(
            account.id() == publisher.account_id()
                && account.events().len() == prior_events + 1
                && event.account_id == publisher.account_id()
                && event.ts_event == ts_event
                && event.balances == balances
                && event.margins == margins
                && balances
                    .iter()
                    .all(|balance| account.balance(Some(balance.currency)) == Some(balance)),
        )
    }

    /// Queues a live update, rejecting changed content after the boundary has been sealed.
    ///
    /// # Errors
    ///
    /// Returns an error for missing clocks, foreign instruments, or conflicting consumed input.
    pub fn queue_live(&mut self, update: FundingRateUpdate) -> anyhow::Result<Option<UnixNanos>> {
        let now = self.clock.borrow().timestamp_ns();
        anyhow::ensure!(
            update.ts_event <= now && update.ts_init <= now,
            "future funding observation"
        );
        anyhow::ensure!(self.poisoned.is_none(), "native funding owner is uncertain");
        anyhow::ensure!(
            update.instrument_id.venue == self.venue,
            "foreign funding venue"
        );
        let boundary = Self::boundary(&update)
            .ok_or_else(|| anyhow::anyhow!("funding boundary unavailable"))?;
        let key = (boundary, update.instrument_id);
        if let Some(window) = self.window {
            if boundary < window.first_boundary {
                self.poisoned = Some("funding_window_schedule_changed".into());
                anyhow::bail!("funding boundary precedes the immutable first settlement");
            }
            let consumed = self
                .applied
                .contains_key(&(window.first_boundary, update.instrument_id));
            if !consumed
                && boundary != window.first_boundary
                && (update.ts_event < window.first_boundary
                    || !self
                        .pending
                        .contains_key(&(window.first_boundary, update.instrument_id)))
            {
                self.poisoned = Some("funding_window_schedule_changed".into());
                anyhow::bail!("original first native funding boundary changed");
            }

            if boundary == window.first_boundary
                && now >= boundary
                && (update.ts_event > boundary || update.ts_init > boundary)
                && let Some(original) = self.pending.get(&key)
            {
                if original.rate != update.rate || original.interval != update.interval {
                    self.poisoned = Some("funding_consumed_input_conflict".into());
                    anyhow::bail!("late first-period funding input changed");
                }
                return Ok(None);
            }
        }
        let original = self
            .applied
            .get(&key)
            .map(|receipt| &receipt.original_rate)
            .or_else(|| {
                self.sealed
                    .contains(&key)
                    .then(|| self.pending.get(&key))
                    .flatten()
            });

        if let Some(original) = original {
            if original.instrument_id != update.instrument_id
                || original.rate != update.rate
                || original.interval != update.interval
                || original.next_funding_ns != update.next_funding_ns
            {
                self.poisoned = Some("funding_consumed_input_conflict".into());
                anyhow::bail!("changed sealed or consumed funding input");
            }
            return Ok(None);
        }
        self.pending.insert(key, update);
        Ok(Some(boundary))
    }

    /// Seals all due input identities before a FIFO execution-channel barrier is queued.
    pub fn seal_due(&mut self, now: UnixNanos) -> Vec<(UnixNanos, InstrumentId)> {
        let due: Vec<_> = self
            .due(now)
            .into_iter()
            .filter(|(boundary, _)| {
                self.window.is_none_or(|window| {
                    *boundary >= window.first_boundary && *boundary < window.deadline
                })
            })
            .collect();
        self.sealed.extend(due.iter().copied());
        due
    }

    /// Applies a sealed live boundary after earlier execution events have propagated.
    #[must_use]
    pub fn process_live<P: FundingAccountPublisher + ?Sized>(
        &mut self,
        instrument: InstrumentId,
        boundary: UnixNanos,
        price: Option<FundingPriceCut>,
        publisher: &P,
    ) -> FundingProgress {
        if self.poisoned.is_some() {
            return FundingProgress::Uncertain;
        }
        let key = (boundary, instrument);
        if let Some(receipt) = self.applied.get(&key) {
            return receipt.progress;
        }
        let Some(rate) = self.pending.get(&key).copied() else {
            return FundingProgress::Deferred;
        };

        if self.window.is_some_and(|window| {
            boundary < window.first_boundary
                || boundary >= window.deadline
                || !Self::observation_fresh(rate.ts_event, rate.ts_init, boundary)
        }) {
            return FundingProgress::Deferred;
        }

        if self.window.is_some()
            && !matches!(&price, Some(FundingPriceCut::Mark(mark)) if Self::observation_fresh(mark.ts_event, mark.ts_init, boundary))
        {
            return FundingProgress::Deferred;
        }

        if !self.sealed.contains(&key) || self.clock.borrow().timestamp_ns() < boundary {
            return FundingProgress::Deferred;
        }

        if self
            .cache
            .borrow()
            .account_for_venue(&self.venue)
            .is_none_or(|account| account.id() != publisher.account_id())
        {
            return FundingProgress::Deferred;
        }

        if price.as_ref().is_some_and(|cut| match cut {
            FundingPriceCut::Mark(mark) => {
                mark.instrument_id != instrument
                    || mark.ts_event > boundary
                    || mark.ts_init > self.clock.borrow().timestamp_ns()
            }
            FundingPriceCut::Midpoint {
                bid,
                ask,
                observed_ns,
            } => bid > ask || *observed_ns > self.clock.borrow().timestamp_ns(),
            FundingPriceCut::Replay(_) => true,
        }) {
            return FundingProgress::Deferred;
        }

        if self.settle_replay(&rate, boundary, price, Some(publisher), true) {
            self.pending.remove(&key);
            self.sealed.remove(&key);
            self.applied
                .get(&key)
                .map_or(FundingProgress::Deferred, |receipt| receipt.progress)
        } else if self.poisoned.is_some() {
            FundingProgress::Uncertain
        } else {
            FundingProgress::Deferred
        }
    }

    /// Releases an unexecuted barrier when its native runner is stopped on the owner thread.
    pub fn cancel_barrier(&mut self) {
        self.sealed.clear();
    }

    /// Returns whether matching must wait for a due, sealed or uncertain boundary.
    #[must_use]
    pub fn blocks_execution(&self, now: UnixNanos) -> bool {
        self.poisoned.is_some()
            || !self.sealed.is_empty()
            || self.next_boundary().is_some_and(|boundary| boundary <= now)
    }

    /// Captures complete pending and consumed identities against current native financial state.
    ///
    /// # Errors
    ///
    /// Returns an error when a boundary is in flight, application is unverified, or the account is absent.
    pub fn checkpoint(&self, account_id: AccountId) -> anyhow::Result<FundingCheckpoint> {
        anyhow::ensure!(
            self.poisoned.is_none() && self.sealed.is_empty(),
            "funding checkpoint has an unresolved boundary"
        );
        anyhow::ensure!(
            self.applied.values().all(|receipt| matches!(
                receipt.progress,
                FundingProgress::Applied | FundingProgress::Empty
            )),
            "funding publication has no application acknowledgment"
        );
        let cache = self.cache.borrow();
        let account = cache
            .account_owned(&account_id)
            .ok_or_else(|| anyhow::anyhow!("native funding account unavailable"))?;
        anyhow::ensure!(
            account_id.get_issuer() == self.venue,
            "foreign funding account"
        );
        let mut positions: Vec<_> = cache
            .positions(Some(&self.venue), None, None, Some(&account_id), None)
            .into_iter()
            .map(|position| position.cloned())
            .collect();
        positions.sort_by_key(|position| position.id);
        self.validate_adjustment_coverage(&positions)?;
        Ok(FundingCheckpoint {
            version: 1,
            venue: self.venue,
            frozen_account: self.frozen_account,
            window: self.window,
            account,
            positions,
            pending: self.pending.values().copied().collect(),
            receipts: self.applied.values().cloned().collect(),
        })
    }

    /// Restores the lifecycle without publishing, charging, matching, or arming a timer.
    ///
    /// # Errors
    ///
    /// Returns an error for a foreign, incomplete or changed native state cut.
    pub fn restore(
        &mut self,
        checkpoint: FundingCheckpoint,
        account_id: AccountId,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.pending.is_empty()
                && self.applied.is_empty()
                && self.sealed.is_empty()
                && self.poisoned.is_none(),
            "funding owner already initialized"
        );
        anyhow::ensure!(
            checkpoint.version == 1
                && checkpoint.venue == self.venue
                && checkpoint.frozen_account == self.frozen_account
                && checkpoint.window == self.window
                && checkpoint.account.id() == account_id,
            "foreign funding checkpoint"
        );
        let cache = self.cache.borrow();
        let account = cache
            .account_owned(&account_id)
            .ok_or_else(|| anyhow::anyhow!("original funding account unavailable"))?;
        let mut positions: Vec<_> = cache
            .positions(Some(&self.venue), None, None, Some(&account_id), None)
            .into_iter()
            .map(|position| position.cloned())
            .collect();
        positions.sort_by_key(|position| position.id);
        anyhow::ensure!(
            serde_json::to_value(&account)? == serde_json::to_value(&checkpoint.account)?
                && serde_json::to_value(&positions)?
                    == serde_json::to_value(&checkpoint.positions)?,
            "native funding checkpoint state mismatch"
        );
        drop(cache);
        let mut pending = BTreeMap::new();
        let mut applied = BTreeMap::new();

        for update in checkpoint.pending {
            let boundary = Self::boundary(&update)
                .ok_or_else(|| anyhow::anyhow!("checkpoint funding boundary unavailable"))?;
            anyhow::ensure!(
                update.instrument_id.venue == self.venue
                    && pending
                        .insert((boundary, update.instrument_id), update)
                        .is_none(),
                "invalid pending funding identity"
            );
        }

        for receipt in checkpoint.receipts {
            let key = (receipt.boundary, receipt.original_rate.instrument_id);
            anyhow::ensure!(
                key.1.venue == self.venue
                    && Self::boundary(&receipt.original_rate) == Some(key.0)
                    && !pending.contains_key(&key),
                "invalid consumed funding identity"
            );

            if let Some(window) = self.window {
                anyhow::ensure!(
                    key.0 >= window.first_boundary
                        && key.0 < window.deadline
                        && Self::observation_fresh(
                            receipt.original_rate.ts_event,
                            receipt.original_rate.ts_init,
                            key.0
                        ),
                    "consumed funding lies outside its original supported window"
                );

                if receipt.progress == FundingProgress::Applied {
                    anyhow::ensure!(
                        matches!(&receipt.price_cut, Some(FundingPriceCut::Mark(mark)) if mark.instrument_id == key.1 && Self::observation_fresh(mark.ts_event, mark.ts_init, key.0)),
                        "bounded funding requires its original fresh mark"
                    );
                }
            }

            match receipt.progress {
                FundingProgress::Empty => anyhow::ensure!(
                    receipt.settlement.is_none()
                        && receipt.adjustments.is_empty()
                        && receipt.account_events.is_empty()
                        && receipt.price_cut.is_none(),
                    "invalid empty funding receipt"
                ),
                FundingProgress::Applied => {
                    let settlement = receipt
                        .settlement
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("funding settlement missing"))?;
                    anyhow::ensure!(
                        settlement.account_id == account_id
                            && settlement.instrument_id == key.1
                            && settlement.ts_event == key.0
                            && settlement.rate == receipt.original_rate.rate
                            && receipt.price_cut.as_ref().and_then(FundingPriceCut::price)
                                == Some(settlement.settlement_price)
                            && !receipt.adjustments.is_empty()
                            && !receipt.account_events.is_empty(),
                        "invalid applied funding receipt"
                    );

                    for event in &receipt.account_events {
                        anyhow::ensure!(
                            event.account_id == account_id
                                && event.ts_event == key.0
                                && checkpoint.account.events().iter().any(|original| original
                                    .event_id
                                    == event.event_id
                                    && serde_json::to_value(original).ok()
                                        == serde_json::to_value(event).ok()),
                            "funding account event missing from original state"
                        );
                    }

                    for adjustment in &receipt.adjustments {
                        anyhow::ensure!(
                            adjustment.adjustment_type == PositionAdjustmentType::Funding
                                && adjustment.account_id == account_id
                                && adjustment.instrument_id == key.1
                                && adjustment.ts_event == key.0
                                && adjustment.reason
                                    == Some(Ustr::from(&format!(
                                        "funding_settlement:{}",
                                        settlement.event_id
                                    )))
                                && checkpoint
                                    .positions
                                    .iter()
                                    .any(|position| position.id == adjustment.position_id
                                        && position.adjustments.contains(adjustment)),
                            "funding adjustment missing from original position"
                        );
                    }
                }
                _ => anyhow::bail!("unverified funding checkpoint"),
            }
            anyhow::ensure!(
                applied.insert(key, receipt).is_none(),
                "duplicate consumed funding identity"
            );
        }

        for position in &checkpoint.positions {
            for adjustment in position
                .adjustments
                .iter()
                .filter(|adjustment| adjustment.adjustment_type == PositionAdjustmentType::Funding)
            {
                anyhow::ensure!(
                    applied
                        .values()
                        .any(|receipt| receipt.adjustments.contains(adjustment)),
                    "original funding adjustment has no consumed boundary receipt"
                );
            }
        }

        if let Some(window) = self.window {
            anyhow::ensure!(
                pending
                    .keys()
                    .chain(applied.keys())
                    .all(|(boundary, instrument)| *boundary >= window.first_boundary
                        && (*boundary == window.first_boundary
                            || applied.contains_key(&(window.first_boundary, *instrument))
                            || pending.contains_key(&(window.first_boundary, *instrument)))),
                "original first funding boundary is missing"
            );
        }
        self.pending = pending;
        self.applied = applied;
        Ok(())
    }
    fn validate_adjustment_coverage(&self, positions: &[Position]) -> anyhow::Result<()> {
        for position in positions {
            for adjustment in position
                .adjustments
                .iter()
                .filter(|adjustment| adjustment.adjustment_type == PositionAdjustmentType::Funding)
            {
                anyhow::ensure!(
                    self.applied
                        .values()
                        .any(|receipt| receipt.adjustments.contains(adjustment)),
                    "native funding adjustment has no consumed boundary receipt"
                );
            }
        }
        Ok(())
    }
}
