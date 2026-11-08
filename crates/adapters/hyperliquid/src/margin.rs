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

use nautilus_model::{
    accounts::margin_model::{MarginAccounting, MarginModel},
    enums::InstrumentClass,
    instruments::Instrument,
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::http::models::{MarginTable, MarginTier};

#[derive(Clone, Copy, Debug, Default)]
pub struct HyperliquidMarginModel;

impl HyperliquidMarginModel {
    fn tiers(instrument: &dyn Instrument) -> anyhow::Result<Vec<MarginTier>> {
        anyhow::ensure!(
            instrument.id().venue.as_str() == "HYPERLIQUID"
                && !instrument.is_inverse()
                && instrument.instrument_class() == InstrumentClass::Swap,
            "Hyperliquid linear perpetual margin terms required"
        );
        let info = instrument
            .info()
            .ok_or_else(|| anyhow::anyhow!("Missing margin metadata"))?;
        let id = info
            .get("marginTableId")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("Missing margin table association"))?;
        let tiers = if id > 0 && id < 50 {
            vec![MarginTier {
                lower_bound: Decimal::ZERO,
                max_leverage: id as u32,
            }]
        } else {
            let table: MarginTable = serde_json::from_value(
                info.get("marginTable")
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Missing associated margin table"))?,
            )?;
            table.margin_tiers
        };
        anyhow::ensure!(
            !tiers.is_empty() && tiers[0].lower_bound == Decimal::ZERO,
            "Margin tiers must begin at zero"
        );

        for (index, tier) in tiers.iter().enumerate() {
            anyhow::ensure!(tier.max_leverage > 0, "Margin leverage must be positive");
            if index > 0 {
                anyhow::ensure!(
                    tier.lower_bound > tiers[index - 1].lower_bound
                        && tier.max_leverage <= tiers[index - 1].max_leverage,
                    "Margin tiers must have increasing bounds and nonincreasing leverage"
                );
            }
        }
        Ok(tiers)
    }

    fn notional(
        instrument: &dyn Instrument,
        quantity: Quantity,
        price: Price,
        use_quote_for_inverse: Option<bool>,
    ) -> anyhow::Result<Money> {
        anyhow::ensure!(
            price.as_decimal() > Decimal::ZERO,
            "Margin price must be positive"
        );
        instrument.try_calculate_notional_value(quantity, price, use_quote_for_inverse)
    }
}

impl MarginModel for HyperliquidMarginModel {
    fn accounting(&self) -> MarginAccounting {
        MarginAccounting::CurrentMark
    }

    fn name(&self) -> &'static str {
        "hyperliquid_tiered"
    }

    fn calculate_initial_margin(
        &self,
        instrument: &dyn Instrument,
        quantity: Quantity,
        price: Price,
        leverage: Decimal,
        use_quote_for_inverse: Option<bool>,
    ) -> anyhow::Result<Money> {
        let tiers = Self::tiers(instrument)?;
        let notional = Self::notional(instrument, quantity, price, use_quote_for_inverse)?;
        let value = notional.as_decimal().abs();
        let tier = tiers
            .iter()
            .rev()
            .find(|tier| value >= tier.lower_bound)
            .unwrap();
        anyhow::ensure!(
            leverage >= Decimal::ONE
                && leverage.fract().is_zero()
                && leverage <= Decimal::from(tier.max_leverage),
            "Leverage exceeds the current notional tier"
        );
        let initial = value
            .checked_div(leverage)
            .ok_or_else(|| anyhow::anyhow!("Initial margin overflow"))?;
        Money::from_decimal(initial, notional.currency).map_err(Into::into)
    }

    fn calculate_reserved_initial_margin(
        &self,
        instrument: &dyn Instrument,
        quantity: Quantity,
        price: Price,
        leverage: Decimal,
        use_quote_for_inverse: Option<bool>,
    ) -> anyhow::Result<Money> {
        Self::tiers(instrument)?;
        anyhow::ensure!(
            leverage >= Decimal::ONE && leverage.fract().is_zero(),
            "Leverage must be an integer of at least one"
        );
        let notional = Self::notional(instrument, quantity, price, use_quote_for_inverse)?;
        let initial = notional
            .as_decimal()
            .abs()
            .checked_div(leverage)
            .ok_or_else(|| anyhow::anyhow!("Initial margin overflow"))?;
        Money::from_decimal(initial, notional.currency).map_err(Into::into)
    }

    fn calculate_maintenance_margin(
        &self,
        instrument: &dyn Instrument,
        quantity: Quantity,
        price: Price,
        _leverage: Decimal,
        use_quote_for_inverse: Option<bool>,
    ) -> anyhow::Result<Money> {
        let tiers = Self::tiers(instrument)?;
        let notional = Self::notional(instrument, quantity, price, use_quote_for_inverse)?;
        let value = notional.as_decimal().abs();
        let mut maintenance = Decimal::ZERO;

        for (index, tier) in tiers.iter().enumerate() {
            if value <= tier.lower_bound {
                break;
            }
            let upper = tiers
                .get(index + 1)
                .map_or(value, |next| value.min(next.lower_bound));
            let amount = upper
                .checked_sub(tier.lower_bound)
                .and_then(|span| span.checked_div(Decimal::from(tier.max_leverage) * Decimal::TWO))
                .ok_or_else(|| anyhow::anyhow!("Maintenance margin overflow"))?;
            maintenance = maintenance
                .checked_add(amount)
                .ok_or_else(|| anyhow::anyhow!("Maintenance margin overflow"))?;
        }
        Money::from_decimal(maintenance, notional.currency).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };

    use nautilus_common::{
        cache::Cache,
        clock::{Clock, TestClock},
        messages::execution::{SubmitOrder, TradingCommand},
        msgbus::{self, MessageBus, MessagingSwitchboard, TypedIntoHandler},
    };
    use nautilus_core::UUID4;
    use nautilus_model::{
        accounts::{
            Account, AccountAny, MarginAccount,
            margin_model::{LeveragedMarginModel, MarginModelHandle},
        },
        data::MarkPriceUpdate,
        enums::{AccountType, LiquiditySide, OmsType, OrderSide, OrderType, TimeInForce},
        events::{
            AccountState, OrderEventAny, PositionClosed, PositionEvent, PositionOpened,
            order::spec::{
                OrderAcceptedSpec, OrderCanceledSpec, OrderFilledSpec, OrderSubmittedSpec,
            },
        },
        identifiers::{AccountId, ClientOrderId, PositionId, TradeId, VenueOrderId},
        instruments::InstrumentAny,
        orders::{Order, OrderTestBuilder},
        position::Position,
        types::{AccountBalance, Currency, MarginBalance},
    };
    use nautilus_portfolio::{Portfolio, config::PortfolioConfig, manager::AccountsManager};
    use nautilus_risk::engine::{RiskEngine, config::RiskEngineConfig};
    use rstest::rstest;

    use super::*;
    use crate::http::{
        models::PerpMeta,
        parse::{create_instrument_from_def, parse_perp_instruments},
    };

    fn btc() -> InstrumentAny {
        let meta: PerpMeta = serde_json::from_value(serde_json::json!({
            "universe":[{"name":"BTC","szDecimals":5,"maxLeverage":40,"marginTableId":56}],
            "marginTables":[[56,{"description":"BTC","marginTiers":[
                {"lowerBound":"0","maxLeverage":40},
                {"lowerBound":"150000000","maxLeverage":20}]}]]
        }))
        .unwrap();
        let definitions = parse_perp_instruments(&meta, 0).unwrap();
        create_instrument_from_def(&definitions[0], 0.into()).unwrap()
    }

    #[rstest]
    #[case("2499.99999", "60000", "1874999.99250000 USDC")]
    #[case("2500", "60000", "1875000.00000000 USDC")]
    #[case("2500.00001", "60000", "1875000.01500000 USDC")]
    #[case("3000", "60000", "2625000.00000000 USDC")]
    #[case("0.001", "61000", "0.76250000 USDC")]
    fn maintenance_is_tiered_and_independent_of_selected_leverage(
        #[case] quantity: &str,
        #[case] price: &str,
        #[case] expected: &str,
    ) {
        let instrument = btc();

        for leverage in [Decimal::ONE, Decimal::TEN, Decimal::from(20)] {
            assert_eq!(
                HyperliquidMarginModel
                    .calculate_maintenance_margin(
                        &instrument,
                        Quantity::from(quantity),
                        Price::from(price),
                        leverage,
                        None
                    )
                    .unwrap()
                    .to_string(),
                expected
            );
        }
    }

    #[rstest]
    fn initial_uses_current_mark_user_leverage_and_original_metadata() {
        let instrument = btc();
        assert_eq!(instrument.quote_currency().code.as_str(), "USDT");
        assert_eq!(instrument.info().unwrap()["marginTableId"], 56);
        assert_eq!(
            HyperliquidMarginModel
                .calculate_initial_margin(
                    &instrument,
                    Quantity::from("0.001"),
                    Price::from("61000"),
                    Decimal::TEN,
                    None
                )
                .unwrap()
                .to_string(),
            "6.10000000 USDC"
        );
        assert!(
            HyperliquidMarginModel
                .calculate_initial_margin(
                    &instrument,
                    Quantity::from("3000"),
                    Price::from("60000"),
                    Decimal::from(40),
                    None
                )
                .is_err()
        );
        assert_eq!(
            instrument
                .try_calculate_notional_value(Quantity::from("0.00025"), Price::from("60001"), None)
                .unwrap()
                .to_string(),
            "15.00025000 USDC"
        );
    }
    #[rstest]
    fn missing_referenced_table_refuses_instead_of_assuming_a_single_tier() {
        let mut instrument = btc();
        let InstrumentAny::CryptoPerpetual(ref mut perp) = instrument else {
            unreachable!()
        };
        perp.info.as_mut().unwrap().shift_remove("marginTable");
        assert!(
            HyperliquidMarginModel
                .calculate_maintenance_margin(
                    &instrument,
                    Quantity::from("3000"),
                    Price::from("60000"),
                    Decimal::ONE,
                    None
                )
                .is_err()
        );
    }
    #[rstest]
    #[case("0.5")]
    #[case("1.5")]
    fn initial_requires_integer_leverage_at_least_one(#[case] leverage: &str) {
        let instrument = btc();
        assert!(
            HyperliquidMarginModel
                .calculate_initial_margin(
                    &instrument,
                    Quantity::from("0.001"),
                    Price::from("61000"),
                    leverage.parse().unwrap(),
                    None
                )
                .is_err()
        );
    }

    fn paper_account() -> MarginAccount {
        let total = Money::from("1000 USDC");
        let mut account = MarginAccount::new(
            AccountState::new(
                AccountId::new("HYPERLIQUID-001"),
                AccountType::Margin,
                vec![AccountBalance::new(
                    total,
                    Money::zero(Currency::USDC()),
                    total,
                )],
                vec![],
                true,
                UUID4::new(),
                0.into(),
                0.into(),
                None,
            ),
            true,
        );
        account.set_default_leverage(Decimal::TEN);
        account
            .set_margin_model(MarginModelHandle::new(HyperliquidMarginModel))
            .unwrap();
        account
    }

    #[rstest]
    fn pinned_restore_preserves_financial_state_and_refuses_fallback() {
        let mut account = paper_account();
        account.update_margin(MarginBalance::new(
            Money::from("6.1 USDC"),
            Money::from("0.7625 USDC"),
            Some(btc().id()),
        ));
        let saved = serde_json::to_value(&account).unwrap();
        let mut restored: MarginAccount = serde_json::from_value(saved.clone()).unwrap();
        assert_eq!(serde_json::to_value(&restored).unwrap(), saved);
        assert!(!restored.is_margin_model_ready());
        assert!(
            restored
                .calculate_initial_margin(
                    &btc(),
                    Quantity::from("0.001"),
                    Price::from("61000"),
                    None
                )
                .is_err()
        );
        assert!(
            restored
                .set_margin_model(MarginModelHandle::new(LeveragedMarginModel))
                .is_err()
        );
        restored
            .set_margin_model(MarginModelHandle::new(HyperliquidMarginModel))
            .unwrap();
        assert!(restored.is_margin_model_ready());
        assert_eq!(serde_json::to_value(&restored).unwrap(), saved);
        assert_eq!(
            restored.base.balances[&Currency::USDC()].locked,
            Money::from("6.1 USDC")
        );
    }

    #[rstest]
    fn portfolio_refreshes_current_mark_reserves_and_records_position_applications() {
        *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
        let instrument = btc();
        let account = paper_account();
        let account_id = account.id();
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        cache
            .borrow_mut()
            .add_account(AccountAny::Margin(account))
            .unwrap();
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let mut portfolio = Portfolio::new(
            clock,
            Rc::clone(&cache),
            Some(
                PortfolioConfig::builder()
                    .use_mark_prices(false)
                    .build()
                    .unwrap(),
            ),
        );
        let publish_mark = |price: &str| {
            let mark =
                MarkPriceUpdate::new(instrument.id(), Price::from(price), 0.into(), 0.into());
            cache.borrow_mut().add_mark_price(mark).unwrap();
            msgbus::publish_mark_price(
                format!("data.mark_prices.{}", instrument.id()).into(),
                &mark,
            );
        };
        let assert_reserve = |initial: &str, maintenance: &str, cash: &str| {
            let cached = cache.borrow();
            let account = cached.account(&account_id).unwrap();
            let AccountAny::Margin(account) = &*account else {
                panic!("Margin account required")
            };
            assert_eq!(
                account.total_initial_margin(Currency::USDC()),
                Money::from(initial)
            );
            assert_eq!(
                account.total_maintenance_margin(Currency::USDC()),
                Money::from(maintenance)
            );
            let balance = account.balance(Some(Currency::USDC())).unwrap();
            assert_eq!(balance.total, Money::from(cash));
            assert_eq!(balance.locked, Money::from(initial));
            assert_eq!(balance.free, balance.total - balance.locked);
        };
        publish_mark("61000");
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument.id())
            .client_order_id(ClientOrderId::new("OPEN"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.001"))
            .price(Price::from("60000"))
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        cache
            .borrow_mut()
            .update_order(&OrderEventAny::Submitted(
                OrderSubmittedSpec::builder()
                    .instrument_id(instrument.id())
                    .client_order_id(order.client_order_id())
                    .account_id(account_id)
                    .build(),
            ))
            .unwrap();
        let accepted = OrderEventAny::Accepted(
            OrderAcceptedSpec::builder()
                .instrument_id(instrument.id())
                .client_order_id(order.client_order_id())
                .account_id(account_id)
                .venue_order_id(VenueOrderId::new("OPEN"))
                .build(),
        );
        cache.borrow_mut().update_order(&accepted).unwrap();
        assert_eq!(
            cache
                .borrow()
                .orders_open(None, Some(&instrument.id()), None, Some(&account_id), None)
                .len(),
            1
        );
        portfolio.update_order(&accepted);
        assert_reserve("6.1 USDC", "0 USDC", "1000 USDC");

        let fill = OrderFilledSpec::builder()
            .instrument_id(instrument.id())
            .client_order_id(order.client_order_id())
            .venue_order_id(VenueOrderId::new("OPEN"))
            .account_id(account_id)
            .position_id(PositionId::new("POSITION"))
            .trade_id(TradeId::new("PARTIAL"))
            .order_side(OrderSide::Buy)
            .last_qty(Quantity::from("0.0004"))
            .last_px(Price::from("60000"))
            .currency(Currency::USDC())
            .commission(Money::from("0.012 USDC"))
            .build();
        let fill_event = OrderEventAny::Filled(fill.clone());
        cache.borrow_mut().update_order(&fill_event).unwrap();
        portfolio.update_order(&fill_event);
        let mut position = Position::new(&instrument, fill.clone());
        cache
            .borrow_mut()
            .add_position(&position, OmsType::Netting)
            .unwrap();
        portfolio.update_position(&PositionEvent::PositionOpened(PositionOpened::create(
            &position,
            &fill,
            UUID4::new(),
            0.into(),
        )));
        assert_reserve("6.1 USDC", "0.305 USDC", "999.988 USDC");
        assert!(cache.borrow().position_account_applied(&position, None));

        publish_mark("59000");
        assert_reserve("5.9 USDC", "0.295 USDC", "999.988 USDC");
        publish_mark("61000");
        let canceled = OrderEventAny::Canceled(
            OrderCanceledSpec::builder()
                .instrument_id(instrument.id())
                .client_order_id(order.client_order_id())
                .account_id(account_id)
                .venue_order_id(VenueOrderId::new("OPEN"))
                .build(),
        );
        cache.borrow_mut().update_order(&canceled).unwrap();
        portfolio.update_order(&canceled);
        assert_reserve("2.44 USDC", "0.305 USDC", "999.988 USDC");

        let closing_order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .client_order_id(ClientOrderId::new("CLOSE"))
            .side(OrderSide::Sell)
            .quantity(Quantity::from("0.0004"))
            .reduce_only(true)
            .time_in_force(TimeInForce::Ioc)
            .build();
        cache
            .borrow_mut()
            .add_order(closing_order, Some(position.id), None, false)
            .unwrap();
        cache
            .borrow_mut()
            .update_order(&OrderEventAny::Submitted(
                OrderSubmittedSpec::builder()
                    .instrument_id(instrument.id())
                    .client_order_id(ClientOrderId::new("CLOSE"))
                    .account_id(account_id)
                    .build(),
            ))
            .unwrap();
        let closing_accepted = OrderEventAny::Accepted(
            OrderAcceptedSpec::builder()
                .instrument_id(instrument.id())
                .client_order_id(ClientOrderId::new("CLOSE"))
                .account_id(account_id)
                .venue_order_id(VenueOrderId::new("CLOSE"))
                .build(),
        );
        cache.borrow_mut().update_order(&closing_accepted).unwrap();
        portfolio.update_order(&closing_accepted);
        assert_reserve("2.44 USDC", "0.305 USDC", "999.988 USDC");

        let close = OrderFilledSpec::builder()
            .instrument_id(instrument.id())
            .client_order_id(ClientOrderId::new("CLOSE"))
            .venue_order_id(VenueOrderId::new("CLOSE"))
            .account_id(account_id)
            .position_id(position.id)
            .trade_id(TradeId::new("CLOSE"))
            .order_side(OrderSide::Sell)
            .last_qty(Quantity::from("0.0004"))
            .last_px(Price::from("61000"))
            .currency(Currency::USDC())
            .commission(Money::from("0.0122 USDC"))
            .build();
        cache
            .borrow_mut()
            .update_order(&OrderEventAny::Filled(close.clone()))
            .unwrap();
        portfolio.update_order(&OrderEventAny::Filled(close.clone()));
        position.apply(&close);
        cache.borrow_mut().update_position(&position).unwrap();
        portfolio.update_position(&PositionEvent::PositionClosed(PositionClosed::create(
            &position,
            &close,
            UUID4::new(),
            0.into(),
        )));
        assert_reserve("0 USDC", "0 USDC", "1000.3758 USDC");
        assert!(cache.borrow().position_account_applied(&position, None));
    }

    #[rstest]
    #[case::missing_mark(None, None, false)]
    #[case::future_event(Some("61000"), Some((1, 0)), false)]
    #[case::future_receipt(Some("61000"), Some((0, 1)), false)]
    #[case::nonpositive_mark(Some("0"), Some((0, 0)), false)]
    #[case::foreign_collateral(Some("61000"), Some((0, 0)), true)]
    fn current_mark_calculation_refuses_unknown_timing_and_fx(
        #[case] price: Option<&str>,
        #[case] timestamps: Option<(u64, u64)>,
        #[case] foreign_collateral: bool,
    ) {
        let instrument = btc();
        let mut account = paper_account();
        if foreign_collateral {
            account.base.base_currency = Some(Currency::USD());
        }
        let fill = OrderFilledSpec::builder()
            .instrument_id(instrument.id())
            .account_id(account.base.id)
            .position_id(PositionId::new("POSITION"))
            .order_side(OrderSide::Buy)
            .last_qty(Quantity::from("0.001"))
            .last_px(Price::from("60000"))
            .currency(Currency::USDC())
            .build();
        let position = Position::new(&instrument, fill);
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        if let (Some(price), Some((event, receipt))) = (price, timestamps) {
            cache
                .borrow_mut()
                .add_mark_price(MarkPriceUpdate::new(
                    instrument.id(),
                    Price::from(price),
                    event.into(),
                    receipt.into(),
                ))
                .unwrap();
        }
        let manager = AccountsManager::new(Rc::new(RefCell::new(TestClock::new())), cache);
        let before = serde_json::to_value(&account).unwrap();
        assert!(
            manager
                .update_positions(&account, &instrument, vec![&position], 0.into())
                .is_none()
        );
        assert_eq!(serde_json::to_value(&account).unwrap(), before);
    }

    #[rstest]
    fn explicit_tier_zero_simulation_fees_flow_through_native_portfolio() {
        *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
        let original = btc();
        let original_definition = serde_json::to_value(&original).unwrap();
        assert_eq!(original.maker_fee(), Decimal::ZERO);
        assert_eq!(original.taker_fee(), Decimal::ZERO);
        let mut simulation = original.clone();
        let InstrumentAny::CryptoPerpetual(ref mut perp) = simulation else {
            unreachable!()
        };
        perp.maker_fee = Decimal::new(15, 5);
        perp.taker_fee = Decimal::new(45, 5);
        assert_eq!(simulation.info(), original.info());
        assert_eq!(simulation.quote_currency(), Currency::USDT());
        assert_eq!(simulation.cost_currency(), Currency::USDC());
        let account = paper_account();
        let account_id = account.id();
        let quantity = Quantity::from("0.001");
        let entry_fee = account
            .calculate_commission(
                &simulation,
                quantity,
                Price::from("60000"),
                LiquiditySide::Taker,
                None,
            )
            .unwrap();
        let exit_fee = account
            .calculate_commission(
                &simulation,
                quantity,
                Price::from("61000"),
                LiquiditySide::Taker,
                None,
            )
            .unwrap();
        assert_eq!(entry_fee, Money::from("0.027 USDC"));
        assert_eq!(exit_fee, Money::from("0.02745 USDC"));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        cache
            .borrow_mut()
            .add_instrument(simulation.clone())
            .unwrap();
        cache
            .borrow_mut()
            .add_account(AccountAny::Margin(account))
            .unwrap();
        cache
            .borrow_mut()
            .add_mark_price(MarkPriceUpdate::new(
                simulation.id(),
                Price::from("61000"),
                0.into(),
                0.into(),
            ))
            .unwrap();
        let mut portfolio = Portfolio::new(
            Rc::new(RefCell::new(TestClock::new())),
            Rc::clone(&cache),
            None,
        );
        let entry = OrderFilledSpec::builder()
            .instrument_id(simulation.id())
            .client_order_id(ClientOrderId::new("TIER0-OPEN"))
            .account_id(account_id)
            .position_id(PositionId::new("TIER0-POSITION"))
            .trade_id(TradeId::new("TIER0-OPEN"))
            .order_side(OrderSide::Buy)
            .last_qty(quantity)
            .last_px(Price::from("60000"))
            .currency(Currency::USDC())
            .commission(entry_fee)
            .build();
        portfolio.update_order(&OrderEventAny::Filled(entry.clone()));
        let mut position = Position::new(&simulation, entry.clone());
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        portfolio.update_position(&PositionEvent::PositionOpened(PositionOpened::create(
            &position,
            &entry,
            UUID4::new(),
            0.into(),
        )));
        {
            let cached = cache.borrow();
            let account = cached.account(&account_id).unwrap();
            let AccountAny::Margin(account) = &*account else {
                panic!("Margin account required")
            };
            assert_eq!(
                account.balance_total(Some(Currency::USDC())),
                Some(Money::from("999.973 USDC"))
            );
            assert_eq!(
                account.total_initial_margin(Currency::USDC()),
                Money::from("6.1 USDC")
            );
            assert_eq!(
                account.total_maintenance_margin(Currency::USDC()),
                Money::from("0.7625 USDC")
            );
        }
        let exit = OrderFilledSpec::builder()
            .instrument_id(simulation.id())
            .client_order_id(ClientOrderId::new("TIER0-CLOSE"))
            .account_id(account_id)
            .position_id(position.id)
            .trade_id(TradeId::new("TIER0-CLOSE"))
            .order_side(OrderSide::Sell)
            .last_qty(quantity)
            .last_px(Price::from("61000"))
            .currency(Currency::USDC())
            .commission(exit_fee)
            .build();
        portfolio.update_order(&OrderEventAny::Filled(exit.clone()));
        position.apply(&exit);
        cache.borrow_mut().update_position(&position).unwrap();
        portfolio.update_position(&PositionEvent::PositionClosed(PositionClosed::create(
            &position,
            &exit,
            UUID4::new(),
            0.into(),
        )));
        let cached = cache.borrow();
        let account = cached.account(&account_id).unwrap();
        let AccountAny::Margin(account) = &*account else {
            panic!("Margin account required")
        };
        let balance = account.balance(Some(Currency::USDC())).unwrap();
        assert_eq!(balance.total, Money::from("1000.94555 USDC"));
        assert_eq!(balance.free, balance.total);
        assert_eq!(balance.locked, Money::zero(Currency::USDC()));
        assert_eq!(
            account.total_initial_margin(Currency::USDC()),
            Money::zero(Currency::USDC())
        );
        assert_eq!(
            account.total_maintenance_margin(Currency::USDC()),
            Money::zero(Currency::USDC())
        );
        assert!(cached.position_account_applied(&position, None));
        assert_eq!(
            serde_json::to_value(&original).unwrap(),
            original_definition
        );
    }

    #[rstest]
    #[case("6 USDC", false, false)]
    #[case("6.02699999 USDC", false, false)]
    #[case("6.027 USDC", true, false)]
    #[case("10000000 USDC", true, true)]
    fn native_risk_budgets_explicit_tier_zero_commission(
        #[case] cash: &str,
        #[case] admitted: bool,
        #[case] reducing_tier_crossing: bool,
    ) {
        *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
        let mut instrument = btc();
        let InstrumentAny::CryptoPerpetual(ref mut perp) = instrument else {
            unreachable!()
        };
        perp.maker_fee = Decimal::new(15, 5);
        perp.taker_fee = Decimal::new(45, 5);
        let mut account = paper_account();
        if reducing_tier_crossing {
            account.set_default_leverage(Decimal::from(40));
        }
        let account_id = account.id();
        let amount = Money::from(cash);
        account.base.balances.insert(
            Currency::USDC(),
            AccountBalance::new(amount, Money::zero(Currency::USDC()), amount),
        );

        if !reducing_tier_crossing {
            assert_eq!(
                account
                    .calculate_initial_margin(
                        &instrument,
                        Quantity::from("0.001"),
                        Price::from("60000"),
                        None
                    )
                    .unwrap(),
                Money::from("6 USDC")
            );
        }
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        cache
            .borrow_mut()
            .add_account(AccountAny::Margin(account))
            .unwrap();
        cache
            .borrow_mut()
            .add_mark_price(MarkPriceUpdate::new(
                instrument.id(),
                Price::from("60000"),
                0.into(),
                0.into(),
            ))
            .unwrap();

        if reducing_tier_crossing {
            let fill = OrderFilledSpec::builder()
                .instrument_id(instrument.id())
                .account_id(account_id)
                .position_id(PositionId::new("TIER-EXIT"))
                .order_side(OrderSide::Buy)
                .last_qty(Quantity::from("3000"))
                .last_px(Price::from("49999"))
                .currency(Currency::USDC())
                .build();
            let position = Position::new(&instrument, fill);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
        let portfolio = Portfolio::new(Rc::clone(&clock), Rc::clone(&cache), None);
        let accepted = Rc::new(Cell::new(0usize));
        let denied = Rc::new(Cell::new(0usize));
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_queue_execute(),
            TypedIntoHandler::from({
                let accepted = Rc::clone(&accepted);
                move |_: TradingCommand| accepted.set(accepted.get() + 1)
            }),
        );
        msgbus::register_order_event_endpoint(
            MessagingSwitchboard::exec_engine_process(),
            TypedIntoHandler::from({
                let denied = Rc::clone(&denied);
                move |event: OrderEventAny| {
                    assert!(matches!(event, OrderEventAny::Denied(_)));
                    denied.set(denied.get() + 1);
                }
            }),
        );
        let mut risk = RiskEngine::new(
            RiskEngineConfig::default(),
            portfolio,
            Rc::clone(&clock),
            Rc::clone(&cache),
        );
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument.id())
            .side(if reducing_tier_crossing {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            })
            .reduce_only(reducing_tier_crossing)
            .time_in_force(TimeInForce::Ioc)
            .quantity(Quantity::from(if reducing_tier_crossing {
                "3000"
            } else {
                "0.001"
            }))
            .price(Price::from("60000"))
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        risk.execute(TradingCommand::SubmitOrder(SubmitOrder::new(
            order.trader_id(),
            None,
            order.strategy_id(),
            instrument.id(),
            order.client_order_id(),
            order.init_event().clone(),
            None,
            None,
            None,
            UUID4::new(),
            0.into(),
            None,
        )));
        assert_eq!(accepted.get(), usize::from(admitted));
        assert_eq!(denied.get(), usize::from(!admitted));
    }

    #[rstest]
    fn two_markets_share_native_admission_margin_and_independent_exits() {
        *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
        let meta: PerpMeta = serde_json::from_value(serde_json::json!({
            "universe":[
                {"name":"BTC","szDecimals":5,"maxLeverage":40,"marginTableId":56},
                {"name":"ETH","szDecimals":4,"maxLeverage":20,"marginTableId":55}
            ],
            "marginTables":[
                [56,{"description":"BTC","marginTiers":[{"lowerBound":"0","maxLeverage":40}]}],
                [55,{"description":"ETH","marginTiers":[{"lowerBound":"0","maxLeverage":20}]}]
            ]
        }))
        .unwrap();
        let definitions = parse_perp_instruments(&meta, 0).unwrap();
        let instruments: Vec<_> = definitions
            .iter()
            .map(|definition| {
                let mut instrument = create_instrument_from_def(definition, 0.into()).unwrap();
                let InstrumentAny::CryptoPerpetual(ref mut perp) = instrument else {
                    unreachable!()
                };
                perp.maker_fee = Decimal::new(15, 5);
                perp.taker_fee = Decimal::new(45, 5);
                instrument
            })
            .collect();
        assert_eq!(instruments[0].id().symbol.as_str(), "BTC-USD-PERP");
        assert_eq!(instruments[1].id().symbol.as_str(), "ETH-USD-PERP");
        let mut account = paper_account();
        account.set_leverage(instruments[1].id(), Decimal::from(20));
        let account_id = account.id();
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_account(AccountAny::Margin(account))
            .unwrap();

        for (instrument, price) in instruments.iter().zip(["60000", "3000"]) {
            cache
                .borrow_mut()
                .add_instrument(instrument.clone())
                .unwrap();
            cache
                .borrow_mut()
                .add_mark_price(MarkPriceUpdate::new(
                    instrument.id(),
                    Price::from(price),
                    0.into(),
                    0.into(),
                ))
                .unwrap();
        }
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
        let portfolio = Portfolio::new(
            Rc::clone(&clock),
            Rc::clone(&cache),
            Some(
                PortfolioConfig::builder()
                    .use_mark_prices(true)
                    .build()
                    .unwrap(),
            ),
        );
        let admitted = Rc::new(Cell::new(0usize));
        let denied = Rc::new(Cell::new(0usize));
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_queue_execute(),
            TypedIntoHandler::from({
                let admitted = Rc::clone(&admitted);
                move |_: TradingCommand| admitted.set(admitted.get() + 1)
            }),
        );
        msgbus::register_order_event_endpoint(
            MessagingSwitchboard::exec_engine_process(),
            TypedIntoHandler::from({
                let denied = Rc::clone(&denied);
                move |event: OrderEventAny| {
                    assert!(matches!(event, OrderEventAny::Denied(_)));
                    denied.set(denied.get() + 1);
                }
            }),
        );
        let mut risk = RiskEngine::new(
            RiskEngineConfig::default(),
            portfolio,
            Rc::clone(&clock),
            Rc::clone(&cache),
        );
        let mut positions = [None::<Position>, None];

        for (step, (index, closing, price, cash, initial, maintenance)) in [
            (0, false, "60000", "999.973 USDC", "6 USDC", "0.75 USDC"),
            (1, false, "3000", "999.946 USDC", "9 USDC", "2.25 USDC"),
            (0, true, "61000", "1000.91855 USDC", "3 USDC", "1.5 USDC"),
            (1, true, "2900", "998.89245 USDC", "0 USDC", "0 USDC"),
        ]
        .into_iter()
        .enumerate()
        {
            let instrument = &instruments[index];
            let quantity = Quantity::from(if index == 0 { "0.001" } else { "0.02" });
            let price = Price::from(price);
            let mark = MarkPriceUpdate::new(instrument.id(), price, 0.into(), 0.into());
            cache.borrow_mut().add_mark_price(mark).unwrap();
            msgbus::publish_mark_price(
                format!("data.mark_prices.{}", instrument.id()).into(),
                &mark,
            );
            let id = ClientOrderId::new(format!("MULTI-{step}"));
            let order = OrderTestBuilder::new(OrderType::Limit)
                .instrument_id(instrument.id())
                .client_order_id(id)
                .side(if closing {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                })
                .reduce_only(closing)
                .time_in_force(TimeInForce::Ioc)
                .quantity(quantity)
                .price(price)
                .build();
            cache
                .borrow_mut()
                .add_order(order.clone(), None, None, false)
                .unwrap();
            risk.execute(TradingCommand::SubmitOrder(SubmitOrder::new(
                order.trader_id(),
                None,
                order.strategy_id(),
                instrument.id(),
                id,
                order.init_event().clone(),
                None,
                None,
                None,
                UUID4::new(),
                0.into(),
                None,
            )));
            assert_eq!(admitted.get(), step + 1, "native admission at step {step}");
            assert_eq!(denied.get(), 0);
            cache
                .borrow_mut()
                .update_order(&OrderEventAny::Submitted(
                    OrderSubmittedSpec::builder()
                        .instrument_id(instrument.id())
                        .client_order_id(id)
                        .account_id(account_id)
                        .build(),
                ))
                .unwrap();
            let accepted = OrderEventAny::Accepted(
                OrderAcceptedSpec::builder()
                    .instrument_id(instrument.id())
                    .client_order_id(id)
                    .account_id(account_id)
                    .venue_order_id(VenueOrderId::new(id.as_str()))
                    .build(),
            );
            cache.borrow_mut().update_order(&accepted).unwrap();
            risk.portfolio_mut().update_order(&accepted);
            let fee = cache
                .borrow()
                .account(&account_id)
                .unwrap()
                .calculate_commission(instrument, quantity, price, LiquiditySide::Taker, None)
                .unwrap();
            let fill = OrderFilledSpec::builder()
                .instrument_id(instrument.id())
                .client_order_id(id)
                .venue_order_id(VenueOrderId::new(id.as_str()))
                .account_id(account_id)
                .position_id(PositionId::new(format!("MULTI-POSITION-{index}")))
                .trade_id(TradeId::new(id.as_str()))
                .order_side(order.order_side())
                .last_qty(quantity)
                .last_px(price)
                .currency(Currency::USDC())
                .commission(fee)
                .build();
            cache
                .borrow_mut()
                .update_order(&OrderEventAny::Filled(fill.clone()))
                .unwrap();
            risk.portfolio_mut()
                .update_order(&OrderEventAny::Filled(fill.clone()));

            if closing {
                let position = positions[index].as_mut().unwrap();
                position.apply(&fill);
                cache.borrow_mut().update_position(position).unwrap();
                risk.portfolio_mut()
                    .update_position(&PositionEvent::PositionClosed(PositionClosed::create(
                        position,
                        &fill,
                        UUID4::new(),
                        0.into(),
                    )));
            } else {
                let position = Position::new(instrument, fill.clone());
                cache
                    .borrow_mut()
                    .add_position_without_order(&position, OmsType::Netting)
                    .unwrap();
                risk.portfolio_mut()
                    .update_position(&PositionEvent::PositionOpened(PositionOpened::create(
                        &position,
                        &fill,
                        UUID4::new(),
                        0.into(),
                    )));
                positions[index] = Some(position);
            }
            let cached = cache.borrow();
            let account = cached.account(&account_id).unwrap();
            let AccountAny::Margin(account) = &*account else {
                unreachable!()
            };
            let balance = account.balance(Some(Currency::USDC())).unwrap();
            assert_eq!(balance.total, Money::from(cash));
            assert_eq!(balance.locked, Money::from(initial));
            assert_eq!(balance.free, balance.total - balance.locked);
            assert_eq!(
                account.total_initial_margin(Currency::USDC()),
                Money::from(initial)
            );
            assert_eq!(
                account.total_maintenance_margin(Currency::USDC()),
                Money::from(maintenance)
            );
            assert_eq!(
                cached
                    .positions_open(None, None, None, Some(&account_id), None)
                    .len(),
                [1, 2, 1, 0][step]
            );
        }
        assert!(
            positions
                .iter()
                .all(|position| position.as_ref().unwrap().is_closed())
        );
        let realized =
            risk.portfolio_mut()
                .realized_pnls(&instruments[0].id().venue, Some(&account_id), None);
        assert_eq!(
            realized.unwrap()[&Currency::USDC()],
            Money::from("-1.10755 USDC")
        );
    }

    #[rstest]
    fn mark_crossing_a_tier_refreshes_existing_position_requirements() {
        *msgbus::get_message_bus().borrow_mut() = MessageBus::default();
        let instrument = btc();
        let mut account = paper_account();
        account.set_default_leverage(Decimal::from(40));
        let total = Money::from("10000000 USDC");
        account.base.balances.insert(
            Currency::USDC(),
            AccountBalance::new(total, Money::zero(Currency::USDC()), total),
        );
        let account_id = account.id();
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_account(AccountAny::Margin(account))
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        cache
            .borrow_mut()
            .add_mark_price(MarkPriceUpdate::new(
                instrument.id(),
                Price::from("49999"),
                0.into(),
                0.into(),
            ))
            .unwrap();
        let mut portfolio = Portfolio::new(
            Rc::new(RefCell::new(TestClock::new())),
            Rc::clone(&cache),
            None,
        );
        let fill = OrderFilledSpec::builder()
            .instrument_id(instrument.id())
            .account_id(account_id)
            .position_id(PositionId::new("CROSSING-TIER"))
            .order_side(OrderSide::Buy)
            .last_qty(Quantity::from("3000"))
            .last_px(Price::from("49999"))
            .currency(Currency::USDC())
            .build();
        let position = Position::new(&instrument, fill.clone());
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        portfolio.update_position(&PositionEvent::PositionOpened(PositionOpened::create(
            &position,
            &fill,
            UUID4::new(),
            0.into(),
        )));
        let mark = MarkPriceUpdate::new(instrument.id(), Price::from("60000"), 0.into(), 0.into());
        cache.borrow_mut().add_mark_price(mark).unwrap();
        msgbus::publish_mark_price(
            format!("data.mark_prices.{}", instrument.id()).into(),
            &mark,
        );
        let cached = cache.borrow();
        let account = cached.account(&account_id).unwrap();
        let AccountAny::Margin(account) = &*account else {
            panic!("Margin account required")
        };
        assert_eq!(
            account.total_initial_margin(Currency::USDC()),
            Money::from("4500000 USDC")
        );
        assert_eq!(
            account.total_maintenance_margin(Currency::USDC()),
            Money::from("2625000 USDC")
        );
        assert_eq!(
            account.balance_locked(Some(Currency::USDC())),
            Some(Money::from("4500000 USDC"))
        );
        assert!(
            account
                .calculate_initial_margin(&instrument, Quantity::from("3000"), mark.value, None)
                .is_err()
        );
    }
}
