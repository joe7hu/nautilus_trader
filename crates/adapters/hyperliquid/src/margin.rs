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
    accounts::margin_model::MarginModel,
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
    use super::*;
    use crate::http::{
        models::PerpMeta,
        parse::{create_instrument_from_def, parse_perp_instruments},
    };
    use nautilus_model::instruments::InstrumentAny;
    use rstest::rstest;

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
}
