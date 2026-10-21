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

//! Factory for creating sandbox execution clients.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::Clock,
    factories::{ClientConfig, SimulatedExecutionClientFactory},
    live::clock::LiveClock,
};
use nautilus_execution::{client::core::ExecutionClientCore, funding::FundingCheckpoint};
use nautilus_model::{
    data::FundingRateUpdate,
    enums::BookType,
    identifiers::{ClientId, TraderId},
};

use crate::{
    config::SandboxExecutionClientConfig,
    execution::{SandboxExecutionClient, SandboxFundingHandle},
};

impl ClientConfig for SandboxExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating sandbox execution clients.
#[derive(Debug, Default, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.sandbox",
        unsendable,
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.sandbox")
)]
pub struct SandboxExecutionClientFactory;

impl SandboxExecutionClientFactory {
    /// Creates a new [`SandboxExecutionClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl SandboxExecutionClientFactory {
    /// Creates an opt-in funded quote-only client and its read-only native capture handle.
    ///
    /// An original checkpoint restores the same native account without startup funding.
    /// Starting or connecting the returned client remains a separate owner operation.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported profile or a mismatched original financial cut.
    pub fn create_funded_quote_only(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &SandboxExecutionClientConfig,
        cache: Rc<RefCell<Cache>>,
        checkpoint: Option<FundingCheckpoint>,
        original_funding: Option<FundingRateUpdate>,
    ) -> anyhow::Result<(Box<dyn ExecutionClient>, SandboxFundingHandle)> {
        anyhow::ensure!(
            config.book_type == BookType::L1_MBP
                && !config.bar_execution
                && !config.trade_execution
                && !config.queue_position
                && !config.liquidity_consumption
                && config.latency_model.is_none()
                && config.fill_model.is_none(),
            "unsupported funded quote-only profile"
        );
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(LiveClock::default()));
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            config.venue,
            config.oms_type,
            config.account_id,
            config.account_type,
            config.base_currency,
            cache.clone(),
        );
        let mut client = SandboxExecutionClient::new(core, config.clone(), clock.clone(), cache);
        anyhow::ensure!(
            checkpoint.is_none() || original_funding.is_none(),
            "restoration cannot replace original native funding input"
        );
        anyhow::ensure!(
            checkpoint.is_some()
                || config.expected_funding_ns.is_none()
                || original_funding.is_some(),
            "bounded native funding requires its original selected update"
        );

        if let Some(checkpoint) = checkpoint {
            client.restore_funding(checkpoint)?;
            client.restore_quote_only()?;
        } else {
            client.enable_funding()?;

            if let Some(update) = original_funding {
                let now = clock.borrow().timestamp_ns();
                anyhow::ensure!(
                    config.expected_funding_ns.is_none()
                        || (update.ts_event <= now
                            && update.ts_init <= now
                            && now.as_u64().saturating_sub(update.ts_event.as_u64())
                                <= 5_000_000_000
                            && now.as_u64().saturating_sub(update.ts_init.as_u64())
                                <= 5_000_000_000),
                    "bounded original funding observation is stale or future"
                );
                client.process_funding_rate(update)?;
            }
        }
        let handle = client
            .funding_handle()
            .ok_or_else(|| anyhow::anyhow!("native funding capture unavailable"))?;
        Ok((Box::new(client), handle))
    }

    /// Creates a sandbox client using its original quote-only account state.
    ///
    /// # Errors
    ///
    /// Returns an error if the original cache state or restoration profile is unsupported.
    pub fn create_restored_quote_only(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &SandboxExecutionClientConfig,
        cache: Rc<RefCell<Cache>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(LiveClock::default()));
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            config.venue,
            config.oms_type,
            config.account_id,
            config.account_type,
            config.base_currency,
            cache.clone(),
        );
        let mut client = SandboxExecutionClient::new(core, config.clone(), clock, cache);
        client.restore_quote_only()?;
        Ok(Box::new(client))
    }
}

impl SimulatedExecutionClientFactory for SandboxExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: Rc<RefCell<Cache>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let sandbox_config = config
            .as_any()
            .downcast_ref::<SandboxExecutionClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for SandboxExecutionClientFactory. Expected SandboxExecutionClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(LiveClock::default()));

        let core = ExecutionClientCore::new(
            trader_id,
            client_id,
            sandbox_config.venue,
            sandbox_config.oms_type,
            sandbox_config.account_id,
            sandbox_config.account_type,
            sandbox_config.base_currency,
            cache.clone(),
        );

        let client = SandboxExecutionClient::new(core, sandbox_config, clock, cache);
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        "SANDBOX"
    }

    fn config_type(&self) -> &'static str {
        "SandboxExecutionClientConfig"
    }
}
