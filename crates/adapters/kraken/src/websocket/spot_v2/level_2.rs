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

//! Runtime state for Kraken Spot L2 book handling.

use std::sync::Arc;

use ahash::AHashMap;
use nautilus_core::{AtomicMap, UnixNanos};
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas},
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    instruments::{Instrument, any::InstrumentAny},
    orderbook::OrderBook,
    types::Price,
};

use super::{
    checksum::{crc32_ieee, format_raw},
    messages::{KrakenWsBookData, KrakenWsBookLevel},
    parse::parse_book_deltas,
};

#[derive(Debug, Clone)]
pub(crate) struct L2Depths {
    depths: Arc<AtomicMap<String, u32>>,
}

impl Default for L2Depths {
    fn default() -> Self {
        Self {
            depths: Arc::new(AtomicMap::new()),
        }
    }
}

impl L2Depths {
    pub(crate) fn get(&self, symbol: &str) -> Option<u32> {
        self.depths.load().get(symbol).copied()
    }

    pub(crate) fn insert(&self, symbol: &str, depth: u32) {
        self.depths.insert(symbol.to_string(), depth);
    }

    pub(crate) fn remove(&self, symbol: &str) {
        self.depths.rcu(|depths| {
            depths.remove(symbol);
        });
    }

    pub(crate) fn clear(&self) {
        self.depths.store(AHashMap::new());
    }
}

#[derive(Debug, Default)]
pub(crate) struct L2BookState {
    pub(crate) books: AHashMap<InstrumentId, OrderBook>,

    // Decimal scale from the wire is retained separately from instrument-rounded domain values
    levels: AHashMap<InstrumentId, AHashMap<(OrderSide, Price), KrakenWsBookLevel>>,
}

impl L2BookState {
    pub(crate) fn process_book(
        &mut self,
        book: &KrakenWsBookData,
        instrument: &InstrumentAny,
        sequence: u64,
        is_snapshot: bool,
        depth: Option<u32>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<(OrderBookDeltas, u64)>> {
        let instrument_id = instrument.id();

        if !is_snapshot && !self.books.contains_key(&instrument_id) {
            log::warn!(
                "Kraken L2 awaiting snapshot: symbol={}, sequence={sequence}",
                book.symbol
            );
            return Ok(Some(self.invalidate(instrument_id, sequence, ts_init)));
        }

        let Some(depth) = depth.filter(|depth| *depth > 0) else {
            log::warn!(
                "Kraken L2 missing subscription depth: symbol={}",
                book.symbol
            );
            return Ok(Some(self.invalidate(instrument_id, sequence, ts_init)));
        };
        let mut deltas = match parse_book_deltas(book, instrument, sequence, is_snapshot, ts_init) {
            Ok(deltas) => deltas,
            Err(e) => {
                log::warn!(
                    "Invalid Kraken L2 deltas: symbol={}, sequence={sequence}: {e}",
                    book.symbol
                );
                return Ok(Some(self.invalidate(instrument_id, sequence, ts_init)));
            }
        };
        let parsed_delta_count = deltas.len();
        let mut next_sequence = sequence + deltas.len() as u64;
        let book_state = self
            .books
            .entry(instrument_id)
            .or_insert_with(|| OrderBook::new(instrument_id, BookType::L2_MBP));

        if !deltas.is_empty()
            && let Err(e) =
                book_state.apply_deltas(&OrderBookDeltas::new(instrument_id, deltas.clone()))
        {
            log::warn!(
                "Failed to apply Kraken L2 deltas: symbol={}: {e}",
                book.symbol
            );
            return Ok(Some(self.invalidate(instrument_id, sequence, ts_init)));
        }

        if let Err(e) = prune_deltas_to_depth(
            book_state,
            depth,
            is_snapshot,
            &mut next_sequence,
            ts_init,
            &mut deltas,
        ) {
            log::warn!(
                "Failed to prune Kraken L2 depth: symbol={}: {e}",
                book.symbol
            );
            return Ok(Some(self.invalidate(instrument_id, sequence, ts_init)));
        }

        let levels = self.levels.entry(instrument_id).or_default();

        if is_snapshot {
            levels.clear();
        }

        for (side, updates) in [(OrderSide::Buy, &book.bids), (OrderSide::Sell, &book.asks)] {
            for level in updates.iter().flatten() {
                let price = Price::from_decimal_dp(level.price, instrument.price_precision())?;

                if level.qty.is_zero() {
                    levels.remove(&(side, price));
                } else {
                    levels.insert((side, price), level.clone());
                }
            }
        }

        for delta in deltas.iter().skip(parsed_delta_count) {
            if delta.action == BookAction::Delete
                && let Some(side) = delta.order.side
            {
                levels.remove(&(side, delta.order.price));
            }
        }

        let Some(calculated) = compute_checksum(book_state, levels) else {
            log::warn!("Kraken L2 missing checksum level: symbol={}", book.symbol);
            return Ok(Some(self.invalidate(instrument_id, sequence, ts_init)));
        };
        let valid = book.checksum == Some(calculated);
        log::trace!(
            "Kraken L2 checksum: symbol={}, sequence={sequence}, snapshot={is_snapshot}, expected={:?}, calculated={calculated}, valid={valid}",
            book.symbol,
            book.checksum,
        );

        if !valid {
            log::warn!(
                "Kraken L2 checksum invalid: symbol={}, sequence={sequence}, expected={:?}, calculated={calculated}; awaiting snapshot",
                book.symbol,
                book.checksum,
            );
            return Ok(Some(self.invalidate(instrument_id, sequence, ts_init)));
        }

        if deltas.is_empty() {
            return Ok(None);
        }
        set_last_delta_flag(&mut deltas);
        Ok(Some((
            OrderBookDeltas::new(instrument_id, deltas),
            next_sequence,
        )))
    }

    pub(crate) fn invalidate(
        &mut self,
        instrument_id: InstrumentId,
        sequence: u64,
        ts_init: UnixNanos,
    ) -> (OrderBookDeltas, u64) {
        self.books.remove(&instrument_id);
        self.levels.remove(&instrument_id);
        let mut clear = OrderBookDelta::clear(instrument_id, sequence, ts_init, ts_init);
        clear.flags = RecordFlag::F_LAST as u8;
        (
            OrderBookDeltas::new(instrument_id, vec![clear]),
            sequence + 1,
        )
    }
}

fn compute_checksum(
    book: &OrderBook,
    levels: &AHashMap<(OrderSide, Price), KrakenWsBookLevel>,
) -> Option<u32> {
    // Native book iteration owns ordering and depth; this map only retains decimal scale
    let mut checksum_input = String::new();

    for (side, price_level) in book
        .asks(None)
        .take(10)
        .map(|level| (OrderSide::Sell, level))
        .chain(
            book.bids(None)
                .take(10)
                .map(|level| (OrderSide::Buy, level)),
        )
    {
        let order = price_level.first()?;
        let level = levels.get(&(side, order.price))?;
        checksum_input.push_str(&format_raw(&level.price.to_string()));
        checksum_input.push_str(&format_raw(&level.qty.to_string()));
    }
    Some(crc32_ieee(checksum_input.as_bytes()))
}

fn prune_deltas_to_depth(
    book: &mut OrderBook,
    depth: u32,
    is_snapshot: bool,
    next_sequence: &mut u64,
    ts_init: UnixNanos,
    deltas: &mut Vec<OrderBookDelta>,
) -> anyhow::Result<()> {
    let prune_orders: Vec<BookOrder> = book
        .bids(None)
        .skip(depth as usize)
        .chain(book.asks(None).skip(depth as usize))
        .filter_map(|level| level.first().copied())
        .collect();

    if prune_orders.is_empty() {
        return Ok(());
    }

    let ts_event = deltas.last().map_or(ts_init, |delta| delta.ts_event);
    let mut flags = RecordFlag::F_MBP as u8;

    if is_snapshot {
        flags |= RecordFlag::F_SNAPSHOT as u8;
    }

    for order in prune_orders {
        let delta = OrderBookDelta::new(
            book.instrument_id,
            BookAction::Delete,
            order,
            flags,
            *next_sequence,
            ts_event,
            ts_init,
        );
        *next_sequence += 1;

        book.apply_delta(&delta)?;

        deltas.push(delta);
    }
    Ok(())
}

fn set_last_delta_flag(deltas: &mut [OrderBookDelta]) {
    for delta in deltas.iter_mut() {
        delta.flags &= !(RecordFlag::F_LAST as u8);
    }

    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
    }
}
