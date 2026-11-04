# Hyperliquid native qualification

This compatible backport starts from Tradecraft's `36b7bd44` native producer.
It does not adopt unrelated changes from the newer develop branch.

BTC keeps `BTC-USD-PERP.HYPERLIQUID` as its public identifier. Its economic quote
is USDT, while cost, notional, fees and position settlement use USDC without an
FX conversion. Equivalent dollar currency classification does not erase settlement
identity or precision. Non-dollar option and inverse valuation conventions remain
unchanged. Risk caps and instrument limits compare compatible units and refuse
mismatches.

The adapter retains each asset's margin-table association and referenced tiers.
`HyperliquidMarginModel` calculates initial margin from the supplied current mark
and user leverage, and tiered maintenance independently of user leverage. It is
an explicit, uninstalled model. The portfolio's current margin price selectors
and aggregate account reserve lifecycle still require qualification before this
model can support a Hyperliquid paper profile. Zero default adapter fees are not
a qualified trading fee schedule.

Hyperliquid funding selects the full-precision positive oracle, with no mark or
BBO fallback. Receipt-timed context predictions cannot become final hourly rates.
Historical responses preserve their actual knowledge clock separately from venue
event time; late history cannot impersonate timely settlement input. A historical
row timestamp does not establish the next funding boundary; that field stays
unknown. A final
rate, exact boundary, contemporaneous oracle and original position/account cut
remain necessary. Public historical rates do not supply an oracle, and active
asset context does not supply a venue boundary clock. These changes do not
qualify genuine funding or enable paper/live trading.

All adapter millisecond conversions use checked integer scaling. Existing
recordings keep their original definitions, clocks and producer identities;
no old timestamp or currency is rewritten.

The `hyperliquid-btc-reconnect-qualification` binary performs a bounded public
mainnet BTC campaign. It requests the native transport reconnect, preserves
original metadata and native output with separate observer clocks, and requires
fresh trades, quotes and complete book snapshots on both sides of an observed
reconnect. Its HTTP client disables system proxies; its WebSocket target is
fixed. The tool uses no wallet, credentials, private API or trading command.
It records unavailable disconnect notifications and receipt-only context clocks
honestly. Its JSONL is retained evidence, not raw WebSocket wire frames.

## Authoritative terms

- [Contract specifications](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/contract-specifications)
- [Margining](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margining)
- [Margin tiers](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margin-tiers)
- [Funding](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding)
- [Tick and lot size](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size)
