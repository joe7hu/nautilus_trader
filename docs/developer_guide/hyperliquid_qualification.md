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
an explicit model selected through `SimulatedVenueConfig.margin_model`; venue names do not
install it. Its current-mark accounting capability makes the native portfolio reserve
initial margin on net open positions plus the gross remaining quantity of non-reduce-only
orders, using a positive mark known by both event and receipt clocks. Opening leverage tier checks remain separate from existing exposure reserves: a mark moving
an existing position into a lower-leverage tier still refreshes position IM and tiered MM, and
does not prevent an eligible reducing exit. Maintenance is reported separately and is not added to locked cash. Mark updates refresh these requirements
even when portfolio PnL uses another price policy. Limit, fill and entry prices cannot replace
the mark for these requirements; collateral currencies are not converted.

The first account is constructed from the execution client's actual `AccountState`. Its
configured model is installed before cache insertion and delivery to the portfolio; the original
event is recorded once. `SimulatedExchange.configure_account` reinstalls configuration on a
restored account without creating a financial event, and refuses leverage or model changes that
differ from the restored contract. The account persists the model name and accounting capability.
Deserialization preserves its original financial values but leaves a pinned account disarmed until the matching model is
reinstalled through native configuration. Native risk refuses a disarmed account. Legacy
accounts without a pin retain their existing model, serialization and reserve semantics.

This is a bounded conservative reserve contract, not a qualified implementation of all venue
cross-margin buying power. Native risk admits one base-quantity limit IOC opening order on a flat account with an opening limit no worse
than the current mark, then only limit IOC reduce-only exits until all positions and opening
orders clear. Resting opening orders are unqualified because native matching does not recheck
venue equity and margin before subsequent matches.
Additional ordinary openings and multi-opening batches are refused. A first opening also needs
cash for initial margin plus the larger nonnegative maker/taker commission budget, calculated
natively at the larger mark/order price. The instrument's simulation fees must be selected
explicitly; this does not qualify arbitrary fee-model overrides. In particular, cash free
balance is not venue opening capacity: the venue includes unrealized PnL in cross equity.
Opposite-side open-order offsets and broader equity-aware admission remain unqualified.
Zero default adapter fees are not a qualified trading fee schedule.

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

Portfolio currency-conversion fixtures select an explicit legacy quote-currency test model when
testing a model-calculated currency that differs from settlement. Default linear models retain
the existing settlement/cost denomination; valuation fixtures assert that denomination and
convert from the matching currency.
