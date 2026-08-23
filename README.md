# NexCandle AI PRO — Resilient Market Engine V11

This build focuses on **fail-safe operation**, not a false promise of guaranteed market availability or guaranteed prediction accuracy.

## V11 resilience upgrades
- Smart provider failover with the configured provider order.
- Automatic bounded retries for transient provider/network failures.
- Immediate provider cooldown on quota/rate-limit/auth failures.
- Last-successful-provider preference per pair/timeframe.
- Single-flight locking to prevent many users from hammering the same API.
- In-memory candle cache plus SQLite recovery cache.
- Recovery of recent validated candles after process restart when deployment storage is persistent.
- Recently confirmed directional recovery during a short provider outage, explicitly marked `RECOVERY` rather than presented as fresh live data.
- Completed-candle validation before analysis.
- Existing multi-timeframe analysis and adaptive regime-aware ensemble retained.
- Telegram webhook remains Render-safe; polling is not used on Render.
- User-facing provider/API errors remain hidden.

## Important
No market-data system can honestly guarantee that it will **never** experience a provider outage, network outage, Render restart, or stale feed. This build is designed so those failures are isolated, retried, bypassed, cached, and recovered from where possible instead of crashing the bot or inventing a signal.

### Recommended Render variables
Keep the existing API keys and settings. Useful resilience controls include:

- `DATA_PROVIDER_ORDER=dukascopy,biquote,twelvedata,finnhub,alphavantage,yahoo2,yahoo`
- `PROVIDER_RETRY_LIMIT=3`
- `PROVIDER_TIMEOUT_SECONDS=10`
- `ALLOW_STALE_CANDLE_FALLBACK=true`
- `PERSISTED_CACHE_MAX_AGE=21600`
- `RECOVERY_MAX_AGE=900`

For durable restart recovery, use persistent Render storage for the SQLite database path. Without persistent storage, the live in-process cache and provider failover still work, but a full restart clears SQLite recovery data.


## V14 all-timeframe next-candle data
- Public premium API now accepts 1m, 5m, 15m, 30m, 45m, 1h and 4h.
- Premium timeframe selector includes all seven intervals.
- Next-candle model bias is reported separately from trade confirmation. A weak/ambiguous setup no longer looks like a market-data outage.
- 45m and 4h can be built deterministically from validated lower-timeframe OHLC when a direct interval is unavailable.
- No prices or directions are fabricated during a true data outage.
