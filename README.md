# NexCandle AI v4.6 — Hyper Fast Free-Data Fix

- BiQuote is first free/public market-data lane.
- Dukascopy official free-service fallback is added.
- SiftingIO → TwelveData → Finnhub → Alpha Vantage remain available.
- Yahoo remains last fallback.
- Provider order no longer lets a failing SiftingIO key block BiQuote.
- BiQuote and Yahoo2 race in parallel to reduce latency.
- urllib3 retry storms are disabled; provider failover/cooldowns handle retries.
- Default market timeout is 8 seconds.
- Telegram signal requests get one quick retry.
- No random direction is generated without validated market data.
- Website public output remains UP/DOWN only.
- Existing multi-factor + MTF ensemble remains the next-candle engine.
- OANDA is not required.

BiQuote documents public no-auth OHLC candles; Dukascopy documents its free historical-price service. Availability/limits can vary.

Accuracy cannot be guaranteed at 100%; the engine refuses to manufacture a direction when data/confluence is insufficient.


Hotfix: signal retry stability patch applied.
