# NexCandle AI v4.3 — Hyper-Reliable Next-Candle Engine

This build addresses the Render/Telegram failure mode seen in the logs.

## Reliability changes
- Render always uses Telegram webhook mode.
- Credentialed market-data providers are tried before public Yahoo fallback.
- 401/403/429/5xx provider failures get cooldowns instead of retry storms.
- Per-timeframe cache TTLs keep next-candle data fresher.
- MTF confirmation resamples the already-fetched entry timeframe whenever possible, greatly reducing API calls.
- `/health/data` does not call external providers unless `?probe=1` is explicitly requested, so uptime monitoring cannot consume API quotas.
- Telegram callback exceptions are caught and surfaced as a visible message instead of silently producing no reply.
- No random UP/DOWN is generated when fresh validated market data is unavailable.

## Analysis engine
The model combines EMA trend, RSI, MACD, ATR, ADX, Bollinger structure, stochastic, CCI, ROC, candle body/wick structure, engulfing patterns, breakout distance, optional volume shock, market regime, MTF confirmation, and an explainable ensemble.

A high-confidence setup is required before the directional Telegram response is returned.

## Market data
For reliable production use, configure at least one credentialed provider. Recommended order:

1. OANDA
2. TwelveData
3. SiftingIO
4. Finnhub
5. Alpha Vantage
6. Yahoo fallback

Do not rely on public Yahoo alone for a production next-candle service; it can rate-limit requests.

Keep API keys only in Render Environment Variables. Never paste them into chat.

## Uptime
Use `/health` for uptime monitoring. Do not use `/health/data?probe=1` as the uptime target.

## Accuracy
This engine is designed to improve data freshness, confluence and robustness. No technical model can guarantee the direction of the next candle or a fixed accuracy percentage. If fresh data or confluence is insufficient, the bot refuses to manufacture a signal.

## Deploy
After deployment, check:
- `/health`
- `/health/telegram`
- `/health/data`

For a one-time provider diagnostic, use `/health/data?probe=1`.
