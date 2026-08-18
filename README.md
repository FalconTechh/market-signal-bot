# NexCandle AI v3.5 Webhook-Hardened — production upgrade

Production-oriented Telegram forex analysis bot. `/start` is NOT required: ordinary text, including `hello`, `hi`, `signal`, or random text, opens the dashboard unless the user is currently completing a payment reference/proof flow.

## Upgrade highlights
- Any text -> welcome/dashboard; no `/start` dependency
- Live market-data provider fallback: Finnhub -> Twelve Data -> Yahoo fallback
- Supports 1m / 5m / 15m / 30m / 45m / 1h / 4h
- Correct 45m/4h aggregation when the provider does not expose that interval directly
- Freshness validation: stale data cannot generate a new signal
- EMA / RSI / MACD / Bollinger / ATR / candle-structure analysis
- Entry timeframe + higher-timeframe confirmation
- CALL / PUT / WAIT logic; no forced signal when confirmation is weak
- Exact next-candle clock plus configurable entry confirmation window
- Entry Timing now asks for pair + timeframe and shows an actual timing window
- Signal output includes data source, data age, setup score, MTF confirmation, reference price, SL/TP and timing window
- User-safe errors; raw API errors are logged but not shown
- Market analysis is moved off the Telegram event loop so slow providers do not freeze the bot
- Payment methods support legacy and alternate Render variable names
- Payment submission automatically notifies the configured admin Telegram chat
- Admin receives approve/reject buttons immediately; proof screenshots are forwarded to admin
- Approval is atomic and activates Premium in the database
- Payment status/history, signal history, backtest, performance tracking and alerts
- Premium scanner and auto alerts
- Flask health endpoint for Render

## Render environment
Required:
- `TELEGRAM_BOT_TOKEN`
- `ADMIN_TELEGRAM_ID` (or supported alias `TELEGRAM_CHAT_ID`)
- At least one market provider key is strongly recommended

Market data:
- `FINNHUB_API_KEY`
- `TWELVEDATA_API_KEY` or `TWELVE_DATA_API_KEY`

Payment:
- `INDIA_UPI` / `UPI_ID` / `PAYMENT_UPI`
- `UAE_BOTIM` / `BOTIM_NUMBER` / `PAYMENT_BOTIM`
- `PREMIUM_PRICE` / `PREMIUM_SCAN_PRICE`

Timing:
- `ENTRY_CONFIRM_SECONDS` default `10`
- `ENTRY_WINDOW_SECONDS` default `45`
- `STALE_DATA_MULTIPLIER` default `2.5`

## Important deployment note
SQLite on Render is only persistent if the service has persistent storage. For production payment/subscription history, use a persistent Render disk or migrate the database layer to managed PostgreSQL. Do not rely on an ephemeral filesystem for permanent Premium/payment records.

## Security
Never put real Telegram/API secrets into GitHub or screenshots. If a real bot token or API key has been exposed, rotate it before deploying.

## Signal disclaimer
The score is a technical setup-quality score, not a guaranteed probability, win rate, or profit forecast. Historical backtests do not guarantee future results.


## v2.1 fixes included
- Free credit is consumed only for an actual CALL/PUT setup; WAIT does not consume a credit.
- Signal resolver now evaluates all completed candles formed after the signal candle instead of checking only the latest candle.
- Pending signals are bounded by a validity window, preventing the Accuracy page from staying at zero forever.
- Accuracy separates WIN/LOSS from PENDING/EXPIRED/AMBIGUOUS and shows pair/timeframe performance.
- Signal cards now show strength label, next-candle timing, entry window, validity deadline and MTF status.
- Payment instructions explicitly request Transaction ID / Reference ID and allow proof screenshots.
- India UPI defaults to 6361472511 and UAE BOTIM defaults to 0522445121 if Render variables are not supplied; Render variables still override these defaults.
- `/health` reports v2.1 status and payment-method configuration.


## v2.2 Professional Signal Engine upgrade

- Professional Get Signal plan with exact next-candle and preferred-entry windows.
- UAE (Asia/Dubai), India (Asia/Kolkata), and UTC clock display.
- Multi-factor scoring: EMA trend, EMA200 regime, MACD momentum, RSI, ADX-style trend strength, candle quality, market structure, breakout context, ROC and volatility.
- Conservative WAIT gate when local and higher-timeframe confirmation disagree.
- Rich MTF confirmation and setup explanation.
- Entry state: WAIT FOR CONFIRMATION / ENTRY WINDOW ACTIVE / EXPIRED — SKIP.
- Stale-data protection remains enabled.
- Existing payment flow and admin approval flow are intentionally unchanged.


## v2.3 Ultra AI engine

The market-analysis core now uses an explainable hybrid ensemble rather than a single indicator score:
- Regime detection: TRENDING / RANGING / EXPANSION / HIGH_VOLATILITY / TRANSITION
- Independent specialist models: trend, momentum, structure, breakout, mean-reversion, volatility, candle, stochastic and CCI
- Regime-aware model weighting and ensemble fusion
- Ensemble agreement is part of the signal gate; conflicting evidence can force WAIT
- Additional features: stochastic, CCI, EMA slope, Bollinger width, ATR percentage and swing-based support/resistance
- Dynamic quality scoring combines raw confluence, regime, model agreement and MTF confirmation
- Scanner ranks qualifying pairs by the same engine instead of using a separate scoring method
- Signal records store the AI/regime fingerprint for later performance analysis
- Get Signal shows AI regime and ensemble evidence alongside the exact entry window
- Existing payment and admin-approval code is intentionally unchanged

This is an explainable quantitative/AI-style ensemble, not a claim of guaranteed prediction or autonomous profit generation.


## v3.2 market-data hardening
- Shared HTTP connection pool with retry/backoff for transient 429/5xx responses.
- Finnhub, Twelve Data and Yahoo fallback all use the hardened request path.
- Yahoo tries both chart hosts before declaring the fallback unavailable.
- Longer market/MTF caching reduces free-provider rate-limit failures during repeated scans.
- `/health/data` performs a lightweight EUR/USD 5m data check and reports the selected provider without exposing API keys.
- If `/health/data` is degraded, verify the Render environment variables and provider account/key limits.
- The bot never fabricates a signal when fresh validated candles are unavailable.

### Render variables to verify
`TELEGRAM_BOT_TOKEN`, `ADMIN_TELEGRAM_ID`, and at least one of:
`FINNHUB_API_KEY`, `TWELVEDATA_API_KEY` (or `TWELVE_DATA_API_KEY`).

## v3.3 critical fixes

### 1) Market data
The previous logs showed two separate provider failures:
- Finnhub: HTTP 403 / access denied.
- TwelveData: connection/retry failure.

The bot now supports a provider chain:
1. OANDA (recommended for FX when configured)
2. TwelveData
3. Finnhub
4. Alpha Vantage
5. Yahoo fallback (last resort; may be delayed and is not a broker feed)

The bot never invents a signal when every provider fails.

For professional FX data, configure `OANDA_API_TOKEN` and optionally `OANDA_ENVIRONMENT=live` or `practice`.

### 2) Telegram 409 Conflict
Render web services should use webhook mode. v3.3 includes a Telegram webhook receiver and avoids `getUpdates` polling conflicts.
Set:
- `BOT_MODE=webhook`
- `RENDER_EXTERNAL_URL=https://YOUR-SERVICE.onrender.com`
- `WEBHOOK_SECRET=` to a long random string (recommended)

Only one bot instance needs to own the Telegram webhook URL. Old polling deployments should be stopped.

### 3) Diagnostics
Open `/health/data` on the Render service to see each configured provider's status, candle count, and last-data age without exposing API keys.


## Render Telegram 409 fix
On Render, the app automatically uses Telegram webhook mode when `RENDER_EXTERNAL_HOSTNAME` is present. This prevents the `Conflict: terminated by other getUpdates request` error caused by polling. If `BOT_MODE` was manually set to `polling`, remove it or set `BOT_MODE=webhook`.


## v3.5 one-time Render hardening

- Render web services forcibly use Telegram webhook mode even if an old `BOT_MODE=polling` environment variable exists.
- `getUpdates` polling is blocked on Render, preventing this codebase from creating a Telegram 409 Conflict itself.
- `/health/telegram` reports webhook state, pending updates, and Telegram's last webhook error without exposing secrets.
- TwelveData 429 responses fail fast and trigger the existing provider cooldown instead of creating retry storms.
- The bot still never fabricates a signal when no fresh validated market feed is available.

### Important Telegram 409 rule

A 409 can still be produced by **another running copy of the same bot token** outside this service (another Render service, local Python process, VPS, Replit, etc.). After deploying v3.5, stop every other copy and rotate the Telegram bot token if it has ever been exposed in logs/screenshots. Then deploy only this Render service.

### Recommended Render market-data configuration

For reliable FX data, configure `OANDA_API_TOKEN` first. Keep TwelveData/Finnhub/Alpha Vantage as independent fallbacks if you have valid keys. If a provider returns 403/429, the bot cools it down and moves to the next provider.

Do not put API keys or Telegram tokens into GitHub, screenshots, or chat messages.
