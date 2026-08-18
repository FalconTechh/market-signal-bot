# NexCandle AI v2.1 Advanced — production upgrade

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
