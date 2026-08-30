# NexCandle AI PRO V37 — MAHIM

## Fixed public configuration
- 15 currency pairs only:
  GBP/JPY, AUD/CAD, AUD/CHF, AUD/JPY, AUD/USD,
  CAD/CHF, CAD/JPY, EUR/CAD, EUR/CHF, EUR/GBP,
  EUR/USD, GBP/CAD, GBP/CHF, USD/CAD, USD/JPY
- Timeframes only: 1 MIN, 5 MIN, 1 HOUR
- Developer display name: MAHIM
- Maximum 20 candles are retained/returned to the signal engine.
- Only validated completed OHLC candles are analyzed.
- No synthetic/fake candle generation is used by the signal path.
- An unfinished current candle is removed before analysis.

## Data reliability
Provider failover remains enabled. A stale feed is rejected rather than converted into a fresh-looking signal. Provider-specific errors are kept in server logs while the public API returns a clean temporary-data-unavailable response.

## Signal model
The next-candle classifier uses candle structure, body/close pressure, wick rejection, engulfing/inside-bar context, recent sequence pressure, breakout context, extreme rejection, continuation/reversal checks and candle-size quality. A weak/conflicting setup can return WAIT instead of forcing UP or DOWN.

This is a probabilistic market-data model; it cannot guarantee the next candle's direction.

## Deploy
Upload the ZIP contents to Render and redeploy the service. Keep the existing environment/API keys. Do not mix files from older V34/V35/V36 builds.
