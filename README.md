# NexCandle AI v4.1 — Premium UP/DOWN 3D Resilient

Premium mobile-first signal dashboard for the existing NexCandle AI engine.

## Public premium UI
- Public signal display is **UP or DOWN only**.
- No `ERR`, `WAIT`, score, price, R:R, or provider error is exposed in the premium scanner.
- 3D radar/orbit scanner with glow, beam, particles and signal-lock animation.
- Pair/timeframe selectors.
- Premium market board with directional outputs.
- Provider/API errors are handled silently by the UI; the last valid direction remains visible during a refresh.
- The server uses the existing validated market-data provider chain and a local multi-factor fallback when MTF fusion is temporarily unavailable.

## Important
UP/DOWN is a directional model output from available validated market data. It is not a guaranteed next-candle outcome or profit promise.

## Render
Keep the existing Render environment variables and deploy `app.py` with:
`pip install -r requirements.txt`
then:
`python app.py`

Do not delete the current Render service until this version has been tested successfully.


## v4.1 resilience
- Telegram signal response is now only `UP` or `DOWN`; the previous market-data error message is removed from the normal signal path.
- A transient provider failure first retries the local validated feed, then uses the last validated direction for the exact pair/timeframe for a short configurable window.
- Market candle cache defaults to 180 seconds and signal cache to 30 seconds to reduce provider-rate-limit bursts.
- No random/fabricated direction is generated.
