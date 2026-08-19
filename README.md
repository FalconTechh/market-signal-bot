# NexCandle AI v4.2 — Advanced Next-Candle UP/DOWN Premium

Premium mobile-first signal dashboard for the existing NexCandle AI engine.

## Public premium UI
- Public signal display is **UP or DOWN only**.
- No `ERR`, `WAIT`, score, price, R:R, or provider error is exposed in the premium scanner.
- 3D radar/orbit scanner with glow, beam, particles and signal-lock animation.
- Pair/timeframe selectors.
- Premium market board with directional outputs.
- Provider/API errors are handled silently by the UI; the last valid direction remains visible during a refresh.
- The server uses the existing validated market-data provider chain and a local multi-factor fallback when MTF fusion is temporarily unavailable.

## v4.2.1 resilience fix
- The premium dashboard no longer fires all pair scans simultaneously on page load or every 90 seconds.
- Full Power Scan now validates pairs sequentially with a small delay to reduce provider 429/rate-limit bursts.
- Rate-limited providers receive a longer cooldown instead of being hammered repeatedly.
- A recently validated signal can be recovered from SQLite after a Render restart during a short provider outage.
- If no recent validated signal exists, Telegram reports that the live data provider is unavailable instead of silently failing.

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

## v4.2 Telegram signal behavior
- **Get Signal** returns exactly `UP` or `DOWN` and nothing else in the signal message.
- Premium automatic alerts also return exactly `UP` or `DOWN`.
- The underlying engine remains multi-factor + AI ensemble + MTF + freshness/cache based.
- The direction is the model's directional estimate for the **next candle**, using the latest completed candle and available validated market data.
- No `WAIT`, `CALL`, `PUT`, `ERR`, score, SL/TP, provider name, or verbose analysis is exposed in the Get Signal result.
- No fake/random direction is generated.
