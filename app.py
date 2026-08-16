"""
Market Signal Telegram + Website Chatbot
Single-file MVP: app.py

Features:
- FastAPI website/API
- Telegram bot polling
- Twelve Data market candles
- RSI, EMA 9/21, MACD, Bollinger Bands, ADX, ATR, Stochastic, VWAP, Fibonacci
  retracement levels
- Multi-timeframe confirmation (1m + 5m + 15m agreement check)
- UP / DOWN / WAIT scoring
- Signal history in SQLite
- Background scheduler that watches a symbol list (multiple currencies) and
  pushes Telegram alerts with a chart image
- Outcome resolution job that grades past signals against real price action,
  so /api/accuracy reflects genuine historical hit-rate (not a guess)
- Per-symbol/timeframe backtest report
- Optional economic-calendar/news alerts (Finnhub, free tier)
- No automatic trading/order placement

Install:
    pip install -r requirements.txt

Create .env (copy from .env.example):
    TWELVE_DATA_API_KEY=YOUR_TWELVE_DATA_KEY
    TELEGRAM_BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID=YOUR_CHAT_ID_FOR_ALERTS   (optional, for scheduler push)
    WATCHLIST=AUD/NZD,EUR/USD                  (optional, comma-separated, for scheduler)
    SCAN_INTERVAL_SECONDS=300                  (optional, default 300)
    FINNHUB_API_KEY=                           (optional, for /news)
    HOST=0.0.0.0
    PORT=8000

Run:
    python app.py

Website:
    http://YOUR_SERVER:8000/

API:
    /api/signal?symbol=EUR/USD&interval=1min
    /api/mtf?symbol=EUR/USD
    /api/history
    /api/accuracy
    /api/backtest_report
    /api/news
    /api/assets
    /health

Telegram:
    /start
    /signal EURUSD 1m
    /signal GBPUSD 5m
    /mtf AUDNZD
    /history
    /accuracy
    /backtest
    /news
    /help

IMPORTANT / READ THIS:
This is a market-analysis tool, not a guaranteed predictor. Nothing in this
codebase, no matter how it's configured, can reliably predict the direction
of the next candle with high accuracy. "Confidence" is a score derived from
indicator agreement, not a statistical probability of being right. The
accuracy numbers shown by /api/accuracy and /api/backtest_report are
historical and calculated from this bot's own past signals -- they are not,
and cannot be, a promise about future performance. This tool does not place
trades in Quotex or any other broker; it only informs.
"""

import os
import asyncio
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
import uvicorn

try:
    from telegram import Update
    from telegram.ext import Application, CommandHandler, ContextTypes
except Exception:
    Update = None
    Application = None
    CommandHandler = None
    ContextTypes = None

load_dotenv()

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
DB_FILE = os.getenv("DB_FILE", "signals.db")

app = FastAPI(title="Market Signal Bot", version="1.0.0")

DB_LOCK = threading.Lock()

SYMBOLS = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "AUDUSD": "AUD/USD",
    "USDCAD": "USD/CAD",
    "USDCHF": "USD/CHF",
    "NZDUSD": "NZD/USD",
    "AUDNZD": "AUD/NZD",
    "XAUUSD": "XAU/USD",
    "BTCUSD": "BTC/USD",
    "ETHUSD": "ETH/USD",
}

INTERVALS = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1min": "1min",
    "5min": "5min",
    "15min": "15min",
}

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def db():
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with DB_LOCK:
        con = db()
        con.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                price REAL,
                direction TEXT NOT NULL,
                score INTEGER NOT NULL,
                confidence INTEGER NOT NULL,
                rsi REAL,
                ema9 REAL,
                ema21 REAL,
                macd REAL,
                macd_signal REAL,
                adx REAL,
                atr REAL,
                bb_mid REAL,
                stoch_k REAL,
                stoch_d REAL,
                vwap REAL,
                tags_json TEXT,
                session TEXT,
                result TEXT DEFAULT 'PENDING'
            )
        """)
        # Backfill columns for DBs created before these fields existed.
        cols = [r[1] for r in con.execute("PRAGMA table_info(signals)").fetchall()]
        for col in ["atr", "stoch_k", "stoch_d", "vwap"]:
            if col not in cols:
                con.execute(f"ALTER TABLE signals ADD COLUMN {col} REAL")
        for col in ["tags_json", "session"]:
            if col not in cols:
                con.execute(f"ALTER TABLE signals ADD COLUMN {col} TEXT")
        con.commit()
        con.close()

init_db()

def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper().replace(" ", "")
    return SYMBOLS.get(s, s if "/" in s else s)

def normalize_interval(interval: str) -> str:
    i = interval.strip().lower()
    if i not in INTERVALS:
        raise ValueError("Supported intervals: 1m, 5m, 15m")
    return INTERVALS[i]

def fetch_candles(symbol: str, interval: str, outputsize: int = 250) -> pd.DataFrame:
    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY is missing in .env")

    params = {
        "symbol": normalize_symbol(symbol),
        "interval": normalize_interval(interval),
        "outputsize": min(max(outputsize, 100), 5000),
        "apikey": TWELVE_DATA_API_KEY,
        "format": "JSON",
    }

    r = requests.get(
        "https://api.twelvedata.com/time_series",
        params=params,
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()

    if "values" not in data:
        raise RuntimeError(data.get("message", "Market-data API returned no candles"))

    df = pd.DataFrame(data["values"])
    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    df = df.dropna(subset=["datetime", "open", "high", "low", "close"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return df

def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()

def rsi(close, n=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/n, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)

def macd(close):
    fast = ema(close, 12)
    slow = ema(close, 26)
    line = fast - slow
    signal = ema(line, 9)
    return line, signal

def bollinger(close, n=20, mult=2):
    mid = close.rolling(n).mean()
    std = close.rolling(n).std()
    upper = mid + mult * std
    lower = mid - mult * std
    return mid, upper, lower

def true_range(df):
    high, low, close = df["high"], df["low"], df["close"]
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

def atr(df, n=14):
    tr = true_range(df)
    return tr.ewm(alpha=1/n, adjust=False).mean()

def adx(df, n=14):
    high, low, close = df["high"], df["low"], df["close"]
    up = high.diff()
    down = -low.diff()

    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)

    tr = true_range(df)
    atr_v = tr.ewm(alpha=1/n, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/n, adjust=False).mean() / atr_v
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/n, adjust=False).mean() / atr_v

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1/n, adjust=False).mean().fillna(0)

def stochastic(df, k_period=14, d_period=3):
    low_min = df["low"].rolling(k_period).min()
    high_max = df["high"].rolling(k_period).max()
    k = 100 * (df["close"] - low_min) / (high_max - low_min).replace(0, np.nan)
    d = k.rolling(d_period).mean()
    return k.fillna(50), d.fillna(50)

def fibonacci_levels(df, lookback=50):
    """
    Classic retracement levels from the most recent swing high/low over the
    lookback window. These are reference price zones traders watch for
    reactions, not a prediction of what will happen at them.
    """
    window = df.tail(lookback)
    swing_high = float(window["high"].max())
    swing_low = float(window["low"].min())
    diff = swing_high - swing_low
    if diff <= 0:
        return None
    ratios = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
    return {
        str(r): round(swing_high - diff * r, 8)
        for r in ratios
    } | {"swing_high": round(swing_high, 8), "swing_low": round(swing_low, 8)}

def vwap(df):
    """
    Volume-weighted average price. Forex pairs from Twelve Data usually carry
    no real trade volume (spot FX is OTC), so this falls back to a plain
    typical-price average when volume is missing/zero and is flagged as such.
    For BTC/ETH, real volume is generally available and VWAP is meaningful.
    """
    typical = (df["high"] + df["low"] + df["close"]) / 3
    if "volume" in df.columns:
        vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    else:
        vol = pd.Series(0, index=df.index)
    has_volume = bool(vol.sum() > 0)
    if has_volume:
        cum_vol = vol.cumsum().replace(0, np.nan)
        return (typical * vol).cumsum() / cum_vol, True
    else:
        return typical.expanding().mean(), False

def detect_candle_pattern(df):
    """
    Detects the most recent candlestick pattern (bullish/bearish engulfing,
    pin bar / hammer / shooting star, doji). These are classic price-action
    signals traders watch for reversals or continuation — reference patterns
    from the last 1-2 candles, not a forecast of what comes next.
    """
    if len(df) < 2:
        return None, 0

    o1, h1, l1, c1 = df["open"].iloc[-1], df["high"].iloc[-1], df["low"].iloc[-1], df["close"].iloc[-1]
    o2, h2, l2, c2 = df["open"].iloc[-2], df["high"].iloc[-2], df["low"].iloc[-2], df["close"].iloc[-2]

    body1 = abs(c1 - o1)
    range1 = h1 - l1
    upper_wick = h1 - max(c1, o1)
    lower_wick = min(c1, o1) - l1

    if range1 <= 0:
        return None, 0

    # Doji: tiny body relative to range -> indecision, weakens conviction
    if body1 <= range1 * 0.1:
        return "Doji (indecision)", 0

    # Bullish engulfing
    if c2 < o2 and c1 > o1 and c1 >= o2 and o1 <= c2:
        return "Bullish engulfing", 15
    # Bearish engulfing
    if c2 > o2 and c1 < o1 and c1 <= o2 and o1 >= c2:
        return "Bearish engulfing", -15

    # Hammer / bullish pin bar: long lower wick, small body near top, after a down move
    if lower_wick >= body1 * 2 and upper_wick <= body1 * 0.6 and c2 > c1 <= o2:
        return "Hammer / bullish pin bar", 12
    # Shooting star / bearish pin bar: long upper wick, small body near bottom
    if upper_wick >= body1 * 2 and lower_wick <= body1 * 0.6:
        return "Shooting star / bearish pin bar", -12

    return None, 0

def detect_support_resistance(df, lookback=60, tolerance_pct=0.08):
    """
    Finds approximate support/resistance zones from local swing highs/lows
    over the lookback window (clustering nearby swing points), and checks
    whether the current price is sitting close to one. This flags a
    reference zone traders watch for reactions — not a forecast of a bounce
    or breakout.
    """
    window = df.tail(lookback).reset_index(drop=True)
    if len(window) < 10:
        return None

    highs, lows = [], []
    for i in range(2, len(window) - 2):
        h = float(window["high"].iloc[i])
        l = float(window["low"].iloc[i])
        if h >= window["high"].iloc[i-1] and h >= window["high"].iloc[i-2] and \
           h >= window["high"].iloc[i+1] and h >= window["high"].iloc[i+2]:
            highs.append(h)
        if l <= window["low"].iloc[i-1] and l <= window["low"].iloc[i-2] and \
           l <= window["low"].iloc[i+1] and l <= window["low"].iloc[i+2]:
            lows.append(l)

    price = float(window["close"].iloc[-1])
    tol = price * tolerance_pct / 100

    def cluster(levels):
        if not levels:
            return None
        levels = sorted(levels)
        clusters = [[levels[0]]]
        for lv in levels[1:]:
            if lv - clusters[-1][-1] <= tol * 3:
                clusters[-1].append(lv)
            else:
                clusters.append([lv])
        return [float(round(sum(c) / len(c), 8)) for c in clusters]

    resistance_levels = cluster(highs)
    support_levels = cluster(lows)

    nearest_resistance = min(
        (r for r in (resistance_levels or []) if r > price), default=None, key=lambda r: r - price
    ) if resistance_levels else None
    nearest_support = max(
        (s for s in (support_levels or []) if s < price), default=None, key=lambda s: price - s
    ) if support_levels else None

    near_resistance = bool(nearest_resistance is not None and (nearest_resistance - price) <= tol)
    near_support = bool(nearest_support is not None and (price - nearest_support) <= tol)

    return {
        "nearest_support": float(round(nearest_support, 8)) if nearest_support is not None else None,
        "nearest_resistance": float(round(nearest_resistance, 8)) if nearest_resistance is not None else None,
        "near_support": near_support,
        "near_resistance": near_resistance,
    }

def detect_divergence(df, lookback=30):
    """
    Detects RSI/price divergence: when price makes a new swing high/low but
    RSI does NOT confirm it (makes a lower high / higher low instead). This
    is a well-known early warning that the current trend's momentum is
    fading — a reversal becomes more likely, though timing is never exact.
    Checks the two most recent swing points in the lookback window.
    """
    window = df.tail(lookback).reset_index(drop=True)
    if len(window) < 15:
        return None

    rv = rsi(window["close"], 14)

    swing_highs, swing_lows = [], []
    for i in range(2, len(window) - 2):
        h, l = window["high"].iloc[i], window["low"].iloc[i]
        if h >= window["high"].iloc[i-1] and h >= window["high"].iloc[i-2] and \
           h >= window["high"].iloc[i+1] and h >= window["high"].iloc[i+2]:
            swing_highs.append((i, float(h)))
        if l <= window["low"].iloc[i-1] and l <= window["low"].iloc[i-2] and \
           l <= window["low"].iloc[i+1] and l <= window["low"].iloc[i+2]:
            swing_lows.append((i, float(l)))

    result = None

    if len(swing_highs) >= 2:
        (i1, p1), (i2, p2) = swing_highs[-2], swing_highs[-1]
        r1, r2 = float(rv.iloc[i1]), float(rv.iloc[i2])
        if p2 > p1 and r2 < r1:
            result = "Bearish divergence (price higher high, RSI lower high)"

    if len(swing_lows) >= 2:
        (i1, p1), (i2, p2) = swing_lows[-2], swing_lows[-1]
        r1, r2 = float(rv.iloc[i1]), float(rv.iloc[i2])
        if p2 < p1 and r2 > r1:
            bull_result = "Bullish divergence (price lower low, RSI higher low)"
            result = result or bull_result

    return result

def trading_session_info(dt=None):
    """
    Classifies the current time into major forex trading sessions (Asia,
    London, New York, and their overlaps) by UTC hour. Overlap sessions
    (London/NY) carry the deepest liquidity, so signals there are generally
    more trustworthy than in low-liquidity windows (e.g. late NY / early
    Asia). This is a liquidity/quality filter, not a prediction.
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    h = dt.hour

    if 13 <= h < 16:
        return "London/New York overlap", 1.10
    elif 8 <= h < 16:
        return "London session", 1.0
    elif 13 <= h < 21:
        return "New York session", 1.0
    elif 0 <= h < 8:
        return "Asia session", 0.9
    else:
        return "Low-liquidity window", 0.8

# --- Adaptive weighting (learns from this bot's own resolved history) ---

_ADAPTIVE_WEIGHTS_CACHE = {"weights": {}, "computed_at": None}
_ADAPTIVE_LOCK = threading.Lock()
MIN_SAMPLES_FOR_ADAPTIVE = 8

def compute_adaptive_weights():
    """
    Looks at this bot's own resolved (WIN/LOSS) signals and computes, per
    scoring tag (e.g. EMA_TREND, MACD, CANDLE_PATTERN...), how often that
    tag was present in a winning signal vs a losing one. Tags with a better
    historical hit-rate get a slightly higher weight next time (max +40%),
    tags with a worse hit-rate get slightly discounted (max -40%). Tags with
    too few samples default to neutral (1.0x) so early/noisy data can't
    swing the system. This adapts to this bot's own track record only — it
    is not a claim about how these indicators perform in general.
    """
    import json as _json
    with DB_LOCK:
        con = db()
        rows = con.execute(
            "SELECT tags_json, result FROM signals WHERE result IN ('WIN','LOSS') AND tags_json IS NOT NULL"
        ).fetchall()
        con.close()

    tag_stats = {}  # tag -> [wins, total]
    for row in rows:
        try:
            tags = _json.loads(row["tags_json"])
        except Exception:
            continue
        for t in tags:
            w, tot = tag_stats.get(t, [0, 0])
            tot += 1
            if row["result"] == "WIN":
                w += 1
            tag_stats[t] = [w, tot]

    weights = {}
    for tag, (w, tot) in tag_stats.items():
        if tot < MIN_SAMPLES_FOR_ADAPTIVE:
            continue
        win_rate = w / tot
        weight = 0.6 + win_rate * 0.8  # win_rate 0.5 -> 1.0, 1.0 -> 1.4, 0.0 -> 0.6
        weights[tag] = round(max(0.6, min(1.4, weight)), 3)

    with _ADAPTIVE_LOCK:
        _ADAPTIVE_WEIGHTS_CACHE["weights"] = weights
        _ADAPTIVE_WEIGHTS_CACHE["computed_at"] = utc_now()
    return weights

def get_adaptive_weights():
    with _ADAPTIVE_LOCK:
        return dict(_ADAPTIVE_WEIGHTS_CACHE["weights"])

def compute_risk_reward(price, atr_v, direction, score):
    """
    Suggests an ATR-based stop-loss / take-profit pair — a standard
    risk-management technique (not a prediction). Stop distance = 1.5x ATR,
    target distance = 2.5x ATR, giving a ~1:1.67 reward:risk ratio. Only
    returned for UP/DOWN/WEAK UP/WEAK DOWN signals; WAIT has no suggestion
    since there's no directional bias to manage risk around.
    """
    if "UP" not in direction and "DOWN" not in direction:
        return None
    if atr_v <= 0:
        return None

    stop_dist = round(atr_v * 1.5, 8)
    target_dist = round(atr_v * 2.5, 8)

    if "UP" in direction:
        stop_loss = round(price - stop_dist, 8)
        take_profit = round(price + target_dist, 8)
    else:
        stop_loss = round(price + stop_dist, 8)
        take_profit = round(price - target_dist, 8)

    return {
        "entry": round(price, 8),
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "risk_distance": stop_dist,
        "reward_distance": target_dist,
        "reward_risk_ratio": round(target_dist / stop_dist, 2) if stop_dist else None,
    }

def make_signal(df: pd.DataFrame):
    close = df["close"]

    e9 = ema(close, 9)
    e21 = ema(close, 21)
    rv = rsi(close, 14)
    ml, ms = macd(close)
    bm, bu, bl = bollinger(close)
    ax = adx(df, 14)
    av = atr(df, 14)
    stoch_k, stoch_d = stochastic(df)
    vw, vw_has_volume = vwap(df)
    fib = fibonacci_levels(df)
    pattern_name, pattern_points = detect_candle_pattern(df)
    divergence = detect_divergence(df)
    sr = detect_support_resistance(df)
    session_name, session_mult = trading_session_info()

    i = len(df) - 1
    price = float(close.iloc[i])
    ema9_v = float(e9.iloc[i])
    ema21_v = float(e21.iloc[i])
    rsi_v = float(rv.iloc[i])
    macd_v = float(ml.iloc[i])
    macd_s = float(ms.iloc[i])
    adx_v = float(ax.iloc[i])
    atr_v = float(av.iloc[i]) if pd.notna(av.iloc[i]) else 0.0
    bbm = float(bm.iloc[i]) if pd.notna(bm.iloc[i]) else price
    stoch_k_v = float(stoch_k.iloc[i])
    stoch_d_v = float(stoch_d.iloc[i])
    vwap_v = float(vw.iloc[i]) if pd.notna(vw.iloc[i]) else price

    weights = get_adaptive_weights()
    score = 0.0
    reasons = []
    tags = []

    def add(tag, delta, text):
        nonlocal score
        w = weights.get(tag, 1.0)
        score += delta * w
        reasons.append(text if w == 1.0 else f"{text} (weight {w}x)")
        tags.append(tag)

    # Trend
    if ema9_v > ema21_v:
        add("EMA_TREND", 20, "EMA bullish")
    elif ema9_v < ema21_v:
        add("EMA_TREND", -20, "EMA bearish")

    # RSI: momentum confirmation, not blind overbought/oversold
    if 50 <= rsi_v <= 70:
        add("RSI_ZONE", 15, "RSI bullish zone")
    elif 30 <= rsi_v < 50:
        add("RSI_ZONE", -15, "RSI bearish zone")
    elif rsi_v > 75:
        add("RSI_EXTREME", -8, "RSI very high")
    elif rsi_v < 25:
        add("RSI_EXTREME", 8, "RSI very low")

    # MACD
    if macd_v > macd_s:
        add("MACD", 20, "MACD bullish")
    elif macd_v < macd_s:
        add("MACD", -20, "MACD bearish")

    # Bollinger midline
    if price > bbm:
        add("BB_MID", 10, "Above BB mid")
    elif price < bbm:
        add("BB_MID", -10, "Below BB mid")

    # ADX confirms trend strength, while DI direction is approximated by EMA/MACD
    if adx_v >= 25:
        if score > 0:
            add("ADX_CONFIRM", 10, "ADX trend confirmation")
        elif score < 0:
            add("ADX_CONFIRM", -10, "ADX trend confirmation")

    # Short candle momentum
    if len(df) >= 4:
        c1 = float(close.iloc[-1])
        c3 = float(close.iloc[-3])
        if c1 > c3:
            add("MOMENTUM", 10, "Short-term momentum up")
        elif c1 < c3:
            add("MOMENTUM", -10, "Short-term momentum down")

    # Stochastic oscillator: momentum confirmation similar to RSI but faster.
    if stoch_k_v > stoch_d_v and stoch_k_v < 80:
        add("STOCH_CROSS", 10, "Stochastic bullish crossover")
    elif stoch_k_v < stoch_d_v and stoch_k_v > 20:
        add("STOCH_CROSS", -10, "Stochastic bearish crossover")
    elif stoch_k_v >= 80:
        add("STOCH_EXTREME", -5, "Stochastic overbought")
    elif stoch_k_v <= 20:
        add("STOCH_EXTREME", 5, "Stochastic oversold")

    # VWAP: price above/below the volume-weighted average acts as a simple
    # institutional-style bias filter. Weighted only when real volume exists.
    if vw_has_volume:
        if price > vwap_v:
            add("VWAP", 8, "Above VWAP")
        elif price < vwap_v:
            add("VWAP", -8, "Below VWAP")
    else:
        reasons.append("VWAP unavailable (no real volume for this instrument)")

    # Candle pattern (price action)
    if pattern_name and pattern_points != 0:
        add("CANDLE_PATTERN", pattern_points, pattern_name)
    elif pattern_name:
        reasons.append(pattern_name)

    # Support/Resistance proximity — a reaction zone, not a forecast
    if sr:
        if sr["near_support"]:
            add("SR_ZONE", 12, f"Near support ~{sr['nearest_support']}")
        elif sr["near_resistance"]:
            add("SR_ZONE", -12, f"Near resistance ~{sr['nearest_resistance']}")

    # RSI/price divergence — an early momentum-fading warning, weighted
    # moderately since it's a leading (not confirming) signal.
    if divergence:
        if "Bullish" in divergence:
            add("DIVERGENCE", 14, divergence)
        elif "Bearish" in divergence:
            add("DIVERGENCE", -14, divergence)

    # ATR-based volatility check. This does not predict direction; it flags
    # when price is barely moving, which makes any signal (from any tool)
    # less meaningful because the "trend" may just be noise.
    atr_pct = (atr_v / price * 100) if price else 0.0
    if atr_pct < 0.03:
        score = score * 0.6
        reasons.append("Low volatility (ATR) - signal weakened")
    else:
        reasons.append(f"ATR volatility {round(atr_pct, 3)}%")

    # Trading session liquidity filter
    score = score * session_mult
    reasons.append(f"Session: {session_name} (x{session_mult})")

    score = int(max(-100, min(100, round(score))))

    # Granular direction label
    if score >= 70:
        direction = "UP"
    elif score >= 50:
        direction = "WEAK UP"
    elif score <= -70:
        direction = "DOWN"
    elif score <= -50:
        direction = "WEAK DOWN"
    else:
        direction = "WAIT"

    # This is a score-derived confidence, NOT a statistical probability.
    confidence = int(min(99, max(1, abs(score))))

    return {
        "direction": direction,
        "score": score,
        "confidence": confidence,
        "price": price,
        "timestamp": df["datetime"].iloc[-1].isoformat(),
        "rsi": round(rsi_v, 2),
        "ema9": round(ema9_v, 8),
        "ema21": round(ema21_v, 8),
        "macd": round(macd_v, 8),
        "macd_signal": round(macd_s, 8),
        "adx": round(adx_v, 2),
        "atr": round(atr_v, 8),
        "bb_mid": round(bbm, 8),
        "stoch_k": round(stoch_k_v, 2),
        "stoch_d": round(stoch_d_v, 2),
        "vwap": round(vwap_v, 8),
        "vwap_has_volume": vw_has_volume,
        "fibonacci": fib,
        "candle_pattern": pattern_name,
        "divergence": divergence,
        "support_resistance": sr,
        "session": session_name,
        "risk_reward": compute_risk_reward(price, atr_v, direction, score),
        "reasons": reasons,
        "tags": tags,
    }

def save_signal(symbol, interval, s):
    import json as _json
    with DB_LOCK:
        con = db()
        cur = con.execute("""
            INSERT INTO signals
            (symbol, interval, timestamp, price, direction, score, confidence,
             rsi, ema9, ema21, macd, macd_signal, adx, atr, bb_mid, stoch_k, stoch_d, vwap,
             tags_json, session)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, interval, s["timestamp"], s["price"], s["direction"],
            s["score"], s["confidence"], s["rsi"], s["ema9"], s["ema21"],
            s["macd"], s["macd_signal"], s["adx"], s.get("atr"), s["bb_mid"],
            s.get("stoch_k"), s.get("stoch_d"), s.get("vwap"),
            _json.dumps(s.get("tags", [])), s.get("session")
        ))
        con.commit()
        signal_id = cur.lastrowid
        con.close()
    return signal_id

def calculate_signal(symbol, interval):
    df = fetch_candles(symbol, interval)
    if len(df) < 60:
        raise RuntimeError("Not enough candles for reliable indicator calculation")
    s = make_signal(df)
    s["id"] = save_signal(normalize_symbol(symbol), normalize_interval(interval), s)
    return s

def multi_timeframe_signal(symbol):
    """
    Computes a signal independently on 1m, 5m and 15m, then checks whether
    they agree. Agreement across timeframes is a stronger (still not
    guaranteed) filter than any single timeframe alone, because it rules out
    signals that are just short-term noise on one chart.
    Does NOT save these to the history table (only /signal saves history),
    to keep the accuracy tracker meaning "signals someone actually acted on".
    """
    results = {}
    for tf in ["1min", "5min", "15min"]:
        try:
            df = fetch_candles(symbol, tf)
            if len(df) < 60:
                results[tf] = None
                continue
            results[tf] = make_signal(df)
        except Exception as e:
            results[tf] = None

    directions = [r["direction"] for r in results.values() if r is not None]
    up_count = sum(1 for d in directions if "UP" in d)
    down_count = sum(1 for d in directions if "DOWN" in d)

    if up_count >= 2 and down_count == 0:
        consensus = "UP"
    elif down_count >= 2 and up_count == 0:
        consensus = "DOWN"
    elif up_count and down_count:
        consensus = "CONFLICT"
    else:
        consensus = "WAIT"

    return {
        "symbol": normalize_symbol(symbol),
        "timeframes": results,
        "consensus": consensus,
        "agreement": f"{max(up_count, down_count)}/{len(directions)} timeframes agree" if directions else "no data",
    }

def scan_symbols(interval="5m", symbol_list=None):
    """
    Runs a signal on every symbol in symbol_list (defaults to all supported
    symbols) and returns them ranked by absolute score — i.e. "which pairs
    currently have the strongest, most one-sided technical read." Does not
    save these to signal history (only /signal saves history).
    """
    syms = symbol_list or list(SYMBOLS.values())
    results = []
    for sym in syms:
        try:
            df = fetch_candles(sym, interval)
            if len(df) < 60:
                continue
            s = make_signal(df)
            results.append({
                "symbol": normalize_symbol(sym),
                "direction": s["direction"],
                "score": s["score"],
                "confidence": s["confidence"],
            })
        except Exception:
            continue
    results.sort(key=lambda r: abs(r["score"]), reverse=True)
    return results

def compute_correlation(symbol_a, symbol_b, interval="5m", lookback=100):
    """
    Computes the Pearson correlation coefficient between two symbols' recent
    price returns. Close to +1 means they tend to move together, close to
    -1 means they tend to move opposite, close to 0 means little
    relationship. Useful for avoiding accidentally doubling up risk on two
    positions that are really the same bet in disguise. This is a
    statistical relationship over the recent past, not a forecast.
    """
    df_a = fetch_candles(symbol_a, interval, outputsize=lookback + 10)
    df_b = fetch_candles(symbol_b, interval, outputsize=lookback + 10)

    merged = pd.merge(
        df_a[["datetime", "close"]].rename(columns={"close": "a"}),
        df_b[["datetime", "close"]].rename(columns={"close": "b"}),
        on="datetime", how="inner"
    ).tail(lookback)

    if len(merged) < 20:
        raise RuntimeError("Not enough overlapping candles to compute correlation")

    ret_a = merged["a"].pct_change().dropna()
    ret_b = merged["b"].pct_change().dropna()
    n = min(len(ret_a), len(ret_b))
    corr = float(np.corrcoef(ret_a.tail(n), ret_b.tail(n))[0, 1])

    if corr >= 0.7:
        interp = "Strong positive — tend to move together"
    elif corr >= 0.3:
        interp = "Moderate positive"
    elif corr > -0.3:
        interp = "Weak/no relationship"
    elif corr > -0.7:
        interp = "Moderate negative"
    else:
        interp = "Strong negative — tend to move opposite"

    return {
        "symbol_a": normalize_symbol(symbol_a),
        "symbol_b": normalize_symbol(symbol_b),
        "interval": normalize_interval(interval),
        "correlation": round(corr, 3),
        "interpretation": interp,
        "sample_size": int(n),
    }

INTERVAL_MINUTES = {"1min": 1, "5min": 5, "15min": 15}

def resolve_pending_signals():
    """
    Grades old PENDING signals as WIN/LOSS/FLAT by comparing the price at
    signal time against the current price, once enough time has passed for
    that signal's timeframe. This is what makes /api/accuracy a real,
    backward-looking hit-rate instead of a made-up number. It still says
    nothing about future accuracy.
    """
    with DB_LOCK:
        con = db()
        pending = con.execute(
            "SELECT * FROM signals WHERE result = 'PENDING' AND direction != 'WAIT'"
        ).fetchall()
        con.close()

    if not pending:
        return 0

    resolved_count = 0
    cache = {}

    for row in pending:
        symbol, interval = row["symbol"], row["interval"]
        try:
            sig_time = datetime.fromisoformat(row["timestamp"])
        except Exception:
            continue
        if sig_time.tzinfo is None:
            sig_time = sig_time.replace(tzinfo=timezone.utc)

        minutes_needed = INTERVAL_MINUTES.get(interval, 1)
        age_minutes = (datetime.now(timezone.utc) - sig_time).total_seconds() / 60
        if age_minutes < minutes_needed:
            continue  # not enough time has passed yet to know the outcome

        key = (symbol, interval)
        try:
            if key not in cache:
                cache[key] = fetch_candles(symbol, interval, outputsize=100)
            df = cache[key]
        except Exception:
            continue

        future = df[df["datetime"] > sig_time]
        if future.empty:
            continue
        future_price = float(future.iloc[0]["close"])

        if "UP" in row["direction"]:
            result = "WIN" if future_price > row["price"] else "LOSS" if future_price < row["price"] else "FLAT"
        else:  # DOWN / WEAK DOWN
            result = "WIN" if future_price < row["price"] else "LOSS" if future_price > row["price"] else "FLAT"

        with DB_LOCK:
            con = db()
            con.execute("UPDATE signals SET result = ? WHERE id = ?", (result, row["id"]))
            con.commit()
            con.close()
        resolved_count += 1

    if resolved_count:
        try:
            compute_adaptive_weights()
        except Exception as e:
            print(f"Adaptive weight recompute failed: {e}")

    return resolved_count

def generate_chart_image(df, s, symbol, interval):
    """
    Renders a PNG (in-memory) — real OHLC candlesticks (not just a line),
    EMA9/21, Bollinger Bands, current-price marker, and a Stochastic
    sub-panel — styled like a trading-platform chart, for sending to
    Telegram as a photo. Returns None if matplotlib isn't installed.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.patches import Rectangle
        import io
    except Exception:
        return None

    tail = 60
    d = df.tail(tail).reset_index(drop=True)
    close = d["close"]
    e9 = ema(df["close"], 9).tail(tail).reset_index(drop=True)
    e21 = ema(df["close"], 21).tail(tail).reset_index(drop=True)
    bm, bu, bl = bollinger(df["close"])
    bm, bu, bl = bm.tail(tail).reset_index(drop=True), bu.tail(tail).reset_index(drop=True), bl.tail(tail).reset_index(drop=True)
    k, dd = stochastic(df)
    k, dd = k.tail(tail).reset_index(drop=True), dd.tail(tail).reset_index(drop=True)

    BG = "#0b1020"
    PANEL = "#0f1530"
    GRID = "#1e2540"
    TEXT = "#c7cbe0"
    UP_COLOR = "#22c55e"
    DOWN_COLOR = "#ef4444"

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(9, 6.5), gridspec_kw={"height_ratios": [3, 1]}, sharex=True,
        facecolor=BG
    )
    for ax in (ax1, ax2):
        ax.set_facecolor(PANEL)
        ax.grid(True, color=GRID, linewidth=0.6, alpha=0.7)
        ax.tick_params(colors=TEXT, labelsize=8)
        for spine in ax.spines.values():
            spine.set_color(GRID)
        ax.set_axisbelow(True)

    # --- Real candlesticks ---
    xpos = np.arange(len(d))
    body_width = 0.6
    for i in range(len(d)):
        o, h, l, c = d["open"].iloc[i], d["high"].iloc[i], d["low"].iloc[i], d["close"].iloc[i]
        color = UP_COLOR if c >= o else DOWN_COLOR
        ax1.plot([xpos[i], xpos[i]], [l, h], color=color, linewidth=0.9, zorder=2)
        y0, height = (o, c - o) if c >= o else (c, o - c)
        height = max(height, (h - l) * 0.01) or (h * 0.0001)
        ax1.add_patch(Rectangle(
            (xpos[i] - body_width / 2, y0), body_width, height,
            facecolor=color, edgecolor=color, linewidth=0.5, zorder=3
        ))

    ax1.plot(xpos, e9.values, color="#60a5fa", linewidth=1.3, label="EMA 9")
    ax1.plot(xpos, e21.values, color="#fbbf24", linewidth=1.3, label="EMA 21")
    ax1.plot(xpos, bu.values, color="#7a7f9c", linewidth=0.8, linestyle="--", label="BB Upper", alpha=0.8)
    ax1.plot(xpos, bl.values, color="#7a7f9c", linewidth=0.8, linestyle="--", label="BB Lower", alpha=0.8)
    ax1.fill_between(xpos, bu.values, bl.values, color="#3a63ff", alpha=0.04)

    # Current price marker + dashed line
    last_price = float(close.iloc[-1])
    dir_color = UP_COLOR if "UP" in s["direction"] else DOWN_COLOR if "DOWN" in s["direction"] else "#9ca3af"
    ax1.axhline(last_price, color=dir_color, linewidth=0.8, linestyle=":", alpha=0.8)
    ax1.scatter([xpos[-1]], [last_price], color=dir_color, s=45, zorder=5, edgecolor="#ffffff", linewidth=0.6)
    ax1.annotate(
        f"  {last_price:.5f}", (xpos[-1], last_price),
        color=dir_color, fontsize=9, fontweight="bold", va="center"
    )

    arrow = "▲" if "UP" in s["direction"] else "▼" if "DOWN" in s["direction"] else "•"
    ax1.set_title(
        f"{normalize_symbol(symbol)}   ·   {normalize_interval(interval)}   ·   "
        f"{arrow} {s['direction']}  (score {s['score']:+d})",
        color="#ffffff", fontsize=13, fontweight="bold", loc="left", pad=12
    )
    leg = ax1.legend(
        facecolor=PANEL, edgecolor=GRID, labelcolor=TEXT, fontsize=7.5,
        loc="upper left", framealpha=0.9, ncol=2
    )

    # --- Stochastic sub-panel ---
    ax2.plot(xpos, k.values, color="#a78bfa", linewidth=1.2, label="%K")
    ax2.plot(xpos, dd.values, color="#f472b6", linewidth=1.2, label="%D")
    ax2.axhline(80, color="#555b7a", linewidth=0.6, linestyle="--")
    ax2.axhline(20, color="#555b7a", linewidth=0.6, linestyle="--")
    ax2.fill_between(xpos, 80, 100, color=DOWN_COLOR, alpha=0.05)
    ax2.fill_between(xpos, 0, 20, color=UP_COLOR, alpha=0.05)
    ax2.set_ylim(0, 100)
    ax2.set_ylabel("Stoch", color=TEXT, fontsize=8)
    ax2.legend(facecolor=PANEL, edgecolor=GRID, labelcolor=TEXT, fontsize=7.5, loc="upper left", framealpha=0.9)

    # X-axis time labels (sampled to avoid crowding)
    step = max(len(d) // 8, 1)
    tick_pos = list(range(0, len(d), step))
    tick_labels = [d["datetime"].iloc[i].strftime("%H:%M") for i in tick_pos]
    ax2.set_xticks(tick_pos)
    ax2.set_xticklabels(tick_labels, rotation=0)
    ax1.set_xlim(-1, len(d))

    fig.text(
        0.5, 0.005,
        "Technical analysis only — not a prediction. No automatic trading.",
        ha="center", color="#6b7094", fontsize=7.5
    )
    plt.tight_layout(rect=[0, 0.02, 1, 1])

    buf = io.BytesIO()
    plt.savefig(buf, format="png", facecolor=fig.get_facecolor(), dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf

def format_signal(symbol, interval, s):
    arrow = "📈" if "UP" in s["direction"] else "📉" if "DOWN" in s["direction"] else "⚪"
    fib_line = ""
    if s.get("fibonacci"):
        f = s["fibonacci"]
        fib_line = (
            f"\nFib 0.382/0.5/0.618: {f['0.382']} / {f['0.5']} / {f['0.618']}\n"
        )
    sr_line = ""
    if s.get("support_resistance"):
        sr = s["support_resistance"]
        sr_line = f"Support: {sr.get('nearest_support')} | Resistance: {sr.get('nearest_resistance')}\n"
    pattern_line = f"Candle pattern: {s['candle_pattern']}\n" if s.get("candle_pattern") else ""
    divergence_line = f"⚠️ {s['divergence']}\n" if s.get("divergence") else ""
    rr_line = ""
    if s.get("risk_reward"):
        rr = s["risk_reward"]
        rr_line = (
            f"\n🎯 Reference levels (1.5x/2.5x ATR, not a prediction):\n"
            f"Entry: {rr['entry']} | Stop: {rr['stop_loss']} | Target: {rr['take_profit']}\n"
            f"Reward:Risk ≈ 1:{rr['reward_risk_ratio']}\n"
        )
    return (
        f"{arrow} MARKET SIGNAL\n\n"
        f"Asset: {normalize_symbol(symbol)}\n"
        f"Timeframe: {normalize_interval(interval)}\n"
        f"Session: {s.get('session', '?')}\n\n"
        f"Direction: {s['direction']}\n"
        f"Signal score: {s['score']:+d}\n"
        f"Score strength: {s['confidence']}/100\n\n"
        f"Price: {s['price']}\n"
        f"RSI: {s['rsi']}\n"
        f"EMA 9: {s['ema9']}\n"
        f"EMA 21: {s['ema21']}\n"
        f"MACD: {s['macd']}\n"
        f"ADX: {s['adx']}\n"
        f"ATR: {s['atr']}\n"
        f"Stochastic %K/%D: {s.get('stoch_k')}/{s.get('stoch_d')}\n"
        f"VWAP: {s.get('vwap')}"
        f"{' (no real volume, approximate)' if not s.get('vwap_has_volume') else ''}\n"
        f"{sr_line}"
        f"{pattern_line}"
        f"{divergence_line}"
        f"{fib_line}"
        f"{rr_line}\n"
        f"Reasons: {', '.join(s['reasons'][:8])}\n\n"
        "⚠️ Analysis only. No guarantee of future price movement. "
        "This bot does not place trades."
    )

def format_mtf(mtf):
    lines = [f"🧭 MULTI-TIMEFRAME CHECK: {mtf['symbol']}\n"]
    for tf_key, label in [("1min", "1m"), ("5min", "5m"), ("15min", "15m")]:
        r = mtf["timeframes"].get(tf_key)
        if r is None:
            lines.append(f"{label}: no data")
        else:
            arrow = "📈" if "UP" in r["direction"] else "📉" if "DOWN" in r["direction"] else "⚪"
            lines.append(f"{label}: {arrow} {r['direction']} (score {r['score']:+d})")
    lines.append(f"\nConsensus: {mtf['consensus']} ({mtf['agreement']})")
    lines.append(
        "\n⚠️ Agreement across timeframes is a stronger filter than one "
        "timeframe alone, but it is still not a guarantee."
    )
    return "\n".join(lines)

@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Market Signal Bot</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
body{font-family:Arial,sans-serif;background:#0b1020;color:#fff;margin:0;padding:20px}
.card{max-width:900px;margin:auto;background:#151c32;border-radius:18px;padding:24px;margin-bottom:16px}
h1{margin-top:0}
select,button{padding:12px;border-radius:10px;border:0;margin:4px}
button{cursor:pointer;background:#3a63ff;color:#fff;font-weight:bold}
button:disabled{opacity:.5;cursor:default}
#result{margin-top:20px;white-space:pre-wrap;background:#0d1427;padding:18px;border-radius:12px;font-family:monospace}
.small{opacity:.7;font-size:13px}
.badge{display:inline-block;padding:4px 12px;border-radius:20px;font-weight:bold}
.up{background:#0f5132;color:#75f0a0}
.down{background:#5c1a1a;color:#ff9b9b}
.wait{background:#3a3a3a;color:#ccc}
.warn{background:#2a1f0d;border:1px solid #7a5a1a;color:#ffcf7a;padding:12px;border-radius:10px;margin-bottom:16px;font-size:13px}
canvas{max-height:280px}
</style>
</head>
<body>
<div class="warn">
⚠️ Educational technical-analysis tool. Signals are derived from indicators (RSI, EMA, MACD, Bollinger Bands, ADX, ATR),
not a prediction of future price. Nobody can reliably predict the next candle. Use for learning, not as financial advice.
</div>

<div class="card">
<h1>⚡ Market Signal Bot</h1>
<p class="small">Independent technical-analysis dashboard. No automatic trading, no broker connection.</p>
<select id="symbol">
<option>EUR/USD</option><option>GBP/USD</option><option>USD/JPY</option>
<option>AUD/USD</option><option>USD/CAD</option><option>USD/CHF</option>
<option>NZD/USD</option><option>AUD/NZD</option><option>XAU/USD</option><option>BTC/USD</option>
<option>ETH/USD</option>
</select>
<select id="interval">
<option value="1min">1 minute</option>
<option value="5min" selected>5 minutes</option>
<option value="15min">15 minutes</option>
</select>
<button id="btn" onclick="getSignal()">Get Signal</button>
<div id="result">Choose an asset and click Get Signal.</div>
</div>

<div class="card">
<h2 style="margin-top:0">Price Chart</h2>
<canvas id="chart"></canvas>
</div>

<div class="card">
<h2 style="margin-top:0">Bot's Own Historical Accuracy</h2>
<p class="small">Calculated only from this bot's own past signals once they resolve. Not a promise about future signals.</p>
<div id="accuracy">Loading...</div>
</div>

<script>
let chartObj = null;

async function loadAssets(){
 try{
  const r = await fetch('/api/assets');
  const d = await r.json();
  const sel = document.getElementById('symbol');
  sel.innerHTML = '';
  d.assets.forEach(a=>{
   const o = document.createElement('option');
   o.value = a; o.textContent = a;
   if(a === 'AUD/NZD') o.selected = true;
   sel.appendChild(o);
  });
 }catch(e){ /* keep static fallback list */ }
}

async function loadAccuracy(){
 try{
  const r = await fetch('/api/accuracy');
  const d = await r.json();
  const box = document.getElementById('accuracy');
  if(d.accuracy_percent === null){
   box.textContent = 'No resolved signals yet. Signals resolve automatically after their timeframe passes.';
  } else {
   box.innerHTML = 'Resolved signals: <b>'+d.resolved+'</b><br>Wins: <b>'+d.wins+'</b><br>Historical hit-rate: <b>'+d.accuracy_percent+'%</b>';
  }
 }catch(e){ document.getElementById('accuracy').textContent = 'Could not load accuracy.'; }
}

function directionBadge(dir){
 const cls = dir.includes('UP') ? 'up' : dir.includes('DOWN') ? 'down' : 'wait';
 return '<span class="badge '+cls+'">'+dir+'</span>';
}

function renderChart(labels, prices){
 const ctx = document.getElementById('chart');
 if(chartObj) chartObj.destroy();
 const gradient = ctx.getContext('2d').createLinearGradient(0, 0, 0, 260);
 gradient.addColorStop(0, 'rgba(58,99,255,0.35)');
 gradient.addColorStop(1, 'rgba(58,99,255,0.02)');
 chartObj = new Chart(ctx, {
  type: 'line',
  data: { labels, datasets: [{
    label: 'Close price', data: prices,
    borderColor: '#5b8dff', backgroundColor: gradient, fill: true,
    tension: 0.35, pointRadius: 0, borderWidth: 2.2,
    pointHoverRadius: 5, pointHoverBackgroundColor: '#ffffff', pointHoverBorderColor: '#3a63ff'
  }]},
  options: {
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: { grid: { color: '#1e2540' }, ticks: { color: '#8b90ad', maxTicksLimit: 8 } },
      y: { grid: { color: '#1e2540' }, ticks: { color: '#8b90ad' } }
    },
    plugins: {
      legend: { labels: { color: '#e5e7f5' } },
      tooltip: { backgroundColor: '#151c32', titleColor: '#fff', bodyColor: '#c7cbe0', borderColor: '#2a3358', borderWidth: 1 }
    }
  }
 });
}

async function getSignal(){
 const s=document.getElementById('symbol').value;
 const i=document.getElementById('interval').value;
 const box=document.getElementById('result');
 const btn=document.getElementById('btn');
 btn.disabled = true;
 box.textContent='Loading...';
 try{
  const r=await fetch('/api/signal?symbol='+encodeURIComponent(s)+'&interval='+i);
  const d=await r.json();
  if(!r.ok) throw new Error(d.detail||'Request failed');
  box.innerHTML =
   directionBadge(d.direction)+' &nbsp; Score: '+d.score+' &nbsp; Strength: '+d.confidence+'/100<br><br>'+
   'Price: '+d.price+'\\n'+
   'RSI: '+d.rsi+'\\n'+
   'EMA 9: '+d.ema9+'\\n'+
   'EMA 21: '+d.ema21+'\\n'+
   'MACD: '+d.macd+'\\n'+
   'ADX: '+d.adx+'\\n'+
   'ATR: '+d.atr+'\\n'+
   'Stochastic %K/%D: '+d.stoch_k+'/'+d.stoch_d+'\\n'+
   'VWAP: '+d.vwap+'\\n\\n'+
   'Reasons: '+d.reasons.join(', ');

  const hr = await fetch('/api/history?limit=30');
  const hist = await hr.json();
  const filtered = hist.filter(x => x.symbol === d.symbol && x.interval === d.interval).reverse();
  if(filtered.length){
   renderChart(filtered.map(x=>x.timestamp.slice(11,16)), filtered.map(x=>x.price));
  }
  loadAccuracy();
 }catch(e){box.textContent='Error: '+e.message}
 finally{ btn.disabled = false; }
}

loadAssets();
loadAccuracy();
</script>
</body>
</html>
"""

@app.get("/health")
def health():
    return {"status": "ok", "time": utc_now()}

@app.get("/api/signal")
def api_signal(
    symbol: str = Query("EUR/USD"),
    interval: str = Query("1min")
):
    try:
        s = calculate_signal(symbol, interval)
        return {"symbol": normalize_symbol(symbol), "interval": normalize_interval(interval), **s}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/history")
def api_history(limit: int = Query(50, ge=1, le=500)):
    with DB_LOCK:
        con = db()
        rows = con.execute(
            "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        con.close()
    return [dict(r) for r in rows]

@app.get("/api/accuracy")
def api_accuracy():
    with DB_LOCK:
        con = db()
        total = con.execute(
            "SELECT COUNT(*) FROM signals WHERE result IN ('WIN','LOSS')"
        ).fetchone()[0]
        wins = con.execute(
            "SELECT COUNT(*) FROM signals WHERE result='WIN'"
        ).fetchone()[0]
        con.close()
    accuracy = round((wins / total) * 100, 2) if total else None
    return {
        "resolved": total,
        "wins": wins,
        "accuracy_percent": accuracy,
        "note": "Historical hit-rate of this bot's own past signals only. "
                "Not a guarantee of future performance.",
    }

@app.get("/api/assets")
def api_assets():
    return {"assets": list(SYMBOLS.values())}

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")

def fetch_economic_events(currencies=None, hours_ahead=48):
    """
    Pulls upcoming economic-calendar events (rate decisions, CPI, NFP, etc.)
    from Finnhub's free economic calendar endpoint. Requires a free
    FINNHUB_API_KEY (finnhub.io) — returns a clear message if not configured,
    rather than failing silently.
    """
    if not FINNHUB_API_KEY:
        return {"configured": False, "events": [],
                "message": "Set FINNHUB_API_KEY (free at finnhub.io) to enable news/economic alerts."}

    from datetime import timedelta
    today = datetime.now(timezone.utc).date()
    end = (datetime.now(timezone.utc) + timedelta(hours=hours_ahead)).date()

    try:
        r = requests.get(
            "https://finnhub.io/api/v1/calendar/economic",
            params={"from": str(today), "to": str(end), "token": FINNHUB_API_KEY},
            timeout=20,
        )
        if r.status_code == 403:
            return {"configured": False, "events": [],
                    "message": "Finnhub rejected the request (403). The economic "
                               "calendar endpoint may require a paid Finnhub plan "
                               "on your account, even though the API key itself is valid."}
        r.raise_for_status()
        data = r.json().get("economicCalendar", [])
    except requests.exceptions.RequestException as e:
        return {"configured": False, "events": [],
                "message": f"Could not reach Finnhub: {e}"}
    except Exception as e:
        return {"configured": False, "events": [],
                "message": f"Unexpected error fetching news: {e}"}

    if currencies:
        data = [e for e in data if e.get("country") in currencies]

    # Keep only medium/high impact to avoid noise
    data = [e for e in data if e.get("impact") in ("medium", "high")]
    data.sort(key=lambda e: e.get("time", ""))
    return {"configured": True, "events": data[:15]}

@app.get("/api/news")
def api_news():
    return fetch_economic_events()

@app.get("/api/mtf")
def api_mtf(symbol: str = Query("AUD/NZD")):
    try:
        return multi_timeframe_signal(symbol)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/scan")
def api_scan(interval: str = Query("5m")):
    try:
        return {"interval": normalize_interval(interval), "results": scan_symbols(interval)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/correlation")
def api_correlation(symbol_a: str = Query("AUD/NZD"), symbol_b: str = Query("EUR/USD"), interval: str = Query("5m")):
    try:
        return compute_correlation(symbol_a, symbol_b, interval)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/backtest_report")
def api_backtest_report():
    """
    Historical hit-rate broken down per symbol and per timeframe, computed
    only from this bot's own resolved past signals. This is a report on what
    already happened, not a forecast.
    """
    with DB_LOCK:
        con = db()
        rows = con.execute("""
            SELECT symbol, interval,
                   COUNT(*) as resolved,
                   SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                   SUM(CASE WHEN result='FLAT' THEN 1 ELSE 0 END) as flats
            FROM signals
            WHERE result IN ('WIN','LOSS','FLAT')
            GROUP BY symbol, interval
            ORDER BY symbol, interval
        """).fetchall()
        con.close()

    report = []
    for r in rows:
        resolved = r["resolved"]
        wins = r["wins"]
        accuracy = round((wins / resolved) * 100, 2) if resolved else None
        report.append({
            "symbol": r["symbol"],
            "interval": r["interval"],
            "resolved": resolved,
            "wins": wins,
            "losses": r["losses"],
            "flats": r["flats"],
            "accuracy_percent": accuracy,
        })

    return {
        "report": report,
        "note": "Historical only, grouped by symbol/timeframe. Small sample "
                "sizes are not statistically reliable — treat early numbers "
                "with caution.",
    }

# ---------------- Scheduler ----------------

WATCHLIST = [s.strip() for s in os.getenv("WATCHLIST", "").split(",") if s.strip()]
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "300"))
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

_telegram_app_ref = {"app": None}

async def scheduler_loop():
    """
    Periodically:
      1. Resolves outcomes for previous signals (real WIN/LOSS grading).
      2. Computes a fresh signal for every symbol in WATCHLIST.
      3. Optionally pushes non-WAIT signals to a configured Telegram chat.
    Does nothing (idles) if WATCHLIST is empty.
    """
    if not WATCHLIST:
        print("Scheduler idle: set WATCHLIST in .env to enable periodic scans.")
        return

    while True:
        try:
            resolved = await asyncio.to_thread(resolve_pending_signals)
            if resolved:
                print(f"Scheduler: resolved {resolved} past signal(s).")
        except Exception as e:
            print(f"Scheduler resolve error: {e}")

        for raw_symbol in WATCHLIST:
            try:
                df = await asyncio.to_thread(fetch_candles, raw_symbol, "5m")
                s = make_signal(df)
                s["id"] = await asyncio.to_thread(save_signal, normalize_symbol(raw_symbol), "5min", s)
                print(f"Scheduler: {normalize_symbol(raw_symbol)} -> {s['direction']} ({s['confidence']})")

                tg_app = _telegram_app_ref["app"]
                if tg_app and TELEGRAM_CHAT_ID and s["direction"] != "WAIT":
                    await tg_app.bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=format_signal(raw_symbol, "5m", s),
                    )
                    chart = await asyncio.to_thread(generate_chart_image, df, s, raw_symbol, "5m")
                    if chart:
                        await tg_app.bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=chart)
            except Exception as e:
                print(f"Scheduler error for {raw_symbol}: {e}")

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

def start_scheduler_thread():
    def runner():
        asyncio.run(scheduler_loop())
    t = threading.Thread(target=runner, daemon=True)
    t.start()

# ---------------- Telegram ----------------

def parse_command_args(args):
    if not args:
        return None, None
    symbol = normalize_symbol(args[0])
    interval = normalize_interval(args[1] if len(args) > 1 else "1m")
    return symbol, interval

async def tg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚡ Market Signal Bot\n\n"
        "Commands:\n"
        "/signal EURUSD 1m — signal + chart\n"
        "/mtf AUDNZD — 1m+5m+15m agreement check\n"
        "/history — last 10 signals\n"
        "/accuracy — overall hit-rate\n"
        "/backtest — hit-rate per symbol/timeframe\n"
        "/weights — see adaptive indicator weights\n"
        "/scan 5m — rank all pairs by signal strength\n"
        "/corr AUDNZD EURUSD 5m — correlation between two pairs\n"
        "/news — upcoming economic events\n"
        "/help\n\n"
        "Analysis only — no automatic trading."
    )

async def tg_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await tg_start(update, context)

async def tg_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        symbol, interval = parse_command_args(context.args)
        if not symbol:
            await update.message.reply_text("Example: /signal EURUSD 1m")
            return

        await update.message.reply_text("⏳ Analyzing latest market candles...")
        df = await asyncio.to_thread(fetch_candles, symbol, interval)
        s = make_signal(df)
        s["id"] = await asyncio.to_thread(save_signal, normalize_symbol(symbol), normalize_interval(interval), s)
        await update.message.reply_text(format_signal(symbol, interval, s))

        chart = await asyncio.to_thread(generate_chart_image, df, s, symbol, interval)
        if chart:
            await update.message.reply_photo(photo=chart)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def tg_mtf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args:
            await update.message.reply_text("Example: /mtf AUDNZD")
            return
        symbol = context.args[0]
        await update.message.reply_text("⏳ Checking 1m / 5m / 15m together...")
        mtf = await asyncio.to_thread(multi_timeframe_signal, symbol)
        await update.message.reply_text(format_mtf(mtf))
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def tg_backtest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = api_backtest_report()
        report = data["report"]
        if not report:
            await update.message.reply_text("No resolved signals yet to report on.")
            return
        lines = ["📊 BACKTEST REPORT (this bot's own history)\n"]
        for row in report:
            acc = row["accuracy_percent"]
            acc_str = f"{acc}%" if acc is not None else "N/A"
            lines.append(
                f"{row['symbol']} {row['interval']}: {row['wins']}W/{row['losses']}L/{row['flats']}F "
                f"— {acc_str}"
            )
        lines.append(f"\n{data['note']}")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def tg_weights(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        weights = get_adaptive_weights()
        if not weights:
            await update.message.reply_text(
                f"ℹ️ No adaptive weights yet — need at least {MIN_SAMPLES_FOR_ADAPTIVE} "
                "resolved signals per indicator before this bot starts adjusting weights "
                "based on its own track record. Until then, every indicator counts equally (1.0x)."
            )
            return
        lines = ["⚖️ ADAPTIVE WEIGHTS (from this bot's own history)\n"]
        for tag, w in sorted(weights.items(), key=lambda x: -x[1]):
            note = "boosted" if w > 1.0 else "discounted" if w < 1.0 else "neutral"
            lines.append(f"{tag}: {w}x ({note})")
        lines.append(
            "\n⚠️ These weights reflect only this bot's own past signals so far "
            "and will keep shifting as more signals resolve. Not a guarantee."
        )
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def tg_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = fetch_economic_events()
        if not data["configured"]:
            await update.message.reply_text(f"ℹ️ {data['message']}")
            return
        events = data["events"]
        if not events:
            await update.message.reply_text("No medium/high-impact events found in the next 48 hours.")
            return
        lines = ["📰 UPCOMING ECONOMIC EVENTS (next 48h)\n"]
        for e in events[:10]:
            lines.append(
                f"{e.get('time','?')} | {e.get('country','?')} | {e.get('event','?')} "
                f"({e.get('impact','?')})"
            )
        lines.append("\n⚠️ High-impact events can cause sudden volatility that overrides any technical signal.")
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Error fetching news: {e}")

async def tg_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        interval = context.args[0] if context.args else "5m"
        await update.message.reply_text(f"⏳ Scanning all pairs on {interval}...")
        results = await asyncio.to_thread(scan_symbols, interval)
        if not results:
            await update.message.reply_text("No results — check TWELVE_DATA_API_KEY / rate limits.")
            return
        lines = [f"🔍 MULTI-SYMBOL SCAN ({interval}) — sorted by signal strength\n"]
        for r in results[:11]:
            arrow = "📈" if "UP" in r["direction"] else "📉" if "DOWN" in r["direction"] else "⚪"
            lines.append(f"{arrow} {r['symbol']}: {r['direction']} (score {r['score']:+d})")
        lines.append(
            "\n⚠️ Ranked by how strongly current indicators agree right now — "
            "not by which will move most, and not a prediction."
        )
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def tg_corr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if len(context.args) < 2:
            await update.message.reply_text("Example: /corr AUDNZD EURUSD 5m")
            return
        sym_a, sym_b = context.args[0], context.args[1]
        interval = context.args[2] if len(context.args) > 2 else "5m"
        await update.message.reply_text(f"⏳ Computing correlation {sym_a} vs {sym_b}...")
        data = await asyncio.to_thread(compute_correlation, sym_a, sym_b, interval)
        await update.message.reply_text(
            f"🔗 CORRELATION: {data['symbol_a']} vs {data['symbol_b']} ({data['interval']})\n\n"
            f"Coefficient: {data['correlation']}\n"
            f"Interpretation: {data['interpretation']}\n"
            f"Sample size: {data['sample_size']} candles\n\n"
            "⚠️ Reflects the recent past only — correlations shift over time, "
            "this is not a forecast of future relationship."
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def tg_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with DB_LOCK:
        con = db()
        rows = con.execute("""
            SELECT symbol, interval, direction, score, timestamp
            FROM signals ORDER BY id DESC LIMIT 10
        """).fetchall()
        con.close()

    if not rows:
        await update.message.reply_text("No signals yet.")
        return

    text = "📊 LAST 10 SIGNALS\n\n"
    for r in rows:
        text += (
            f"{r['symbol']} | {r['interval']} | "
            f"{r['direction']} | {r['score']:+d}\n"
        )
    await update.message.reply_text(text)

async def tg_accuracy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = api_accuracy()
    if data["accuracy_percent"] is None:
        await update.message.reply_text("No resolved signals yet.")
    else:
        await update.message.reply_text(
            f"📊 Resolved: {data['resolved']}\n"
            f"WIN: {data['wins']}\n"
            f"Accuracy: {data['accuracy_percent']}%\n\n"
            "Accuracy is historical and does not guarantee future results."
        )

async def run_telegram():
    if not TELEGRAM_BOT_TOKEN or Application is None:
        return

    tg_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", tg_start))
    tg_app.add_handler(CommandHandler("help", tg_help))
    tg_app.add_handler(CommandHandler("signal", tg_signal))
    tg_app.add_handler(CommandHandler("history", tg_history))
    tg_app.add_handler(CommandHandler("accuracy", tg_accuracy))
    tg_app.add_handler(CommandHandler("mtf", tg_mtf))
    tg_app.add_handler(CommandHandler("backtest", tg_backtest))
    tg_app.add_handler(CommandHandler("weights", tg_weights))
    tg_app.add_handler(CommandHandler("news", tg_news))
    tg_app.add_handler(CommandHandler("scan", tg_scan))
    tg_app.add_handler(CommandHandler("corr", tg_corr))

    _telegram_app_ref["app"] = tg_app

    await tg_app.initialize()
    await tg_app.start()
    await tg_app.updater.start_polling()

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await tg_app.updater.stop()
        await tg_app.stop()
        await tg_app.shutdown()

def start_telegram_thread():
    if not TELEGRAM_BOT_TOKEN:
        print("Telegram disabled: TELEGRAM_BOT_TOKEN not configured.")
        return
    def runner():
        asyncio.run(run_telegram())
    t = threading.Thread(target=runner, daemon=True)
    t.start()

if __name__ == "__main__":
    try:
        compute_adaptive_weights()
    except Exception as e:
        print(f"Initial adaptive weight computation skipped: {e}")
    start_telegram_thread()
    start_scheduler_thread()
    uvicorn.run(app, host=HOST, port=PORT)
