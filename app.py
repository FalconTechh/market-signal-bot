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
from datetime import datetime, timezone, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
import uvicorn

try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application, CommandHandler, CallbackQueryHandler, ContextTypes,
        MessageHandler, filters
    )
except Exception:
    Update = None
    InlineKeyboardButton = None
    InlineKeyboardMarkup = None
    Application = None
    CommandHandler = None
    CallbackQueryHandler = None
    ContextTypes = None
    MessageHandler = None
    filters = None

load_dotenv()

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
DB_FILE = os.getenv("DB_FILE", "signals.db")

app = FastAPI(title="Market Signal Bot", version="1.0.0")

DB_LOCK = threading.RLock()

# --- Subscription / payment config ---
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "")  # bot owner's Telegram user ID (for /approve)
FREE_TRIAL_LIMIT = 3
PRICE_INR = 299
PRICE_AED = 29
UPI_NUMBER = "6361472511"       # PhonePe / GPay / Paytm (India)
BOTIM_NUMBER = "0522445121"     # BOTIM Pay (UAE)

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
    # Premium-only priority symbols
    "GBPJPY": "GBP/JPY",
    "EURJPY": "EUR/JPY",
    "EURGBP": "EUR/GBP",
    "XAGUSD": "XAG/USD",
    "SOLUSD": "SOL/USD",
}

PREMIUM_ONLY_KEYS = {"GBPJPY", "EURJPY", "EURGBP", "XAGUSD", "SOLUSD"}

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

        con.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                telegram_id TEXT PRIMARY KEY,
                username TEXT,
                trial_used INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 0,
                expires_at TEXT,
                order_ref TEXT,
                created_at TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS watchlist_items (
                telegram_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                added_at TEXT,
                last_alert_at TEXT,
                PRIMARY KEY (telegram_id, symbol)
            )
        """)
        con.commit()
        con.close()

init_db()

import random
import string

def _gen_order_ref():
    return "MSB-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))

def get_or_create_user(telegram_id, username=None):
    telegram_id = str(telegram_id)
    with DB_LOCK:
        con = db()
        row = con.execute("SELECT * FROM subscriptions WHERE telegram_id = ?", (telegram_id,)).fetchone()
        if row is None:
            order_ref = _gen_order_ref()
            con.execute(
                "INSERT INTO subscriptions (telegram_id, username, trial_used, is_active, order_ref, created_at) "
                "VALUES (?, ?, 0, 0, ?, ?)",
                (telegram_id, username, order_ref, utc_now())
            )
            con.commit()
            row = con.execute("SELECT * FROM subscriptions WHERE telegram_id = ?", (telegram_id,)).fetchone()
        con.close()
    return row

def has_access(telegram_id, username=None):
    """
    Returns (allowed: bool, reason: str, trial_remaining: int).
    Access is granted if the subscription is active and not expired, OR
    if the user still has free trial signals remaining.
    """
    row = get_or_create_user(telegram_id, username)
    now = datetime.now(timezone.utc)

    if row["is_active"]:
        if row["expires_at"]:
            try:
                exp = datetime.fromisoformat(row["expires_at"])
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                if now < exp:
                    return True, "subscribed", 0
            except Exception:
                pass
        else:
            return True, "subscribed", 0

    trial_used = row["trial_used"] or 0
    remaining = max(0, FREE_TRIAL_LIMIT - trial_used)
    if remaining > 0:
        return True, "trial", remaining
    return False, "expired", 0

def consume_trial(telegram_id):
    telegram_id = str(telegram_id)
    with DB_LOCK:
        con = db()
        con.execute(
            "UPDATE subscriptions SET trial_used = trial_used + 1 WHERE telegram_id = ?",
            (telegram_id,)
        )
        con.commit()
        con.close()

def activate_subscription(telegram_id, days=30):
    telegram_id = str(telegram_id)
    expires = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    with DB_LOCK:
        con = db()
        get_or_create_user(telegram_id)
        con.execute(
            "UPDATE subscriptions SET is_active = 1, expires_at = ? WHERE telegram_id = ?",
            (expires, telegram_id)
        )
        con.commit()
        con.close()
    return expires

# --- Personal watchlist (premium feature) ---

MAX_WATCHLIST_SIZE = 5
PREMIUM_ALERT_THRESHOLD = 80  # |score| must reach this for a premium push alert
PREMIUM_ALERT_COOLDOWN_MINUTES = 15  # don't re-alert same symbol/user more often than this

def get_watchlist(telegram_id):
    telegram_id = str(telegram_id)
    with DB_LOCK:
        con = db()
        rows = con.execute(
            "SELECT symbol, last_alert_at FROM watchlist_items WHERE telegram_id = ? ORDER BY added_at",
            (telegram_id,)
        ).fetchall()
        con.close()
    return rows

def add_to_watchlist(telegram_id, symbol):
    telegram_id = str(telegram_id)
    symbol = normalize_symbol(symbol)
    current = get_watchlist(telegram_id)
    if len(current) >= MAX_WATCHLIST_SIZE:
        return False, f"Watchlist full (max {MAX_WATCHLIST_SIZE}). Remove one first with /delwatch."
    if any(r["symbol"] == symbol for r in current):
        return False, f"{symbol} is already in your watchlist."
    with DB_LOCK:
        con = db()
        con.execute(
            "INSERT OR IGNORE INTO watchlist_items (telegram_id, symbol, added_at) VALUES (?, ?, ?)",
            (telegram_id, symbol, utc_now())
        )
        con.commit()
        con.close()
    return True, f"{symbol} added to your watchlist."

def remove_from_watchlist(telegram_id, symbol):
    telegram_id = str(telegram_id)
    symbol = normalize_symbol(symbol)
    with DB_LOCK:
        con = db()
        cur = con.execute(
            "DELETE FROM watchlist_items WHERE telegram_id = ? AND symbol = ?",
            (telegram_id, symbol)
        )
        con.commit()
        removed = cur.rowcount > 0
        con.close()
    return removed

def mark_watchlist_alerted(telegram_id, symbol):
    with DB_LOCK:
        con = db()
        con.execute(
            "UPDATE watchlist_items SET last_alert_at = ? WHERE telegram_id = ? AND symbol = ?",
            (utc_now(), str(telegram_id), symbol)
        )
        con.commit()
        con.close()

def get_all_active_premium_users():
    """Returns telegram_ids of users with a currently active, non-expired subscription."""
    now = datetime.now(timezone.utc)
    with DB_LOCK:
        con = db()
        rows = con.execute(
            "SELECT telegram_id, expires_at FROM subscriptions WHERE is_active = 1"
        ).fetchall()
        con.close()
    active = []
    for r in rows:
        if not r["expires_at"]:
            continue
        try:
            exp = datetime.fromisoformat(r["expires_at"])
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if now < exp:
                active.append(r["telegram_id"])
        except Exception:
            continue
    return active

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

def generate_daily_report_pdf():
    """
    Builds a one-page PDF summarizing the last 20 signals (table) plus the
    overall accuracy breakdown, using matplotlib's built-in PDF backend (no
    extra dependency needed). Returns an in-memory buffer, or None if there
    is no signal history yet.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import io
    except Exception:
        return None

    with DB_LOCK:
        con = db()
        rows = con.execute("""
            SELECT symbol, interval, direction, score, confidence, timestamp, result
            FROM signals ORDER BY id DESC LIMIT 20
        """).fetchall()
        con.close()

    if not rows:
        return None

    back_data = api_backtest_report()

    fig = plt.figure(figsize=(8.5, 11))
    fig.patch.set_facecolor("white")

    fig.text(0.5, 0.96, "Market Signal Bot — Report", ha="center", fontsize=18, fontweight="bold")
    fig.text(0.5, 0.935, datetime.now(timezone.utc).strftime("Generated %Y-%m-%d %H:%M UTC"),
              ha="center", fontsize=9, color="#555")

    table_data = [["Symbol", "TF", "Direction", "Score", "Result", "Time"]]
    for r in rows:
        ts = r["timestamp"][:16].replace("T", " ") if r["timestamp"] else ""
        table_data.append([r["symbol"], r["interval"], r["direction"], f"{r['score']:+d}", r["result"], ts])

    ax1 = fig.add_axes([0.05, 0.55, 0.9, 0.35])
    ax1.axis("off")
    tbl = ax1.table(cellText=table_data, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7)
    tbl.scale(1, 1.4)
    for j in range(len(table_data[0])):
        tbl[0, j].set_facecolor("#1e2540")
        tbl[0, j].set_text_props(color="white", fontweight="bold")

    ax2 = fig.add_axes([0.05, 0.08, 0.9, 0.4])
    ax2.axis("off")
    ax2.text(0, 1.0, "Backtest Summary (by symbol/timeframe)", fontsize=12, fontweight="bold")
    y = 0.92
    for row in back_data["report"][:15]:
        acc = row["accuracy_percent"]
        acc_str = f"{acc}%" if acc is not None else "N/A"
        ax2.text(0, y, f"{row['symbol']} {row['interval']}: {row['wins']}W/{row['losses']}L/{row['flats']}F — {acc_str}", fontsize=9)
        y -= 0.055
    ax2.text(
        0, max(y - 0.03, 0.02),
        "Historical data only. Not a guarantee of future performance.\n"
        "This report does not constitute financial advice.",
        fontsize=8, color="#777", style="italic"
    )

    buf = io.BytesIO()
    plt.savefig(buf, format="pdf")
    plt.close(fig)
    buf.seek(0)
    return buf

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
PREMIUM_SCAN_INTERVAL_SECONDS = int(os.getenv("PREMIUM_SCAN_INTERVAL_SECONDS", "60"))
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

async def premium_watchlist_loop():
    """
    Runs more frequently than the base scheduler (default every 60s — the
    'faster scanning' premium perk). For every user with an active
    subscription, checks their personal watchlist and pushes a Telegram
    alert (with chart) ONLY when the signal score reaches
    PREMIUM_ALERT_THRESHOLD — i.e. only strong, high-conviction reads, not
    every minor wobble. Each symbol/user pair has its own cooldown so the
    same setup doesn't spam repeatedly.
    """
    while True:
        try:
            active_users = await asyncio.to_thread(get_all_active_premium_users)
            for uid in active_users:
                watch = await asyncio.to_thread(get_watchlist, uid)
                for item in watch:
                    symbol = item["symbol"]
                    last_alert = item["last_alert_at"]
                    if last_alert:
                        try:
                            last_dt = datetime.fromisoformat(last_alert)
                            if last_dt.tzinfo is None:
                                last_dt = last_dt.replace(tzinfo=timezone.utc)
                            if (datetime.now(timezone.utc) - last_dt).total_seconds() < PREMIUM_ALERT_COOLDOWN_MINUTES * 60:
                                continue
                        except Exception:
                            pass
                    try:
                        df = await asyncio.to_thread(fetch_candles, symbol, "1min")
                        s = make_signal(df)
                        if abs(s["score"]) >= PREMIUM_ALERT_THRESHOLD:
                            tg_app = _telegram_app_ref["app"]
                            if tg_app:
                                await tg_app.bot.send_message(
                                    chat_id=uid,
                                    text="🔔 *HIGH-CONFIDENCE ALERT*\n\n" + format_signal(symbol, "1m", s),
                                    parse_mode="Markdown",
                                )
                                chart = await asyncio.to_thread(generate_chart_image, df, s, symbol, "1m")
                                if chart:
                                    await tg_app.bot.send_photo(chat_id=uid, photo=chart)
                            await asyncio.to_thread(mark_watchlist_alerted, uid, symbol)
                    except Exception as e:
                        print(f"Premium watchlist error {uid}/{symbol}: {e}")
        except Exception as e:
            print(f"Premium watchlist loop error: {e}")

        await asyncio.sleep(PREMIUM_SCAN_INTERVAL_SECONDS)

def start_premium_watchlist_thread():
    def runner():
        asyncio.run(premium_watchlist_loop())
    t = threading.Thread(target=runner, daemon=True)
    t.start()

# ---------------- Telegram ----------------

def parse_command_args(args):
    if not args:
        return None, None
    symbol = normalize_symbol(args[0])
    interval = normalize_interval(args[1] if len(args) > 1 else "1m")
    return symbol, interval

MAJOR_SYMBOL_KEYS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD", "AUDNZD", "XAUUSD", "BTCUSD", "ETHUSD"]

def build_main_menu():
    rows = [
        [InlineKeyboardButton("📊 Get Signal", callback_data="m:sig"),
         InlineKeyboardButton("🧭 Multi-Timeframe", callback_data="m:mtf")],
        [InlineKeyboardButton("🔍 Scan All Pairs", callback_data="m:scan"),
         InlineKeyboardButton("🔗 Correlation", callback_data="m:corr1")],
        [InlineKeyboardButton("📜 History", callback_data="m:hist"),
         InlineKeyboardButton("🎯 Accuracy", callback_data="m:acc")],
        [InlineKeyboardButton("📈 Backtest", callback_data="m:back"),
         InlineKeyboardButton("⚖️ Weights", callback_data="m:wt")],
        [InlineKeyboardButton("📰 News", callback_data="m:news"),
         InlineKeyboardButton("ℹ️ Help", callback_data="m:help")],
        [InlineKeyboardButton("💎 Subscribe / My Status", callback_data="m:subscribe")],
    ]
    return InlineKeyboardMarkup(rows)

def build_symbol_menu(prefix, exclude=None):
    keys = [k for k in MAJOR_SYMBOL_KEYS if k != exclude]
    rows = []
    row = []
    for i, k in enumerate(keys):
        row.append(InlineKeyboardButton(SYMBOLS[k], callback_data=f"{prefix}:{k}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("« Back to menu", callback_data="m:main")])
    return InlineKeyboardMarkup(rows)

def build_timeframe_menu(prefix, sym):
    rows = [[
        InlineKeyboardButton("1m", callback_data=f"{prefix}:{sym}:1m"),
        InlineKeyboardButton("5m", callback_data=f"{prefix}:{sym}:5m"),
        InlineKeyboardButton("15m", callback_data=f"{prefix}:{sym}:15m"),
    ], [InlineKeyboardButton("« Back to menu", callback_data="m:main")]]
    return InlineKeyboardMarkup(rows)

def build_scan_timeframe_menu():
    rows = [[
        InlineKeyboardButton("1m", callback_data="scan:1m"),
        InlineKeyboardButton("5m", callback_data="scan:5m"),
        InlineKeyboardButton("15m", callback_data="scan:15m"),
    ], [InlineKeyboardButton("« Back to menu", callback_data="m:main")]]
    return InlineKeyboardMarkup(rows)

def build_back_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Back to menu", callback_data="m:main")]])

def build_payment_card(order_ref):
    return (
        "╔═══════════════════════╗\n"
        "   💎 *PREMIUM ACCESS* 💎\n"
        "╚═══════════════════════╝\n\n"
        "*Market Signal Bot — Monthly Plan*\n"
        "_Unlimited signals, /mtf, /scan, /corr, charts_\n"
        "_+ personal watchlist alerts, priority symbols, PDF reports_\n\n"
        "──────────────────────\n"
        f"🇮🇳 *India — UPI*\n"
        f"PhonePe / GPay / Paytm\n"
        f"`{UPI_NUMBER}`\n"
        f"Amount: *₹{PRICE_INR}/month*\n\n"
        f"🇦🇪 *UAE — BOTIM Pay*\n"
        f"`{BOTIM_NUMBER}`\n"
        f"Amount: *AED {PRICE_AED}/month*\n"
        "──────────────────────\n\n"
        f"🧾 Your order reference:\n`{order_ref}`\n\n"
        "*How to activate:*\n"
        "1️⃣ Pay the amount above via UPI or BOTIM Pay\n"
        "2️⃣ Screenshot the payment\n"
        "3️⃣ Send the screenshot *+ your order reference* "
        "to the bot owner for approval\n"
        "4️⃣ You'll be upgraded within a short time\n\n"
        "_Payments are verified manually — this bot does not "
        "auto-charge you or store card details._"
    )

def build_subscribe_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Check My Status", callback_data="m:status")],
        [InlineKeyboardButton("« Back to menu", callback_data="m:main")],
    ])

def access_denied_text(trial_used_msg=False):
    return (
        "🔒 *Free trial used up*\n\n"
        f"You've used all {FREE_TRIAL_LIMIT} free signals. "
        "Tap below to see subscription options and keep using the bot."
    )

MENU_INTRO = (
    "⚡ *Market Signal Bot*\n\n"
    "Tap a button below — no need to type commands.\n\n"
    "_Analysis only — no automatic trading. Nothing here is a guaranteed "
    "prediction of future price._"
)

async def gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Call this before running any signal-generating command. Returns True if
    the user may proceed (consuming one trial credit if they're on trial),
    or sends the paywall message and returns False if their access is used
    up. Menu navigation itself stays free — this only gates at the point of
    actually pulling a signal/scan/correlation.
    """
    user = update.effective_user
    allowed, reason, remaining = has_access(user.id, user.username)
    if not allowed:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=access_denied_text(),
            parse_mode="Markdown",
            reply_markup=build_subscribe_menu(),
        )
        return False
    if reason == "trial":
        consume_trial(user.id)
    return True

async def tg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        MENU_INTRO, parse_mode="Markdown", reply_markup=build_main_menu()
    )

async def tg_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await tg_start(update, context)

async def tg_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        symbol, interval = parse_command_args(context.args)
        if not symbol:
            await update.message.reply_text("Example: /signal EURUSD 1m")
            return
        if not await gate(update, context):
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
        if not await gate(update, context):
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
        if not await gate(update, context):
            return
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

async def tg_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    row = get_or_create_user(user.id, user.username)
    await update.message.reply_text(
        build_payment_card(row["order_ref"]),
        parse_mode="Markdown",
        reply_markup=build_subscribe_menu(),
    )
    if ADMIN_TELEGRAM_ID and str(user.id) != str(ADMIN_TELEGRAM_ID):
        try:
            await context.bot.send_message(
                chat_id=ADMIN_TELEGRAM_ID,
                text=(
                    f"💰 New subscribe request\n"
                    f"User: @{user.username or user.id} (id: {user.id})\n"
                    f"Order ref: {row['order_ref']}\n\n"
                    f"Once payment is confirmed, run:\n/approve {user.id} 30"
                ),
            )
        except Exception:
            pass

def build_status_text(reason, remaining, row):
    if reason == "subscribed":
        exp = row["expires_at"]
        exp_date = exp[:10] if exp else "N/A"
        return (
            "╔═══════════════════╗\n"
            "   👑 *PREMIUM MEMBER* 👑\n"
            "╚═══════════════════╝\n\n"
            "✅ *Status:* Active\n"
            f"📅 *Valid until:* {exp_date}\n\n"
            "_Unlimited signals, /mtf, /scan, /corr, and charts unlocked._"
        )
    elif reason == "trial":
        return (
            "🆓 *Free Trial*\n\n"
            f"Signals remaining: *{remaining}*\n\n"
            "_Upgrade anytime to keep unlimited access after your trial ends._"
        )
    else:
        return (
            "🔒 *Free trial used up*\n\n"
            "Tap Subscribe below to unlock unlimited access."
        )

async def tg_mystatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    allowed, reason, remaining = has_access(user.id, user.username)
    row = get_or_create_user(user.id)
    txt = build_status_text(reason, remaining, row)
    await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=build_subscribe_menu())

async def tg_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not ADMIN_TELEGRAM_ID or str(user.id) != str(ADMIN_TELEGRAM_ID):
        await update.message.reply_text("❌ This command is for the bot owner only.")
        return
    if len(context.args) < 1:
        await update.message.reply_text("Example: /approve 123456789 30")
        return
    target_id = context.args[0]
    days = int(context.args[1]) if len(context.args) > 1 else 30
    expires = activate_subscription(target_id, days)
    await update.message.reply_text(f"✅ Approved user {target_id} for {days} days (until {expires[:10]}).")
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=f"🎉 Your subscription is now active for {days} days! Enjoy unlimited access.",
        )
    except Exception:
        pass

def require_premium_text():
    return (
        "🔒 *This is a Premium feature*\n\n"
        "Your personal watchlist with high-confidence push alerts, priority "
        "symbols, and PDF reports are part of the paid plan.\n\n"
        "Tap Subscribe to unlock."
    )

async def tg_addwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    allowed, reason, remaining = has_access(user.id, user.username)
    if reason != "subscribed":
        await update.message.reply_text(require_premium_text(), parse_mode="Markdown", reply_markup=build_subscribe_menu())
        return
    if not context.args:
        await update.message.reply_text("Example: /addwatch GBPJPY")
        return
    ok, msg = add_to_watchlist(user.id, context.args[0])
    await update.message.reply_text(("✅ " if ok else "⚠️ ") + msg)

async def tg_mywatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    allowed, reason, remaining = has_access(user.id, user.username)
    if reason != "subscribed":
        await update.message.reply_text(require_premium_text(), parse_mode="Markdown", reply_markup=build_subscribe_menu())
        return
    items = get_watchlist(user.id)
    if not items:
        await update.message.reply_text(
            f"Your watchlist is empty. Add up to {MAX_WATCHLIST_SIZE} pairs with /addwatch SYMBOL.\n\n"
            f"High-confidence alerts (score ≥{PREMIUM_ALERT_THRESHOLD}) get pushed to you automatically, "
            f"checked every ~{PREMIUM_SCAN_INTERVAL_SECONDS}s."
        )
        return
    lines = ["⭐ *Your Watchlist*\n"]
    for r in items:
        lines.append(f"• {r['symbol']}")
    lines.append(f"\nAlerts fire only when score ≥{PREMIUM_ALERT_THRESHOLD}, checked every ~{PREMIUM_SCAN_INTERVAL_SECONDS}s.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def tg_delwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not context.args:
        await update.message.reply_text("Example: /delwatch GBPJPY")
        return
    removed = remove_from_watchlist(user.id, context.args[0])
    await update.message.reply_text("✅ Removed." if removed else "Not found in your watchlist.")

async def tg_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    allowed, reason, remaining = has_access(user.id, user.username)
    if reason != "subscribed":
        await update.message.reply_text(require_premium_text(), parse_mode="Markdown", reply_markup=build_subscribe_menu())
        return
    await update.message.reply_text("⏳ Generating your report...")
    try:
        pdf_buf = await asyncio.to_thread(generate_daily_report_pdf)
        if pdf_buf is None:
            await update.message.reply_text("No signal data yet to build a report from.")
            return
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=pdf_buf,
            filename="signal_report.pdf",
            caption="📄 Your signal report — historical data only, not a forecast.",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error generating report: {e}")

async def tg_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Any photo a user sends (e.g. a payment screenshot) gets automatically
    forwarded to the bot owner along with the user's info and their order
    reference, so nothing gets lost waiting in the user's chat.
    """
    user = update.effective_user
    row = get_or_create_user(user.id, user.username)

    if not ADMIN_TELEGRAM_ID:
        await update.message.reply_text(
            "⚠️ The bot owner hasn't configured ADMIN_TELEGRAM_ID yet, so I can't "
            "forward this automatically. Please contact the bot owner directly."
        )
        return

    caption = (
        f"📸 Payment screenshot received\n\n"
        f"From: @{user.username or 'no_username'} (id: {user.id})\n"
        f"Order ref: {row['order_ref']}\n\n"
        f"If verified, run:\n/approve {user.id} 30"
    )
    try:
        photo = update.message.photo[-1]
        await context.bot.send_photo(chat_id=ADMIN_TELEGRAM_ID, photo=photo.file_id, caption=caption)
        await update.message.reply_text(
            "✅ Screenshot received and sent for approval. You'll be upgraded shortly "
            "once it's verified.",
            reply_markup=build_subscribe_menu(),
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Couldn't forward screenshot: {e}")

async def tg_fallback_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Catches any plain text message that isn't a recognized command (e.g. a
    user just says "hi" or taps into the chat without knowing commands) and
    shows the main button menu, so nobody gets stuck not knowing what to type.
    """
    await update.message.reply_text(
        MENU_INTRO, parse_mode="Markdown", reply_markup=build_main_menu()
    )

async def tg_corr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if len(context.args) < 2:
            await update.message.reply_text("Example: /corr AUDNZD EURUSD 5m")
            return
        if not await gate(update, context):
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

async def tg_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split(":")
    action = parts[0]

    try:
        # --- Main menu navigation ---
        if data == "m:main" or data == "m:help":
            await query.edit_message_text(MENU_INTRO, parse_mode="Markdown", reply_markup=build_main_menu())
            return

        # --- Get Signal flow: m:sig -> sig:SYM -> sig:SYM:TF ---
        if data == "m:sig":
            await query.edit_message_text("Choose a pair:", reply_markup=build_symbol_menu("sig"))
            return
        if action == "sig" and len(parts) == 2:
            sym = parts[1]
            await query.edit_message_text(
                f"{SYMBOLS.get(sym, sym)} — choose a timeframe:",
                reply_markup=build_timeframe_menu("sig", sym)
            )
            return
        if action == "sig" and len(parts) == 3:
            sym, tf = parts[1], parts[2]
            if not await gate(update, context):
                return
            await query.edit_message_text(f"⏳ Analyzing {SYMBOLS.get(sym, sym)} ({tf})...")
            df = await asyncio.to_thread(fetch_candles, sym, tf)
            s = make_signal(df)
            s["id"] = await asyncio.to_thread(save_signal, normalize_symbol(sym), normalize_interval(tf), s)
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=format_signal(sym, tf, s),
                reply_markup=build_back_menu(),
            )
            chart = await asyncio.to_thread(generate_chart_image, df, s, sym, tf)
            if chart:
                await context.bot.send_photo(chat_id=query.message.chat_id, photo=chart)
            return

        # --- Multi-timeframe flow: m:mtf -> mtf:SYM ---
        if data == "m:mtf":
            await query.edit_message_text("Choose a pair for 1m+5m+15m check:", reply_markup=build_symbol_menu("mtf"))
            return
        if action == "mtf" and len(parts) == 2:
            sym = parts[1]
            if not await gate(update, context):
                return
            await query.edit_message_text(f"⏳ Checking {SYMBOLS.get(sym, sym)} across timeframes...")
            mtf = await asyncio.to_thread(multi_timeframe_signal, sym)
            await context.bot.send_message(
                chat_id=query.message.chat_id, text=format_mtf(mtf), reply_markup=build_back_menu()
            )
            return

        # --- Scan flow: m:scan -> scan:TF ---
        if data == "m:scan":
            await query.edit_message_text("Choose a timeframe to scan all pairs:", reply_markup=build_scan_timeframe_menu())
            return
        if action == "scan" and len(parts) == 2:
            tf = parts[1]
            if not await gate(update, context):
                return
            await query.edit_message_text(f"⏳ Scanning all pairs on {tf}...")
            results = await asyncio.to_thread(scan_symbols, tf)
            lines = [f"🔍 MULTI-SYMBOL SCAN ({tf}) — sorted by signal strength\n"]
            for r in results[:11]:
                arrow = "📈" if "UP" in r["direction"] else "📉" if "DOWN" in r["direction"] else "⚪"
                lines.append(f"{arrow} {r['symbol']}: {r['direction']} (score {r['score']:+d})")
            lines.append("\n⚠️ Ranked by current indicator strength, not a prediction.")
            await context.bot.send_message(
                chat_id=query.message.chat_id, text="\n".join(lines), reply_markup=build_back_menu()
            )
            return

        # --- Correlation flow: m:corr1 -> corr1:SYM_A -> corr2:SYM_A:SYM_B ---
        if data == "m:corr1":
            await query.edit_message_text("Choose the FIRST pair:", reply_markup=build_symbol_menu("corr1"))
            return
        if action == "corr1" and len(parts) == 2:
            sym_a = parts[1]
            await query.edit_message_text(
                f"First pair: {SYMBOLS.get(sym_a, sym_a)}\nNow choose the SECOND pair:",
                reply_markup=build_symbol_menu(f"corr2:{sym_a}", exclude=sym_a)
            )
            return
        if action == "corr2" and len(parts) == 3:
            sym_a, sym_b = parts[1], parts[2]
            if not await gate(update, context):
                return
            await query.edit_message_text(f"⏳ Computing correlation {sym_a} vs {sym_b}...")
            corr_data = await asyncio.to_thread(compute_correlation, sym_a, sym_b, "5m")
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=(
                    f"🔗 CORRELATION: {corr_data['symbol_a']} vs {corr_data['symbol_b']} ({corr_data['interval']})\n\n"
                    f"Coefficient: {corr_data['correlation']}\n"
                    f"Interpretation: {corr_data['interpretation']}\n"
                    f"Sample size: {corr_data['sample_size']} candles\n\n"
                    "⚠️ Recent past only, not a forecast."
                ),
                reply_markup=build_back_menu(),
            )
            return

        # --- Simple one-tap actions ---
        if data == "m:hist":
            await query.edit_message_text("⏳ Loading history...")
            await tg_history_content(context.bot, query.message.chat_id)
            return
        if data == "m:acc":
            await query.edit_message_text("⏳ Loading accuracy...")
            acc_data = api_accuracy()
            accuracy = acc_data["accuracy_percent"]
            txt = (
                f"🎯 OVERALL ACCURACY\n\nResolved: {acc_data['resolved']}\nWins: {acc_data['wins']}\n"
                f"Hit-rate: {accuracy if accuracy is not None else 'N/A'}%\n\n{acc_data['note']}"
            ) if acc_data["resolved"] else "No resolved signals yet."
            await context.bot.send_message(chat_id=query.message.chat_id, text=txt, reply_markup=build_back_menu())
            return
        if data == "m:back":
            await query.edit_message_text("⏳ Loading backtest report...")
            back_data = api_backtest_report()
            report = back_data["report"]
            if not report:
                txt = "No resolved signals yet to report on."
            else:
                lines = ["📊 BACKTEST REPORT\n"]
                for row in report:
                    acc = row["accuracy_percent"]
                    lines.append(f"{row['symbol']} {row['interval']}: {row['wins']}W/{row['losses']}L/{row['flats']}F — {acc if acc is not None else 'N/A'}%")
                lines.append(f"\n{back_data['note']}")
                txt = "\n".join(lines)
            await context.bot.send_message(chat_id=query.message.chat_id, text=txt, reply_markup=build_back_menu())
            return
        if data == "m:wt":
            await query.edit_message_text("⏳ Loading adaptive weights...")
            weights = get_adaptive_weights()
            if not weights:
                txt = f"No adaptive weights yet — need at least {MIN_SAMPLES_FOR_ADAPTIVE} resolved signals per indicator."
            else:
                lines = ["⚖️ ADAPTIVE WEIGHTS\n"]
                for tag, w in sorted(weights.items(), key=lambda x: -x[1]):
                    lines.append(f"{tag}: {w}x")
                txt = "\n".join(lines)
            await context.bot.send_message(chat_id=query.message.chat_id, text=txt, reply_markup=build_back_menu())
            return
        if data == "m:news":
            await query.edit_message_text("⏳ Loading news...")
            news_data = fetch_economic_events()
            if not news_data["configured"]:
                txt = f"ℹ️ {news_data['message']}"
            elif not news_data["events"]:
                txt = "No medium/high-impact events found in the next 48 hours."
            else:
                lines = ["📰 UPCOMING ECONOMIC EVENTS\n"]
                for e in news_data["events"][:10]:
                    lines.append(f"{e.get('time','?')} | {e.get('country','?')} | {e.get('event','?')}")
                txt = "\n".join(lines)
            await context.bot.send_message(chat_id=query.message.chat_id, text=txt, reply_markup=build_back_menu())
            return

        if data == "m:subscribe":
            user = update.effective_user
            row = get_or_create_user(user.id, user.username)
            await query.edit_message_text(
                build_payment_card(row["order_ref"]),
                parse_mode="Markdown",
                reply_markup=build_subscribe_menu(),
            )
            if ADMIN_TELEGRAM_ID and str(user.id) != str(ADMIN_TELEGRAM_ID):
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_TELEGRAM_ID,
                        text=(
                            f"💰 New subscribe request\n"
                            f"User: @{user.username or user.id} (id: {user.id})\n"
                            f"Order ref: {row['order_ref']}\n\n"
                            f"Once payment is confirmed, run:\n/approve {user.id} 30"
                        ),
                    )
                except Exception:
                    pass
            return

        if data == "m:status":
            user = update.effective_user
            allowed, reason, remaining = has_access(user.id, user.username)
            row = get_or_create_user(user.id)
            txt = build_status_text(reason, remaining, row)
            await query.edit_message_text(txt, parse_mode="Markdown", reply_markup=build_subscribe_menu())
            return

    except Exception as e:
        await context.bot.send_message(chat_id=query.message.chat_id, text=f"❌ Error: {e}", reply_markup=build_back_menu())

async def tg_history_content(bot, chat_id):
    with DB_LOCK:
        con = db()
        rows = con.execute("""
            SELECT symbol, interval, direction, score, timestamp
            FROM signals ORDER BY id DESC LIMIT 10
        """).fetchall()
        con.close()
    if not rows:
        await bot.send_message(chat_id=chat_id, text="No signals yet.", reply_markup=build_back_menu())
        return
    lines = ["📊 LAST 10 SIGNALS\n"]
    for r in rows:
        lines.append(f"{r['symbol']} | {r['interval']} | {r['direction']} | {r['score']:+d}")
    await bot.send_message(chat_id=chat_id, text="\n".join(lines), reply_markup=build_back_menu())

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
    tg_app.add_handler(CommandHandler("subscribe", tg_subscribe))
    tg_app.add_handler(CommandHandler("mystatus", tg_mystatus))
    tg_app.add_handler(CommandHandler("approve", tg_approve))
    tg_app.add_handler(CommandHandler("addwatch", tg_addwatch))
    tg_app.add_handler(CommandHandler("mywatch", tg_mywatch))
    tg_app.add_handler(CommandHandler("delwatch", tg_delwatch))
    tg_app.add_handler(CommandHandler("report", tg_report))
    tg_app.add_handler(CallbackQueryHandler(tg_button_callback))
    tg_app.add_handler(MessageHandler(filters.PHOTO, tg_photo_handler))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, tg_fallback_message))

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
    start_premium_watchlist_thread()
    uvicorn.run(app, host=HOST, port=PORT)
