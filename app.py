
"""
NexCandle AI — Full Power Telegram Market Analysis Bot
Version 5.0

GitHub entry point: app.py

Core:
- Real market candles from Twelve Data
- 1m / 5m / 15m / 30m / 35m / 45m / 1H
- Custom 35m/45m/1H candles built from 5m data
- RSI, EMA, MACD, Bollinger Bands, ATR, ADX, Stochastic, ROC
- ML ensemble: Logistic Regression + Random Forest + HistGradientBoosting
- Multi-timeframe confirmation
- Market-regime filter
- Entry timing engine
- Best Setup scanner
- Real signal-resolution accuracy tracker
- Time-series walk-forward style backtest
- SQLite signal history
- News/economic-calendar filter when FINNHUB_API_KEY is configured
- Premium watchlist alerts
- FastAPI health/API endpoints
- Safe Telegram callback handling (fixes NoneType.reply_text)
- No automatic order placement

IMPORTANT:
This is probabilistic market analysis, not a guaranteed future-candle predictor.
Historical hit rate is calculated from resolved signals. ML probability is not
the same thing as accuracy. The bot deliberately returns WAIT when data quality
or confirmation is weak.
"""

import os
import asyncio
import sqlite3
import threading
import logging
import math
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
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False

try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application, CommandHandler, CallbackQueryHandler,
        ContextTypes, MessageHandler, filters
    )
    TELEGRAM_OK = True
except Exception:
    TELEGRAM_OK = False
    Update = InlineKeyboardButton = InlineKeyboardMarkup = None
    Application = CommandHandler = CallbackQueryHandler = ContextTypes = MessageHandler = filters = None

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("nexcandle")

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
DB_FILE = os.getenv("DB_FILE", "signals.db")

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
RESOLUTION_INTERVAL_SECONDS = int(os.getenv("RESOLUTION_INTERVAL_SECONDS", "30"))
PREMIUM_SCAN_INTERVAL_SECONDS = int(os.getenv("PREMIUM_SCAN_INTERVAL_SECONDS", "60"))
PREMIUM_ALERT_THRESHOLD = float(os.getenv("PREMIUM_ALERT_THRESHOLD", "78"))
PREMIUM_ALERT_COOLDOWN_MINUTES = int(os.getenv("PREMIUM_ALERT_COOLDOWN_MINUTES", "10"))
FREE_TRIAL_LIMIT = int(os.getenv("FREE_TRIAL_LIMIT", "3"))
PRICE_INR = int(os.getenv("PRICE_INR", "299"))
PRICE_AED = int(os.getenv("PRICE_AED", "29"))
UPI_NUMBER = os.getenv("UPI_NUMBER", "")
BOTIM_NUMBER = os.getenv("BOTIM_NUMBER", "")

SYMBOLS = {
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
    "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD", "USDCHF": "USD/CHF",
    "NZDUSD": "NZD/USD", "AUDNZD": "AUD/NZD", "GBPJPY": "GBP/JPY",
    "EURJPY": "EUR/JPY", "EURGBP": "EUR/GBP", "XAUUSD": "XAU/USD",
    "XAGUSD": "XAG/USD", "BTCUSD": "BTC/USD", "ETHUSD": "ETH/USD",
}
INTERVALS = {
    "1m": "1min", "1min": "1min",
    "5m": "5min", "5min": "5min",
    "15m": "15min", "15min": "15min",
    "30m": "30min", "30min": "30min",
    "35m": "35min", "35min": "35min",
    "45m": "45min", "45min": "45min",
    "1h": "1hour", "60m": "1hour", "1hour": "1hour",
}
TF_MINUTES = {"1min": 1, "5min": 5, "15min": 15, "30min": 30, "35min": 35,
              "45min": 45, "1hour": 60}
SUPPORTED_TF = ["1min", "5min", "15min", "30min", "35min", "45min", "1hour"]

WATCHLIST = [x.strip() for x in os.getenv("WATCHLIST", "").split(",") if x.strip()]

app = FastAPI(title="NexCandle AI", version="5.0.0")
DB_LOCK = threading.RLock()
TG_REF = {"app": None}
MODEL_CACHE = {}
MODEL_LOCK = threading.RLock()


# ------------------------- Helpers -------------------------

def utc_now():
    return datetime.now(timezone.utc)

def iso_now():
    return utc_now().isoformat()

def normalize_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper().replace(" ", "")
    if s in SYMBOLS:
        return SYMBOLS[s]
    if "/" in s:
        return s
    if len(s) == 6:
        return s[:3] + "/" + s[3:]
    return s

def normalize_interval(interval: str) -> str:
    i = str(interval or "").strip().lower()
    if i not in INTERVALS:
        raise ValueError("Supported: 1m, 5m, 15m, 30m, 35m, 45m, 1h")
    return INTERVALS[i]

def tf_label(interval: str) -> str:
    m = TF_MINUTES[normalize_interval(interval)]
    return "1H" if m == 60 else f"{m}M"

def floor_time(dt, minutes):
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    epoch = int(dt.timestamp())
    floored = epoch - (epoch % (minutes * 60))
    return datetime.fromtimestamp(floored, tz=timezone.utc)

def timeframe_timing(interval: str, now=None):
    minutes = TF_MINUTES[normalize_interval(interval)]
    now = now or utc_now()
    current_start = floor_time(now, minutes)
    next_start = current_start + timedelta(minutes=minutes)
    wait = max(0.0, (next_start - now).total_seconds() / 60)
    return {
        "current_candle_start": current_start,
        "next_candle_start": next_start,
        "wait_minutes": round(wait, 1),
        "duration_minutes": minutes,
    }


# ------------------------- Database -------------------------

def db():
    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with DB_LOCK:
        con = db()
        con.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id TEXT PRIMARY KEY,
            username TEXT,
            trial_used INTEGER DEFAULT 0,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            telegram_id TEXT PRIMARY KEY,
            expires_at TEXT,
            is_active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS watchlist_items (
            telegram_id TEXT,
            symbol TEXT,
            last_alert_at TEXT,
            PRIMARY KEY (telegram_id, symbol)
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id TEXT,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            created_at TEXT NOT NULL,
            entry_time TEXT,
            expiry_time TEXT,
            entry_price REAL,
            direction TEXT NOT NULL,
            quality_score REAL,
            ml_probability REAL,
            technical_score REAL,
            mtf_score REAL,
            regime TEXT,
            result TEXT DEFAULT 'PENDING',
            exit_price REAL,
            resolved_at TEXT,
            data_timestamp TEXT,
            reasons TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_signals_result ON signals(result);
        CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at);
        CREATE INDEX IF NOT EXISTS idx_signals_tf ON signals(interval);
        """)
        con.commit()
        con.close()

init_db()


# ------------------------- Market Data -------------------------

def _api_interval(interval):
    return normalize_interval(interval)

def fetch_raw(symbol: str, api_interval: str, outputsize=500):
    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY is missing in .env")
    params = {
        "symbol": normalize_symbol(symbol),
        "interval": api_interval,
        "outputsize": min(max(int(outputsize), 100), 5000),
        "apikey": TWELVE_DATA_API_KEY,
        "format": "JSON",
    }
    r = requests.get("https://api.twelvedata.com/time_series", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if "values" not in data:
        raise RuntimeError(data.get("message", "No market candles returned"))
    return data["values"]

def raw_to_df(values):
    rows = []
    for x in values:
        try:
            rows.append({
                "datetime": pd.to_datetime(x["datetime"], utc=True),
                "open": float(x["open"]),
                "high": float(x["high"]),
                "low": float(x["low"]),
                "close": float(x["close"]),
                "volume": float(x.get("volume", 0) or 0),
            })
        except Exception:
            continue
    if not rows:
        raise RuntimeError("Market data contained no valid OHLC candles")
    df = pd.DataFrame(rows).sort_values("datetime").drop_duplicates("datetime")
    df = df.set_index("datetime").reset_index()
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["datetime", "open", "high", "low", "close"])
    if len(df) < 80:
        raise RuntimeError(f"Only {len(df)} valid candles available; need at least 80")
    return df.reset_index(drop=True)

def aggregate_from_5m(df5, minutes):
    if minutes % 5 != 0:
        raise ValueError("Custom timeframe must be a multiple of 5 minutes")
    d = df5.copy()
    d["bucket"] = d["datetime"].apply(lambda x: floor_time(x, minutes))
    g = d.groupby("bucket", sort=True)
    out = g.agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        count=("close", "count")
    ).reset_index().rename(columns={"bucket": "datetime"})
    expected = minutes // 5
    out = out[out["count"] >= expected].drop(columns=["count"])
    return out.reset_index(drop=True)

def fetch_candles(symbol: str, interval: str, outputsize=500):
    tf = normalize_interval(interval)
    # 35m/45m/1H are constructed from 5m candles for reliable support.
    if tf in ("35min", "45min", "1hour"):
        base = raw_to_df(fetch_raw(symbol, "5min", max(outputsize, 500)))
        return aggregate_from_5m(base, TF_MINUTES[tf])
    return raw_to_df(fetch_raw(symbol, _api_interval(tf), outputsize))

def validate_candles(df):
    if df is None or df.empty:
        raise RuntimeError("Empty market data")
    if df["datetime"].duplicated().any():
        raise RuntimeError("Duplicate candle timestamps")
    if not np.isfinite(df[["open","high","low","close"]].to_numpy()).all():
        raise RuntimeError("Invalid numeric market data")
    if (df[["open","high","low","close"]] <= 0).any().any():
        raise RuntimeError("Invalid non-positive price")
    return True


# ------------------------- Indicators -------------------------

def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def rsi(s, n=14):
    delta = s.diff()
    up = delta.clip(lower=0)
    dn = -delta.clip(upper=0)
    ag = up.ewm(alpha=1/n, adjust=False).mean()
    al = dn.ewm(alpha=1/n, adjust=False).mean()
    rs = ag / al.replace(0, np.nan)
    return (100 - 100/(1+rs)).fillna(50)

def macd(s):
    m = ema(s, 12) - ema(s, 26)
    sig = ema(m, 9)
    return m, sig

def bollinger(s, n=20, k=2):
    mid = s.rolling(n).mean()
    sd = s.rolling(n).std()
    return mid, mid+k*sd, mid-k*sd

def atr(df, n=14):
    pc = df["close"].shift(1)
    tr = pd.concat([
        df["high"]-df["low"],
        (df["high"]-pc).abs(),
        (df["low"]-pc).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def adx(df, n=14):
    up = df["high"].diff()
    dn = -df["low"].diff()
    plus = up.where((up > dn) & (up > 0), 0.0)
    minus = dn.where((dn > up) & (dn > 0), 0.0)
    a = atr(df, n).replace(0, np.nan)
    pdi = 100 * plus.rolling(n).mean() / a
    mdi = 100 * minus.rolling(n).mean() / a
    dx = 100 * (pdi-mdi).abs() / (pdi+mdi).replace(0, np.nan)
    return dx.rolling(n).mean().fillna(0)

def stochastic(df, n=14, d=3):
    lo = df["low"].rolling(n).min()
    hi = df["high"].rolling(n).max()
    k = 100*(df["close"]-lo)/(hi-lo).replace(0, np.nan)
    return k.fillna(50), k.rolling(d).mean().fillna(50)

def roc(s, n=10):
    return (s / s.shift(n) - 1) * 100

def feature_frame(df):
    c = df["close"]
    e9, e21, e50, e200 = ema(c,9), ema(c,21), ema(c,50), ema(c,200)
    m, ms = macd(c)
    bm, bu, bl = bollinger(c)
    av, ax = atr(df), adx(df)
    sk, sd = stochastic(df)
    rr = roc(c)
    rng = (df["high"]-df["low"]).replace(0, np.nan)
    body = (c-df["open"]) / rng
    upper = (df["high"]-df[["open","close"]].max(axis=1)) / rng
    lower = (df[["open","close"]].min(axis=1)-df["low"]) / rng
    ret1 = c.pct_change()*100
    ret5 = c.pct_change(5)*100
    vol = ret1.rolling(20).std()
    out = pd.DataFrame({
        "rsi": rsi(c),
        "ema_gap": (e9-e21)/c*100,
        "ema50_gap": (c-e50)/c*100,
        "ema200_gap": (c-e200)/c*100,
        "macd_gap": m-ms,
        "bb_pos": (c-bm)/(bu-bl).replace(0,np.nan),
        "atr_pct": av/c*100,
        "adx": ax,
        "stoch_k": sk,
        "stoch_d": sd,
        "roc": rr,
        "body": body,
        "upper_wick": upper,
        "lower_wick": lower,
        "ret1": ret1,
        "ret5": ret5,
        "volatility": vol,
    })
    return out.replace([np.inf,-np.inf],np.nan)


# ------------------------- ML -------------------------

FEATURES = [
    "rsi","ema_gap","ema50_gap","ema200_gap","macd_gap","bb_pos",
    "atr_pct","adx","stoch_k","stoch_d","roc","body","upper_wick",
    "lower_wick","ret1","ret5","volatility"
]

def ml_predict(df):
    if not SKLEARN_OK or len(df) < 180:
        return {"prob_up": None, "models": 0, "status": "ML unavailable/insufficient data"}

    Xall = feature_frame(df)
    # Target is the NEXT candle direction.
    y = (df["close"].shift(-1) > df["close"]).astype(int)
    work = Xall.copy()
    work["target"] = y
    work = work.dropna()
    if len(work) < 140 or work["target"].nunique() < 2:
        return {"prob_up": None, "models": 0, "status": "Insufficient training diversity"}

    X = work[FEATURES].to_numpy()
    Y = work["target"].to_numpy()
    # Time ordered split; no random shuffle.
    split = max(100, int(len(X)*0.80))
    Xtr, Xte, ytr, yte = X[:split], X[split:], Y[:split], Y[split:]
    if len(Xte) < 10 or len(np.unique(ytr)) < 2:
        return {"prob_up": None, "models": 0, "status": "Insufficient time-series training data"}

    models = [
        make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
        RandomForestClassifier(n_estimators=160, max_depth=7, random_state=42, min_samples_leaf=3),
        HistGradientBoostingClassifier(max_iter=120, learning_rate=0.05, max_leaf_nodes=15, random_state=42),
    ]
    probs = []
    valid = 0
    for model in models:
        try:
            model.fit(Xtr, ytr)
            p = float(model.predict_proba(Xte[-1:].reshape(1,-1))[0,1])
            probs.append(p)
            valid += 1
        except Exception as e:
            log.warning("ML model failed: %s", e)
    if not probs:
        return {"prob_up": None, "models": 0, "status": "ML training failed"}
    return {"prob_up": round(float(np.mean(probs)),4), "models": valid, "status": "OK"}


# ------------------------- Signal Engine -------------------------

def regime_for(df):
    c = df["close"]
    e21, e50 = ema(c,21).iloc[-1], ema(c,50).iloc[-1]
    ax = adx(df).iloc[-1]
    av = atr(df).iloc[-1]
    vol_pct = (av / c.iloc[-1]) * 100 if c.iloc[-1] else 0
    if not np.isfinite(ax): ax = 0
    if ax >= 25 and e21 > e50: return "TRENDING UP"
    if ax >= 25 and e21 < e50: return "TRENDING DOWN"
    if vol_pct > 1.5: return "HIGH VOLATILITY"
    if ax < 16: return "RANGING"
    return "UNCERTAIN"

def technical_analysis(df):
    c = df["close"]
    e9, e21, e50, e200 = ema(c,9), ema(c,21), ema(c,50), ema(c,200)
    rv = rsi(c)
    m, ms = macd(c)
    bm, bu, bl = bollinger(c)
    ax, av = adx(df).iloc[-1], atr(df).iloc[-1]
    sk, sd = stochastic(df)
    rr = roc(c).iloc[-1]
    price = float(c.iloc[-1])
    score = 0.0
    reasons = []
    parts = []

    def add(name, val, weight=1):
        nonlocal score
        score += val*weight
        parts.append((name, 1 if val > 0 else -1 if val < 0 else 0))

    add("EMA9/21", 1 if e9.iloc[-1] > e21.iloc[-1] else -1, 1.4)
    add("EMA50", 1 if price > e50.iloc[-1] else -1, 1.0)
    add("EMA200", 1 if price > e200.iloc[-1] else -1, 1.0)
    add("MACD", 1 if m.iloc[-1] > ms.iloc[-1] else -1, 1.2)

    r = rv.iloc[-1]
    if 52 <= r <= 68:
        add("RSI", 1, 0.8); reasons.append("RSI supports bullish momentum")
    elif 32 <= r < 48:
        add("RSI", -1, 0.8); reasons.append("RSI supports bearish momentum")
    elif r > 72:
        add("RSI", -1, 0.5); reasons.append("RSI is overbought")
    elif r < 28:
        add("RSI", 1, 0.5); reasons.append("RSI is oversold")
    else:
        parts.append(("RSI",0))

    mid, upper, lower = bm.iloc[-1], bu.iloc[-1], bl.iloc[-1]
    if price > mid: add("Bollinger", 1, 0.7)
    else: add("Bollinger", -1, 0.7)

    add("Stochastic", 1 if sk.iloc[-1] > sd.iloc[-1] else -1, 0.6)
    add("ROC", 1 if rr > 0 else -1, 0.6)
    add("ADX trend", 1 if ax >= 20 and e9.iloc[-1] > e21.iloc[-1] else -1 if ax >= 20 else 0, 0.7)

    raw = max(-100, min(100, score / 8.0 * 100))
    direction = "UP" if raw >= 18 else "DOWN" if raw <= -18 else "WAIT"
    confidence = round(min(99, 50 + abs(raw)*0.45), 1)

    return {
        "technical_score": round(raw,2),
        "direction": direction,
        "confidence": confidence,
        "rsi": round(float(r),2),
        "adx": round(float(ax),2),
        "atr": round(float(av),8),
        "price": round(price,8),
        "reasons": reasons,
        "parts": parts,
        "regime": regime_for(df),
    }

def mtf_analysis(symbol):
    results = {}
    dirs = []
    for tf in ["5min","15min","30min","45min","1hour"]:
        try:
            d = fetch_candles(symbol, tf, 350)
            validate_candles(d)
            a = technical_analysis(d)
            results[tf] = a
            if a["direction"] in ("UP","DOWN"):
                dirs.append(a["direction"])
        except Exception as e:
            results[tf] = {"direction":"NO DATA", "error":str(e)}
    up = dirs.count("UP"); down = dirs.count("DOWN")
    if up >= 4 and down == 0: consensus = "UP"
    elif down >= 4 and up == 0: consensus = "DOWN"
    elif up >= 3 and down == 0: consensus = "UP"
    elif down >= 3 and up == 0: consensus = "DOWN"
    else: consensus = "WAIT"
    agreement = max(up,down)
    return {"consensus":consensus, "agreement":agreement, "available":len(dirs), "timeframes":results}

def build_signal(symbol, interval):
    tf = normalize_interval(interval)
    df = fetch_candles(symbol, tf, 500)
    validate_candles(df)
    if len(df) < 100:
        raise RuntimeError("Not enough completed candles")

    ta = technical_analysis(df)
    ml = ml_predict(df)
    mtf = mtf_analysis(symbol)

    direction = ta["direction"]
    if mtf["consensus"] in ("UP","DOWN") and direction in ("UP","DOWN"):
        if mtf["consensus"] != direction:
            direction = "WAIT"
    elif mtf["consensus"] == "WAIT":
        # Keep only strong single-TF setups; otherwise wait.
        if abs(ta["technical_score"]) < 45:
            direction = "WAIT"

    ml_prob = ml["prob_up"]
    ml_dir = None
    if ml_prob is not None:
        ml_dir = "UP" if ml_prob >= 0.55 else "DOWN" if ml_prob <= 0.45 else "WAIT"
        if direction in ("UP","DOWN") and ml_dir in ("UP","DOWN") and direction != ml_dir:
            direction = "WAIT"

    timing = timeframe_timing(tf)
    quality = abs(ta["technical_score"]) * 0.45 + min(100, abs(ml_prob-0.5)*200 if ml_prob is not None else 0) * 0.25
    quality += (mtf["agreement"]/max(1,mtf["available"])) * 100 * 0.30
    quality = round(max(0,min(99,quality)),1)

    # Strong signal threshold; never force a trade.
    if direction in ("UP","DOWN") and quality < 55:
        direction = "WAIT"

    next_entry = timing["next_candle_start"].strftime("%H:%M UTC")
    wait = timing["wait_minutes"]
    if direction == "WAIT":
        action = "WAIT — no high-quality setup"
    else:
        action = f"WAIT {wait} min → ENTER AT NEXT {tf_label(tf)} CANDLE OPEN"

    return {
        "symbol": normalize_symbol(symbol),
        "interval": tf,
        "timeframe": tf_label(tf),
        "direction": direction,
        "quality_score": quality,
        "technical_score": ta["technical_score"],
        "confidence": ta["confidence"],
        "ml_probability": round(ml_prob*100,1) if ml_prob is not None else None,
        "ml_models": ml["models"],
        "ml_status": ml["status"],
        "mtf_consensus": mtf["consensus"],
        "mtf_agreement": mtf["agreement"],
        "mtf_available": mtf["available"],
        "regime": ta["regime"],
        "price": ta["price"],
        "rsi": ta["rsi"],
        "adx": ta["adx"],
        "atr": ta["atr"],
        "entry_time": next_entry,
        "wait_minutes": wait,
        "duration_minutes": timing["duration_minutes"],
        "next_candle_start": timing["next_candle_start"].isoformat(),
        "current_candle_start": timing["current_candle_start"].isoformat(),
        "reasons": ta["reasons"],
        "data_timestamp": df["datetime"].iloc[-1].isoformat(),
        "action": action,
    }


# ------------------------- Persistence / Accuracy -------------------------

def save_signal(signal, telegram_id=""):
    if signal["direction"] == "WAIT":
        return None
    created = utc_now()
    expiry = created + timedelta(minutes=signal["duration_minutes"])
    with DB_LOCK:
        con = db()
        cur = con.execute("""
            INSERT INTO signals(
                telegram_id,symbol,interval,created_at,entry_time,expiry_time,
                entry_price,direction,quality_score,ml_probability,
                technical_score,mtf_score,regime,data_timestamp,reasons
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            str(telegram_id), signal["symbol"], signal["interval"], created.isoformat(),
            signal["next_candle_start"], expiry.isoformat(), signal["price"],
            signal["direction"], signal["quality_score"], signal["ml_probability"],
            signal["technical_score"], signal["mtf_agreement"], signal["regime"],
            signal["data_timestamp"], " | ".join(signal["reasons"][:8])
        ))
        con.commit()
        sid = cur.lastrowid
        con.close()
    return sid

def resolve_pending_signals():
    resolved = 0
    now = utc_now()
    with DB_LOCK:
        con = db()
        rows = con.execute("""
            SELECT * FROM signals
            WHERE result='PENDING' AND expiry_time <= ?
            ORDER BY id ASC LIMIT 100
        """, (now.isoformat(),)).fetchall()
        con.close()
    for row in rows:
        try:
            df = fetch_candles(row["symbol"], row["interval"], 150)
            price = float(df["close"].iloc[-1])
            entry = float(row["entry_price"])
            direction = row["direction"]
            if abs(price-entry) < max(entry*1e-7, 1e-10):
                result = "FLAT"
            elif direction == "UP":
                result = "WIN" if price > entry else "LOSS"
            else:
                result = "WIN" if price < entry else "LOSS"
            with DB_LOCK:
                con = db()
                con.execute("""
                    UPDATE signals SET result=?, exit_price=?, resolved_at=?
                    WHERE id=?
                """, (result, price, now.isoformat(), row["id"]))
                con.commit(); con.close()
            resolved += 1
        except Exception as e:
            log.warning("Resolve failed id=%s: %s", row["id"], e)
    return resolved

def accuracy_report():
    with DB_LOCK:
        con = db()
        total = con.execute("SELECT COUNT(*) FROM signals WHERE result IN ('WIN','LOSS')").fetchone()[0]
        wins = con.execute("SELECT COUNT(*) FROM signals WHERE result='WIN'").fetchone()[0]
        pending = con.execute("SELECT COUNT(*) FROM signals WHERE result='PENDING'").fetchone()[0]
        rows = con.execute("""
            SELECT interval,
                   COUNT(*) resolved,
                   SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) wins,
                   SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) losses
            FROM signals WHERE result IN ('WIN','LOSS')
            GROUP BY interval ORDER BY interval
        """).fetchall()
        con.close()
    return {
        "resolved": total, "wins": wins,
        "losses": total-wins, "pending": pending,
        "accuracy_percent": round(wins/total*100,2) if total else None,
        "by_timeframe": [
            {
                "interval": r["interval"], "resolved": r["resolved"],
                "wins": r["wins"], "losses": r["losses"],
                "accuracy_percent": round(r["wins"]/r["resolved"]*100,2)
                if r["resolved"] else None
            } for r in rows
        ],
        "note": "Historical resolved signals only. Not a guarantee of future performance."
    }

def backtest_report(symbol=None, interval=None):
    # Walk-forward style evaluation of the technical engine using real historical
    # candles. The test is chronological: each prediction uses only candles before it.
    syms = [normalize_symbol(symbol)] if symbol else list(SYMBOLS.values())[:8]
    tfs = [normalize_interval(interval)] if interval else ["5min","15min","30min","35min","45min","1hour"]
    reports = []
    for sym in syms:
        for tf in tfs:
            try:
                df = fetch_candles(sym, tf, 500)
                if len(df) < 160: continue
                wins = losses = skipped = 0
                # Use technical-only walk-forward to avoid training on the future.
                start = max(120, len(df)-180)
                for i in range(start, len(df)-1):
                    hist = df.iloc[:i].copy()
                    a = technical_analysis(hist)
                    d = a["direction"]
                    if d == "WAIT":
                        skipped += 1
                        continue
                    actual_up = df["close"].iloc[i] > df["close"].iloc[i-1]
                    good = (d=="UP" and actual_up) or (d=="DOWN" and not actual_up)
                    wins += int(good); losses += int(not good)
                resolved = wins+losses
                reports.append({
                    "symbol": sym, "interval": tf, "tested": resolved,
                    "wins": wins, "losses": losses,
                    "accuracy_percent": round(wins/resolved*100,2) if resolved else None,
                    "skipped_wait": skipped,
                })
            except Exception as e:
                reports.append({"symbol":sym,"interval":tf,"error":str(e)})
    return reports


# ------------------------- News -------------------------

def fetch_news():
    if not FINNHUB_API_KEY:
        return {"configured":False,"events":[],"message":"FINNHUB_API_KEY not configured."}
    today = utc_now().date()
    end = (utc_now()+timedelta(hours=48)).date()
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/calendar/economic",
            params={"from":str(today),"to":str(end),"token":FINNHUB_API_KEY},
            timeout=15
        )
        r.raise_for_status()
        events = r.json().get("economicCalendar",[])
        events = [x for x in events if x.get("impact") in ("high","medium")]
        return {"configured":True,"events":events[:20]}
    except Exception as e:
        return {"configured":False,"events":[],"message":str(e)}


# ------------------------- Telegram UI -------------------------

def main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Get Signal", callback_data="sig"),
         InlineKeyboardButton("🧭 Multi-Timeframe", callback_data="mtf")],
        [InlineKeyboardButton("🔥 Best Setup", callback_data="best"),
         InlineKeyboardButton("🔍 Scan All Pairs", callback_data="scan")],
        [InlineKeyboardButton("⏰ Entry Timing", callback_data="timing"),
         InlineKeyboardButton("🎯 Accuracy", callback_data="accuracy")],
        [InlineKeyboardButton("📈 Backtest", callback_data="backtest"),
         InlineKeyboardButton("📜 History", callback_data="history")],
        [InlineKeyboardButton("🔗 Correlation", callback_data="corr"),
         InlineKeyboardButton("📰 News", callback_data="news")],
        [InlineKeyboardButton("💎 Premium / Status", callback_data="premium"),
         InlineKeyboardButton("ℹ️ Help", callback_data="help")],
    ])

def symbol_menu(prefix="sig"):
    keys = list(SYMBOLS.keys())
    rows=[]
    for i in range(0,len(keys),2):
        row=[]
        for k in keys[i:i+2]:
            row.append(InlineKeyboardButton(SYMBOLS[k], callback_data=f"{prefix}:{k}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("« Back", callback_data="home")])
    return InlineKeyboardMarkup(rows)

def tf_menu(sym):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1M",callback_data=f"go:{sym}:1min"),
         InlineKeyboardButton("5M",callback_data=f"go:{sym}:5min"),
         InlineKeyboardButton("15M",callback_data=f"go:{sym}:15min")],
        [InlineKeyboardButton("30M",callback_data=f"go:{sym}:30min"),
         InlineKeyboardButton("35M",callback_data=f"go:{sym}:35min"),
         InlineKeyboardButton("45M",callback_data=f"go:{sym}:45min")],
        [InlineKeyboardButton("1H",callback_data=f"go:{sym}:1hour")],
        [InlineKeyboardButton("« Back",callback_data="sig")],
    ])

def format_signal(s, signal_id=None):
    ml = f"{s['ml_probability']}%" if s["ml_probability"] is not None else "N/A"
    direction_icon = "🟢 UP" if s["direction"]=="UP" else "🔴 DOWN" if s["direction"]=="DOWN" else "⚪ WAIT"
    hist = accuracy_report()
    hist_text = f"{hist['accuracy_percent']}%" if hist["accuracy_percent"] is not None else "No resolved data"
    idline = f"\nSignal ID: #{signal_id}" if signal_id else ""
    return (
        "🔥 *NEXCANDLE AI — SIGNAL*\n\n"
        f"💱 *{s['symbol']}*\n"
        f"⏱ Timeframe: *{s['timeframe']}*\n\n"
        f"📊 Direction: *{direction_icon}*\n"
        f"🎯 Quality: *{s['quality_score']}/100*\n"
        f"🧠 ML probability (UP): *{ml}*\n"
        f"📐 Technical score: *{s['technical_score']}*\n"
        f"🧭 MTF: *{s['mtf_agreement']}/{s['mtf_available']} agreement*\n"
        f"🌐 Regime: *{s['regime']}*\n\n"
        "⏰ *ENTRY TIMING*\n"
        f"Action: *{s['action']}*\n"
        f"Next candle: *{s['entry_time']}*\n"
        f"Duration: *{s['duration_minutes']} minutes*\n\n"
        f"💰 Price: `{s['price']}`\n"
        f"RSI: {s['rsi']} | ADX: {s['adx']}\n"
        f"🧪 Bot historical resolved hit-rate: *{hist_text}*\n"
        f"📡 Data candle: `{s['data_timestamp']}`{idline}\n\n"
        f"Reason: {', '.join(s['reasons'][:5]) or 'No single dominant reason'}\n\n"
        "⚠️ Historical hit-rate and ML probability are different metrics. "
        "No system can guarantee the next candle."
    )

def format_best(items):
    if not items:
        return "🚫 *NO HIGH-QUALITY SETUP*\n\nCurrent market conditions do not meet the bot's quality filters. WAIT."
    s=items[0]
    return (
        "🔥 *BEST SETUP*\n\n"
        f"💱 {s['symbol']} — {s['timeframe']}\n"
        f"Direction: {'🟢 UP' if s['direction']=='UP' else '🔴 DOWN'}\n"
        f"Quality: *{s['quality_score']}/100*\n"
        f"ML: {s['ml_probability']}%\n"
        f"MTF: {s['mtf_agreement']}/{s['mtf_available']}\n"
        f"⏰ {s['action']}\n"
        f"Duration: {s['duration_minutes']} min\n\n"
        f"Regime: {s['regime']}\n"
        "Only the strongest setup is shown; weak setups are filtered out."
    )

async def safe_reply(update, text, reply_markup=None, parse_mode=None):
    """Universal reply helper. Fixes callback update.message == None."""
    try:
        if getattr(update, "effective_message", None):
            return await update.effective_message.reply_text(
                text, reply_markup=reply_markup, parse_mode=parse_mode
            )
    except Exception:
        pass
    return None

async def safe_edit(query, text, reply_markup=None, parse_mode=None):
    try:
        return await query.edit_message_text(
            text, reply_markup=reply_markup, parse_mode=parse_mode
        )
    except Exception as e:
        if "not modified" in str(e).lower():
            return None
        log.exception("Telegram edit failed")
        return await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)

async def gate(update, context):
    # Premium users are always allowed. Free users get a small trial.
    user = update.effective_user
    uid = str(user.id)
    with DB_LOCK:
        con=db()
        row=con.execute("SELECT * FROM users WHERE telegram_id=?",(uid,)).fetchone()
        if not row:
            con.execute("INSERT INTO users(telegram_id,username,created_at) VALUES(?,?,?)",
                        (uid,user.username,iso_now()))
            con.commit()
            trial_used=0
        else:
            trial_used=row["trial_used"]
        sub=con.execute("SELECT * FROM subscriptions WHERE telegram_id=? AND is_active=1",(uid,)).fetchone()
        con.close()
    if sub and sub["expires_at"]:
        try:
            if utc_now() < datetime.fromisoformat(sub["expires_at"]):
                return True
        except Exception:
            pass
    if trial_used >= FREE_TRIAL_LIMIT:
        await safe_reply(update,
            f"🔒 Free trial finished ({FREE_TRIAL_LIMIT} signals).\n\n"
            "Use Premium / Status to activate access.")
        return False
    with DB_LOCK:
        con=db(); con.execute("UPDATE users SET trial_used=trial_used+1 WHERE telegram_id=?",(uid,)); con.commit(); con.close()
    return True

async def start_cmd(update, context):
    await safe_reply(update,
        "⚡ *NexCandle AI V5*\n\n"
        "Real market-data analysis with multi-timeframe filters, ML ensemble, "
        "entry timing, Best Setup, accuracy tracking and backtesting.\n\n"
        "Tap a button below.",
        main_menu(), "Markdown")

async def help_cmd(update, context):
    await safe_reply(update,
        "ℹ️ *NexCandle AI Help*\n\n"
        "Get Signal = full signal + entry timing.\n"
        "Best Setup = scans supported pairs/timeframes and filters weak setups.\n"
        "Accuracy = only resolved historical bot signals.\n"
        "Backtest = chronological historical test.\n"
        "35M/45M/1H = built from real 5M candles.\n\n"
        "The bot does not place trades and cannot guarantee future candles.",
        main_menu(), "Markdown")

async def signal_cmd(update, context):
    if not context.args:
        await safe_reply(update,"Example: /signal EURUSD 45m",main_menu()); return
    if not await gate(update,context): return
    try:
        sym=normalize_symbol(context.args[0]); tf=normalize_interval(context.args[1] if len(context.args)>1 else "5m")
        await safe_reply(update,f"⏳ Analyzing {sym} ({tf_label(tf)})...")
        s=await asyncio.to_thread(build_signal,sym,tf)
        sid=await asyncio.to_thread(save_signal,s,str(update.effective_user.id))
        await safe_reply(update,format_signal(s,sid),main_menu(),"Markdown")
    except Exception as e:
        log.exception("signal command")
        await safe_reply(update,f"❌ Analysis unavailable.\n\n{str(e)[:300]}",main_menu())

async def accuracy_cmd(update,context):
    try:
        d=await asyncio.to_thread(resolve_pending_signals)
        r=accuracy_report()
        lines=[
            "🎯 *ACCURACY DASHBOARD*",
            "",
            f"Resolved: *{r['resolved']}*",
            f"WIN: *{r['wins']}*",
            f"LOSS: *{r['losses']}*",
            f"Pending: *{r['pending']}*",
            f"Overall historical hit-rate: *{r['accuracy_percent']}%*" if r["accuracy_percent"] is not None else "Overall historical hit-rate: *No resolved signals*",
            "",
        ]
        for x in r["by_timeframe"]:
            lines.append(f"{x['interval']}: {x['wins']}W / {x['losses']}L — {x['accuracy_percent']}%")
        lines.append("\n⚠️ Historical performance only; not a future guarantee.")
        await safe_reply(update,"\n".join(lines),main_menu(),"Markdown")
    except Exception as e:
        await safe_reply(update,f"❌ Accuracy error: {str(e)[:300]}",main_menu())

async def backtest_cmd(update,context):
    try:
        await safe_reply(update,"⏳ Running chronological walk-forward test...")
        sym=normalize_symbol(context.args[0]) if context.args else None
        tf=normalize_interval(context.args[1]) if len(context.args)>1 else None
        rows=await asyncio.to_thread(backtest_report,sym,tf)
        lines=["📈 *WALK-FORWARD BACKTEST*",""]
        for x in rows[:30]:
            if "error" in x: lines.append(f"⚠️ {x['symbol']} {x['interval']}: {x['error'][:80]}")
            else: lines.append(f"{x['symbol']} {x['interval']}: {x['wins']}W/{x['losses']}L — {x['accuracy_percent']}% (tested {x['tested']})")
        lines.append("\n⚠️ Historical test, not a prediction.")
        await safe_reply(update,"\n".join(lines),main_menu(),"Markdown")
    except Exception as e:
        await safe_reply(update,f"❌ Backtest error: {str(e)[:300]}",main_menu())

async def button(update,context):
    query=update.callback_query
    try: await query.answer()
    except Exception: pass
    data=query.data or ""
    try:
        if data=="home":
            await safe_edit(query,"⚡ *NexCandle AI V5*\n\nChoose an analysis tool.",main_menu(),"Markdown"); return
        if data=="help":
            await safe_edit(query,"ℹ️ *Help*\n\nGet Signal includes direction, quality, ML, MTF, entry timing and duration.\n\nAccuracy is based only on resolved stored signals.\n\n⚠️ No guarantee.",main_menu(),"Markdown"); return
        if data=="sig":
            await safe_edit(query,"📊 *Choose a pair*",symbol_menu("pick"),"Markdown"); return
        if data.startswith("pick:"):
            sym=data.split(":")[1]
            await safe_edit(query,f"{SYMBOLS.get(sym,sym)} — *Choose timeframe*",tf_menu(sym),"Markdown"); return
        if data.startswith("go:"):
            _,sym,tf=data.split(":")
            if not await gate(update,context): return
            await safe_edit(query,f"⏳ Analyzing {SYMBOLS.get(sym,sym)} ({tf_label(tf)})...")
            s=await asyncio.to_thread(build_signal,SYMBOLS.get(sym,sym),tf)
            sid=await asyncio.to_thread(save_signal,s,str(update.effective_user.id))
            await safe_edit(query,format_signal(s,sid),main_menu(),"Markdown"); return
        if data=="accuracy":
            r=accuracy_report()
            txt=("🎯 *ACCURACY*\n\n"
                 f"Resolved: {r['resolved']}\nWIN: {r['wins']}\nLOSS: {r['losses']}\n"
                 f"Historical hit-rate: {r['accuracy_percent']}%\n\n"
                 "Only resolved bot signals are counted. No fabricated accuracy.")
            await safe_edit(query,txt,main_menu(),"Markdown"); return
        if data=="backtest":
            await safe_edit(query,"⏳ Running walk-forward backtest...")
            rows=await asyncio.to_thread(backtest_report,None,None)
            text="📈 *BACKTEST*\n\n"+"\n".join(
                f"{x.get('symbol')} {x.get('interval')}: {x.get('accuracy_percent','N/A')}% ({x.get('tested',0)} tested)"
                for x in rows[:24]
            )
            await safe_edit(query,text,main_menu(),"Markdown"); return
        if data=="timing":
            await safe_edit(query,
                "⏰ *Entry Timing*\n\nUse:\n`/timing EURUSD 45m`\n\n"
                "The bot calculates the next candle boundary and tells you how many minutes remain.",
                main_menu(),"Markdown"); return
        if data=="best":
            await safe_edit(query,"⏳ Scanning pairs and timeframes for the strongest setup...")
            items=[]
            for sym in list(SYMBOLS.values())[:12]:
                for tf in ["15min","30min","35min","45min","1hour"]:
                    try:
                        s=await asyncio.to_thread(build_signal,sym,tf)
                        if s["direction"]!="WAIT": items.append(s)
                    except Exception: pass
            items.sort(key=lambda x:x["quality_score"],reverse=True)
            await safe_edit(query,format_best(items[:5]),main_menu(),"Markdown"); return
        if data=="scan":
            await safe_edit(query,"⏳ Scanning supported pairs...")
            items=[]
            for sym in list(SYMBOLS.values())[:12]:
                try:
                    s=await asyncio.to_thread(build_signal,sym,"15min")
                    items.append(s)
                except Exception: pass
            items.sort(key=lambda x:x["quality_score"],reverse=True)
            text="🔍 *SCAN RESULTS*\n\n"+"\n".join(
                f"{x['symbol']}: {x['direction']} — {x['quality_score']}/100"
                for x in items[:15]
            ) or "No valid market data returned."
            await safe_edit(query,text,main_menu(),"Markdown"); return
        if data=="mtf":
            await safe_edit(query,"🧭 *Multi-Timeframe*\n\nUse `/mtf EURUSD`",main_menu(),"Markdown"); return
        if data=="history":
            with DB_LOCK:
                con=db(); rows=con.execute(
                    "SELECT symbol,interval,direction,quality_score,result,created_at FROM signals ORDER BY id DESC LIMIT 15"
                ).fetchall(); con.close()
            text="📜 *SIGNAL HISTORY*\n\n"+"\n".join(
                f"{r['symbol']} {r['interval']} — {r['direction']} {r['quality_score']}/100 — {r['result']}"
                for r in rows
            ) if rows else "No saved signals yet."
            await safe_edit(query,text,main_menu(),"Markdown"); return
        if data=="news":
            n=await asyncio.to_thread(fetch_news)
            if not n["configured"]:
                txt=f"📰 News filter: {n['message']}"
            else:
                ev=n["events"]
                txt="📰 *UPCOMING ECONOMIC EVENTS*\n\n"+"\n".join(
                    f"{x.get('time','?')} | {x.get('country','?')} | {x.get('event','?')} | {x.get('impact','?')}"
                    for x in ev[:10]
                ) or "No medium/high-impact events returned."
            await safe_edit(query,txt,main_menu(),"Markdown"); return
        if data=="corr":
            await safe_edit(query,"🔗 Correlation tool is available via:\n`/corr EURUSD GBPUSD 15m`",main_menu(),"Markdown"); return
        if data=="premium":
            await safe_edit(query,
                "💎 *PREMIUM*\n\n"
                "Premium features:\n"
                "• All supported timeframes\n• Best Setup scanner\n• Advanced MTF\n"
                "• Historical accuracy\n• Backtest\n• Personal watchlist alerts\n\n"
                f"Price: ₹{PRICE_INR}/month or AED {PRICE_AED}/month\n\n"
                "Configure payment details in .env.",
                main_menu(),"Markdown"); return
    except Exception as e:
        log.exception("callback failed")
        await safe_edit(query,"❌ Temporary analysis error. Please try again.",main_menu())


async def timing_cmd(update,context):
    try:
        if not context.args:
            await safe_reply(update,"Example: /timing EURUSD 45m",main_menu()); return
        sym=normalize_symbol(context.args[0]); tf=normalize_interval(context.args[1] if len(context.args)>1 else "5m")
        t=timeframe_timing(tf)
        await safe_reply(update,
            f"⏰ *ENTRY TIMING*\n\n💱 {sym}\n⏱ {tf_label(tf)}\n"
            f"Current candle: {t['current_candle_start'].strftime('%H:%M UTC')}\n"
            f"Next candle: *{t['next_candle_start'].strftime('%H:%M UTC')}*\n"
            f"Wait: *{t['wait_minutes']} minutes*\n"
            f"Duration: *{t['duration_minutes']} minutes*\n\n"
            "Timing tells when the candle opens; it does not guarantee its direction.",
            main_menu(),"Markdown")
    except Exception as e:
        await safe_reply(update,f"❌ Timing error: {str(e)[:300]}",main_menu())

async def mtf_cmd(update,context):
    if not context.args:
        await safe_reply(update,"Example: /mtf EURUSD",main_menu()); return
    if not await gate(update,context): return
    try:
        sym=normalize_symbol(context.args[0])
        m=await asyncio.to_thread(mtf_analysis,sym)
        text=f"🧭 *MTF {sym}*\n\nConsensus: *{m['consensus']}*\nAgreement: {m['agreement']}/{m['available']}\n\n"
        for tf,a in m["timeframes"].items():
            text += f"{tf}: {a.get('direction','NO DATA')}\n"
        await safe_reply(update,text,main_menu(),"Markdown")
    except Exception as e:
        await safe_reply(update,f"❌ MTF error: {str(e)[:300]}",main_menu())

async def scan_cmd(update,context):
    if not await gate(update,context): return
    try:
        items=[]
        for sym in list(SYMBOLS.values())[:12]:
            try: items.append(await asyncio.to_thread(build_signal,sym,"15min"))
            except Exception: pass
        items.sort(key=lambda x:x["quality_score"],reverse=True)
        text="🔍 *SCAN*\n\n"+"\n".join(
            f"{x['symbol']} — {x['direction']} — {x['quality_score']}/100"
            for x in items
        ) or "No valid data."
        await safe_reply(update,text,main_menu(),"Markdown")
    except Exception as e:
        await safe_reply(update,f"❌ Scan error: {str(e)[:300]}",main_menu())

async def history_cmd(update,context):
    with DB_LOCK:
        con=db(); rows=con.execute(
            "SELECT symbol,interval,direction,quality_score,result,created_at FROM signals ORDER BY id DESC LIMIT 20"
        ).fetchall(); con.close()
    text="📜 *HISTORY*\n\n"+"\n".join(
        f"{r['symbol']} {r['interval']} | {r['direction']} | {r['quality_score']}/100 | {r['result']}"
        for r in rows
    ) if rows else "No signals yet."
    await safe_reply(update,text,main_menu(),"Markdown")

async def news_cmd(update,context):
    n=await asyncio.to_thread(fetch_news)
    if not n["configured"]:
        await safe_reply(update,f"📰 {n['message']}",main_menu()); return
    text="📰 *NEWS*\n\n"+"\n".join(
        f"{x.get('time','?')} | {x.get('country','?')} | {x.get('event','?')} | {x.get('impact','?')}"
        for x in n["events"][:15]
    ) or "No medium/high impact events found."
    await safe_reply(update,text,main_menu(),"Markdown")

async def corr_cmd(update,context):
    if len(context.args)<2:
        await safe_reply(update,"Example: /corr EURUSD GBPUSD 15m",main_menu()); return
    try:
        a=normalize_symbol(context.args[0]); b=normalize_symbol(context.args[1]); tf=normalize_interval(context.args[2] if len(context.args)>2 else "15m")
        da=await asyncio.to_thread(fetch_candles,a,tf,250); dbb=await asyncio.to_thread(fetch_candles,b,tf,250)
        n=min(len(da),len(dbb))
        corr=float(da["close"].pct_change().tail(n).corr(dbb["close"].pct_change().tail(n)))
        await safe_reply(update,f"🔗 *CORRELATION*\n\n{a} ↔ {b}\nTimeframe: {tf_label(tf)}\nCorrelation: *{corr:.3f}*\n\nCorrelation is confirmation only, not a trade signal.",main_menu(),"Markdown")
    except Exception as e:
        await safe_reply(update,f"❌ Correlation error: {str(e)[:300]}",main_menu())

async def premium_cmd(update,context):
    uid=str(update.effective_user.id)
    with DB_LOCK:
        con=db(); sub=con.execute("SELECT * FROM subscriptions WHERE telegram_id=?",(uid,)).fetchone(); con.close()
    status="ACTIVE" if sub and sub["expires_at"] and utc_now()<datetime.fromisoformat(sub["expires_at"]) else "NOT ACTIVE"
    await safe_reply(update,f"💎 *PREMIUM STATUS*\n\nStatus: *{status}*\n\nConfigure payment and approve users with `/approve USER_ID DAYS`.",main_menu(),"Markdown")

async def approve_cmd(update,context):
    if not ADMIN_TELEGRAM_ID or str(update.effective_user.id) != str(ADMIN_TELEGRAM_ID):
        await safe_reply(update,"Unauthorized."); return
    if len(context.args)<2:
        await safe_reply(update,"Usage: /approve USER_ID DAYS"); return
    uid=str(context.args[0]); days=int(context.args[1])
    exp=utc_now()+timedelta(days=days)
    with DB_LOCK:
        con=db()
        con.execute("INSERT INTO subscriptions(telegram_id,expires_at,is_active) VALUES(?,?,1) "
                    "ON CONFLICT(telegram_id) DO UPDATE SET expires_at=excluded.expires_at,is_active=1",
                    (uid,exp.isoformat()))
        con.commit(); con.close()
    await safe_reply(update,f"✅ Premium activated for {uid} until {exp.strftime('%Y-%m-%d %H:%M UTC')}.")

async def addwatch_cmd(update,context):
    if len(context.args)<1:
        await safe_reply(update,"Example: /addwatch EURUSD"); return
    uid=str(update.effective_user.id); sym=normalize_symbol(context.args[0])
    with DB_LOCK:
        con=db(); con.execute("INSERT OR IGNORE INTO watchlist_items(telegram_id,symbol) VALUES(?,?)",(uid,sym)); con.commit(); con.close()
    await safe_reply(update,f"✅ Added {sym} to your premium watchlist.")

async def mywatch_cmd(update,context):
    uid=str(update.effective_user.id)
    with DB_LOCK:
        con=db(); rows=con.execute("SELECT symbol,last_alert_at FROM watchlist_items WHERE telegram_id=?",(uid,)).fetchall(); con.close()
    await safe_reply(update,"⭐ *MY WATCHLIST*\n\n"+("\n".join(r["symbol"] for r in rows) if rows else "Empty."),main_menu(),"Markdown")

async def delwatch_cmd(update,context):
    if not context.args:
        await safe_reply(update,"Example: /delwatch EURUSD"); return
    uid=str(update.effective_user.id); sym=normalize_symbol(context.args[0])
    with DB_LOCK:
        con=db(); con.execute("DELETE FROM watchlist_items WHERE telegram_id=? AND symbol=?",(uid,sym)); con.commit(); con.close()
    await safe_reply(update,f"✅ Removed {sym}.")

async def premium_watch_loop():
    while True:
        try:
            with DB_LOCK:
                con=db()
                users=con.execute("""
                    SELECT DISTINCT w.telegram_id,w.symbol,w.last_alert_at
                    FROM watchlist_items w
                    JOIN subscriptions s ON s.telegram_id=w.telegram_id
                    WHERE s.is_active=1
                """).fetchall(); con.close()
            tg=TG_REF["app"]
            if tg:
                for row in users:
                    try:
                        if row["last_alert_at"]:
                            last=datetime.fromisoformat(row["last_alert_at"])
                            if utc_now()-last < timedelta(minutes=PREMIUM_ALERT_COOLDOWN_MINUTES):
                                continue
                        s=await asyncio.to_thread(build_signal,row["symbol"],"5min")
                        if s["direction"]!="WAIT" and s["quality_score"]>=PREMIUM_ALERT_THRESHOLD:
                            await tg.bot.send_message(chat_id=row["telegram_id"],
                                text="🔔 *PREMIUM HIGH-QUALITY SETUP*\n\n"+format_signal(s),
                                parse_mode="Markdown")
                            with DB_LOCK:
                                con=db(); con.execute("UPDATE watchlist_items SET last_alert_at=? WHERE telegram_id=? AND symbol=?",
                                    (iso_now(),row["telegram_id"],row["symbol"])); con.commit(); con.close()
                    except Exception as e:
                        log.warning("Premium alert error: %s",e)
        except Exception as e:
            log.warning("Premium loop: %s",e)
        await asyncio.sleep(PREMIUM_SCAN_INTERVAL_SECONDS)

async def resolver_loop():
    while True:
        try: await asyncio.to_thread(resolve_pending_signals)
        except Exception as e: log.warning("Resolver: %s",e)
        await asyncio.sleep(RESOLUTION_INTERVAL_SECONDS)


# ------------------------- API -------------------------

@app.get("/health")
def health():
    return {
        "status":"ok",
        "version":"5.0.0",
        "telegram_configured":bool(TELEGRAM_BOT_TOKEN),
        "market_data_configured":bool(TWELVE_DATA_API_KEY),
        "news_configured":bool(FINNHUB_API_KEY),
        "sklearn_available":SKLEARN_OK,
        "timeframes":SUPPORTED_TF,
    }

@app.get("/api/signal")
def api_signal(symbol: str=Query("EUR/USD"), interval: str=Query("5m")):
    try:
        s=build_signal(symbol,interval)
        sid=save_signal(s,"api")
        s["signal_id"]=sid
        return s
    except Exception as e:
        raise HTTPException(status_code=400,detail=str(e))

@app.get("/api/accuracy")
def api_accuracy():
    try: resolve_pending_signals()
    except Exception: pass
    return accuracy_report()

@app.get("/api/backtest")
def api_backtest(symbol: Optional[str]=None, interval: Optional[str]=None):
    return {"reports":backtest_report(symbol,interval)}

@app.get("/api/timing")
def api_timing(symbol: str=Query("EUR/USD"), interval: str=Query("45m")):
    tf=normalize_interval(interval)
    t=timeframe_timing(tf)
    return {"symbol":normalize_symbol(symbol),"timeframe":tf_label(tf),**{
        "current_candle_start":t["current_candle_start"].isoformat(),
        "next_candle_start":t["next_candle_start"].isoformat(),
        "wait_minutes":t["wait_minutes"],"duration_minutes":t["duration_minutes"]
    }}

@app.get("/api/news")
def api_news():
    return fetch_news()

@app.get("/api/history")
def api_history(limit:int=Query(50,ge=1,le=500)):
    with DB_LOCK:
        con=db(); rows=con.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?",(limit,)).fetchall(); con.close()
    return [dict(r) for r in rows]

@app.get("/api/assets")
def api_assets():
    return {"assets":list(SYMBOLS.values()),"timeframes":[tf_label(x) for x in SUPPORTED_TF]}

@app.get("/",response_class=HTMLResponse)
def home():
    return """
<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NexCandle AI V5</title>
<style>
body{font-family:Arial;background:#0b1020;color:#fff;padding:18px}
.card{max-width:900px;margin:12px auto;background:#151c32;padding:20px;border-radius:16px}
button,select{padding:12px;border-radius:10px;margin:4px}
button{background:#3b63ff;color:#fff;border:0;font-weight:bold}
pre{white-space:pre-wrap;background:#0d1427;padding:16px;border-radius:12px}
</style></head><body>
<div class="card"><h1>⚡ NexCandle AI V5</h1>
<p>Real-data technical/ML analysis. No guaranteed future prediction.</p>
<select id="s"><option>EUR/USD</option><option>GBP/USD</option><option>USD/JPY</option><option>USD/CAD</option><option>XAU/USD</option></select>
<select id="t"><option>5m</option><option>15m</option><option>30m</option><option>35m</option><option>45m</option><option>1h</option></select>
<button onclick="go()">Get Signal</button><pre id="out">Ready.</pre></div>
<script>
async function go(){
 const s=document.getElementById('s').value,t=document.getElementById('t').value;
 document.getElementById('out').textContent='Analyzing...';
 try{
  const r=await fetch('/api/signal?symbol='+encodeURIComponent(s)+'&interval='+t);
  const d=await r.json(); if(!r.ok) throw new Error(d.detail||'Request failed');
  document.getElementById('out').textContent=
`NEXCANDLE AI
${d.symbol} — ${d.timeframe}

Direction: ${d.direction}
Quality: ${d.quality_score}/100
ML probability UP: ${d.ml_probability ?? 'N/A'}%
Technical score: ${d.technical_score}
MTF: ${d.mtf_agreement}/${d.mtf_available}
Regime: ${d.regime}

ENTRY TIMING
${d.action}
Next candle: ${d.entry_time}
Duration: ${d.duration_minutes} minutes

Price: ${d.price}
RSI: ${d.rsi}
ADX: ${d.adx}

Historical accuracy is shown separately in /api/accuracy.`;
 }catch(e){document.getElementById('out').textContent='Error: '+e.message}
}
</script></body></html>
"""

async def run_telegram():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_OK:
        log.warning("Telegram disabled: token/library missing.")
        return
    tg=Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    tg.add_handler(CommandHandler("start",start_cmd))
    tg.add_handler(CommandHandler("help",help_cmd))
    tg.add_handler(CommandHandler("signal",signal_cmd))
    tg.add_handler(CommandHandler("accuracy",accuracy_cmd))
    tg.add_handler(CommandHandler("backtest",backtest_cmd))
    tg.add_handler(CommandHandler("timing",timing_cmd))
    tg.add_handler(CommandHandler("mtf",mtf_cmd))
    tg.add_handler(CommandHandler("scan",scan_cmd))
    tg.add_handler(CommandHandler("history",history_cmd))
    tg.add_handler(CommandHandler("news",news_cmd))
    tg.add_handler(CommandHandler("corr",corr_cmd))
    tg.add_handler(CommandHandler("premium",premium_cmd))
    tg.add_handler(CommandHandler("mystatus",premium_cmd))
    tg.add_handler(CommandHandler("approve",approve_cmd))
    tg.add_handler(CommandHandler("addwatch",addwatch_cmd))
    tg.add_handler(CommandHandler("mywatch",mywatch_cmd))
    tg.add_handler(CommandHandler("delwatch",delwatch_cmd))
    tg.add_handler(CallbackQueryHandler(button))
    TG_REF["app"]=tg
    await tg.initialize()
    await tg.start()
    await tg.updater.start_polling(drop_pending_updates=True)
    log.info("Telegram bot started.")
    while True:
        await asyncio.sleep(3600)

def start_async_thread(coro):
    def runner():
        asyncio.run(coro())
    t=threading.Thread(target=runner,daemon=True)
    t.start()

if __name__=="__main__":
    start_async_thread(run_telegram)
    start_async_thread(resolver_loop)
    start_async_thread(premium_watch_loop)
    uvicorn.run(app,host=HOST,port=PORT)
