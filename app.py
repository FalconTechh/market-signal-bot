import os
import time
import sqlite3
import threading
import asyncio
import logging
import math
import html
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple

import numpy as np
import pandas as pd
import requests
from flask import Flask
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, MessageHandler, ContextTypes, filters

# ============================================================
# NexCandle AI v2 - production-oriented Telegram signal bot
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
TWELVEDATA_API_KEY = (os.getenv("TWELVEDATA_API_KEY") or os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TWELVE_DATA_API_K") or "").strip()
ADMIN_ID = (os.getenv("ADMIN_TELEGRAM_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
DB = os.getenv("DATABASE_PATH", "nexcandle.db")
PREMIUM_DAYS = int(os.getenv("PREMIUM_DAYS", "30"))
ALERT_SEC = max(30, int(os.getenv("ALERT_INTERVAL_SECONDS") or os.getenv("SCAN_INTERVAL") or "60"))
INDIA_UPI = (os.getenv("INDIA_UPI") or os.getenv("UPI_ID") or os.getenv("PAYMENT_UPI") or "6361472511").strip()
UAE_BOTIM = (os.getenv("UAE_BOTIM") or os.getenv("BOTIM_NUMBER") or os.getenv("PAYMENT_BOTIM") or "0522445121").strip()
PREMIUM_PRICE = (os.getenv("PREMIUM_PRICE") or os.getenv("PREMIUM_SCAN_PRICE") or "").strip()
MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", "72"))
ENTRY_CONFIRM_SECONDS = max(5, int(os.getenv("ENTRY_CONFIRM_SECONDS", "10")))
ENTRY_WINDOW_SECONDS = max(15, int(os.getenv("ENTRY_WINDOW_SECONDS", "45")))
STALE_MULTIPLIER = max(1.5, float(os.getenv("STALE_DATA_MULTIPLIER", "2.5")))
CACHE_SECONDS = int(os.getenv("MARKET_CACHE_SECONDS", "20"))

PAIRS = {
    "EUR/USD": {"finnhub": "OANDA:EUR_USD", "yahoo": "EURUSD=X", "td": "EUR/USD"},
    "GBP/USD": {"finnhub": "OANDA:GBP_USD", "yahoo": "GBPUSD=X", "td": "GBP/USD"},
    "USD/JPY": {"finnhub": "OANDA:USD_JPY", "yahoo": "JPY=X", "td": "USD/JPY"},
    "USD/CHF": {"finnhub": "OANDA:USD_CHF", "yahoo": "CHF=X", "td": "USD/CHF"},
    "AUD/USD": {"finnhub": "OANDA:AUD_USD", "yahoo": "AUDUSD=X", "td": "AUD/USD"},
    "USD/CAD": {"finnhub": "OANDA:USD_CAD", "yahoo": "CAD=X", "td": "USD/CAD"},
    "NZD/USD": {"finnhub": "OANDA:NZD_USD", "yahoo": "NZDUSD=X", "td": "NZD/USD"},
    "EUR/GBP": {"finnhub": "OANDA:EUR_GBP", "yahoo": "EURGBP=X", "td": "EUR/GBP"},
    "EUR/JPY": {"finnhub": "OANDA:EUR_JPY", "yahoo": "EURJPY=X", "td": "EUR/JPY"},
    "GBP/JPY": {"finnhub": "OANDA:GBP_JPY", "yahoo": "GBPJPY=X", "td": "GBP/JPY"},
}
TF_MIN = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "45m": 45, "1h": 60, "4h": 240}
TF_SECONDS = {k: v * 60 for k, v in TF_MIN.items()}

S = requests.Session()
S.headers.update({"User-Agent": "NexCandleAI/2.0 market-data-client"})
LOCK = threading.RLock()
MARKET_CACHE: Dict[Tuple[str, str], Tuple[float, pd.DataFrame, str]] = {}
USER_STATE: Dict[int, Dict[str, Any]] = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nexcandle")

web = Flask(__name__)

@web.get("/")
def home():
    return {"service": "NexCandle AI", "version": "2.1 Advanced", "status": "online"}

@web.get("/health")
def health():
    provider = []
    if FINNHUB_API_KEY:
        provider.append("finnhub")
    if TWELVEDATA_API_KEY:
        provider.append("twelvedata")
    provider.append("yahoo_fallback")
    return {"status": "ok", "version": "2.1", "providers": provider,
            "cache_items": len(MARKET_CACHE), "database": DB,
            "payment_methods": {"india_upi": bool(INDIA_UPI), "uae_botim": bool(UAE_BOTIM)}}

def run_web():
    web.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")), debug=False, use_reloader=False)

# ---------------- DB ----------------

def con():
    c = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def init():
    with LOCK:
        c = con()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            premium_until TEXT,
            free_signals INTEGER DEFAULT 3,
            alerts INTEGER DEFAULT 0,
            alert_score INTEGER DEFAULT 80,
            alert_tf TEXT DEFAULT '5m',
            alert_direction TEXT DEFAULT 'BOTH',
            created TEXT,
            updated TEXT
        );
        CREATE TABLE IF NOT EXISTS payments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            amount TEXT,
            method TEXT,
            reference TEXT,
            proof_file_id TEXT,
            status TEXT DEFAULT 'pending',
            admin_note TEXT,
            created TEXT,
            reviewed TEXT
        );
        CREATE TABLE IF NOT EXISTS signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            pair TEXT,
            tf TEXT,
            direction TEXT,
            score INTEGER,
            entry REAL,
            stop REAL,
            target REAL,
            rr REAL,
            candle TEXT,
            created TEXT,
            resolved TEXT DEFAULT 'PENDING',
            resolved_at TEXT,
            result_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS alert_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            pair TEXT,
            tf TEXT,
            direction TEXT,
            candle TEXT,
            UNIQUE(user_id,pair,tf,direction,candle)
        );
        CREATE TABLE IF NOT EXISTS system_logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT,
            component TEXT,
            message TEXT,
            created TEXT
        );
        """)
        # Lightweight migrations for databases created by v1.
        migrations = {
            "users": {"alert_score":"INTEGER DEFAULT 80", "alert_tf":"TEXT DEFAULT '5m'", "alert_direction":"TEXT DEFAULT 'BOTH'", "updated":"TEXT"},
            "payments": {"amount":"TEXT", "method":"TEXT", "proof_file_id":"TEXT", "admin_note":"TEXT", "reviewed":"TEXT"},
            "signals": {
                "resolved_at":"TEXT", "result_reason":"TEXT",
                "entry_start":"TEXT", "entry_end":"TEXT",
                "valid_until":"TEXT", "provider":"TEXT", "data_age":"INTEGER",
                "confidence_label":"TEXT"
            }
        }
        for table, cols in migrations.items():
            existing = {r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}
            for col, spec in cols.items():
                if col not in existing:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {spec}")
        c.commit(); c.close()

def db_log(level, component, message):
    log.log(getattr(logging, level.upper(), logging.INFO), "%s: %s", component, message)
    try:
        with LOCK:
            c = con(); c.execute("INSERT INTO system_logs(level,component,message,created) VALUES(?,?,?,?)", (level, component, str(message)[:1000], now_iso())); c.commit(); c.close()
    except Exception:
        pass

def ensure_user(tg_user):
    with LOCK:
        c = con()
        c.execute("""INSERT INTO users(user_id,username,first_name,free_signals,created,updated)
        VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name,updated=excluded.updated""",
                  (tg_user.id, tg_user.username or "", tg_user.first_name or "", 3, now_iso(), now_iso()))
        c.commit(); c.close()

def is_admin(uid):
    return bool(ADMIN_ID) and str(uid) == str(ADMIN_ID)

def premium_until(uid):
    if is_admin(uid):
        return datetime.now(timezone.utc) + timedelta(days=3650)
    with LOCK:
        c = con(); r = c.execute("SELECT premium_until FROM users WHERE user_id=?", (uid,)).fetchone(); c.close()
    if not r or not r["premium_until"]:
        return None
    try:
        dt = datetime.fromisoformat(r["premium_until"])
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def premium(uid):
    dt = premium_until(uid)
    return bool(dt and dt > datetime.now(timezone.utc))

def user_row(uid):
    with LOCK:
        c = con(); r = c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone(); c.close()
    return r

def free_ok(uid):
    if premium(uid): return True
    r = user_row(uid)
    return bool(r and r["free_signals"] > 0)

def consume(uid):
    if premium(uid): return
    with LOCK:
        c = con(); c.execute("UPDATE users SET free_signals=MAX(free_signals-1,0),updated=? WHERE user_id=?", (now_iso(), uid)); c.commit(); c.close()

# ---------------- Market data ----------------

def _clean_df(df):
    if df is None or df.empty: return pd.DataFrame()
    df = df.copy()
    cols = {c.lower(): c for c in df.columns}
    needed = {}
    for name in ("open", "high", "low", "close"):
        if name in cols: needed[name] = cols[name]
    if len(needed) < 4: return pd.DataFrame()
    out = df[[needed["open"], needed["high"], needed["low"], needed["close"]]].copy()
    out.columns = ["open", "high", "low", "close"]
    out = out.apply(pd.to_numeric, errors="coerce").dropna()
    out.index = pd.to_datetime(out.index, utc=True, errors="coerce")
    out = out[~out.index.isna()].sort_index()
    return out[~out.index.duplicated(keep="last")]

def _finnhub(pair, tf, limit):
    if not FINNHUB_API_KEY: raise RuntimeError("Finnhub key not configured")
    # Finnhub supports 1/5/15/30/60/ D/W/M. 45m is built from validated 15m candles.
    base_tf = 15 if tf == "45m" else (60 if tf == "4h" else TF_MIN[tf])
    if base_tf not in (1,5,15,30,60):
        raise RuntimeError("Unsupported Finnhub resolution")
    now = int(time.time())
    span = max(400, limit * 5)
    start = now - base_tf * 60 * span
    r = S.get("https://finnhub.io/api/v1/forex/candle", params={"symbol": PAIRS[pair]["finnhub"], "resolution": base_tf, "from": start, "to": now, "token": FINNHUB_API_KEY}, timeout=12)
    if r.status_code != 200:
        raise RuntimeError(f"Finnhub HTTP {r.status_code}")
    d = r.json()
    if d.get("s") != "ok" or not d.get("t"):
        raise RuntimeError(f"Finnhub status: {d.get('s','unknown')}")
    x = pd.DataFrame({"open": d["o"], "high": d["h"], "low": d["l"], "close": d["c"]}, index=pd.to_datetime(d["t"], unit="s", utc=True))
    x = _clean_df(x)
    if tf == "45m":
        x = x.resample("45min", origin="epoch", label="left", closed="left").agg({"open":"first","high":"max","low":"min","close":"last"}).dropna()
    elif tf == "4h":
        x = x.resample("4h", origin="epoch", label="left", closed="left").agg({"open":"first","high":"max","low":"min","close":"last"}).dropna()
    return x.tail(limit)

def _twelvedata(pair, tf, limit):
    if not TWELVEDATA_API_KEY: raise RuntimeError("TwelveData key not configured")
    intervals = {"1m":"1min","5m":"5min","15m":"15min","30m":"30min","45m":"45min","1h":"1h","4h":"4h"}
    r = S.get("https://api.twelvedata.com/time_series", params={"symbol": PAIRS[pair]["td"], "interval": intervals[tf], "outputsize": min(max(limit, 200), 5000), "timezone":"UTC", "apikey":TWELVEDATA_API_KEY}, timeout=15)
    if r.status_code != 200: raise RuntimeError(f"TwelveData HTTP {r.status_code}")
    d = r.json()
    if "values" not in d: raise RuntimeError(d.get("message", "TwelveData returned no data"))
    x = pd.DataFrame(d["values"])
    x["datetime"] = pd.to_datetime(x["datetime"], utc=True)
    x = x.set_index("datetime").sort_index()
    return _clean_df(x).tail(limit)

def _yahoo(pair, tf, limit):
    # Public Yahoo chart endpoint is used only as a fallback. It may be delayed and is not a broker feed.
    intervals = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","45m":"15m","1h":"60m","4h":"60m"}
    interval = intervals[tf]
    # Yahoo limits 1m to a short period; request a bounded window.
    seconds = {"1m": 2*86400, "5m": 10*86400, "15m": 30*86400, "30m": 45*86400, "45m": 60*86400, "1h": 180*86400, "4h": 365*86400}[tf]
    now = int(time.time()); start = now - seconds
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{PAIRS[pair]['yahoo']}"
    r = S.get(url, params={"period1":start,"period2":now,"interval":interval,"events":"history","includeAdjustedClose":"true"}, timeout=15)
    if r.status_code != 200: raise RuntimeError(f"Yahoo HTTP {r.status_code}")
    js = r.json(); res = js.get("chart",{}).get("result")
    if not res: raise RuntimeError("Yahoo returned no chart data")
    res = res[0]
    q = res.get("indicators",{}).get("quote",[{}])[0]
    x = pd.DataFrame({"open":q.get("open",[]),"high":q.get("high",[]),"low":q.get("low",[]),"close":q.get("close",[])}, index=pd.to_datetime(res.get("timestamp",[]),unit="s",utc=True))
    x = _clean_df(x)
    if tf == "45m":
        x = x.resample("45min", origin="epoch", label="left", closed="left").agg({"open":"first","high":"max","low":"min","close":"last"}).dropna()
    elif tf == "4h":
        x = x.resample("4h", origin="epoch", label="left", closed="left").agg({"open":"first","high":"max","low":"min","close":"last"}).dropna()
    return x.tail(limit)

def candles(pair, tf, limit=350):
    if pair not in PAIRS or tf not in TF_MIN: raise ValueError("Unsupported pair/timeframe")
    key = (pair, tf)
    with LOCK:
        cached = MARKET_CACHE.get(key)
        if cached and time.time() - cached[0] <= CACHE_SECONDS and len(cached[1]) >= min(limit, 100):
            return cached[1].tail(limit).copy()
    errors=[]
    providers=[]
    if FINNHUB_API_KEY: providers.append(("finnhub", _finnhub))
    if TWELVEDATA_API_KEY: providers.append(("twelvedata", _twelvedata))
    providers.append(("yahoo", _yahoo))
    for name, fn in providers:
        try:
            x=fn(pair,tf,limit)
            if len(x) < 80: raise RuntimeError(f"insufficient candles ({len(x)})")
            # Remove a potentially incomplete last candle for stable technical analysis.
            interval = TF_SECONDS[tf]
            last_epoch = int(x.index[-1].timestamp())
            if int(time.time()) - last_epoch < interval and len(x)>80:
                x=x.iloc[:-1]
            with LOCK: MARKET_CACHE[key]=(time.time(),x.copy(),name)
            return x.tail(limit).copy()
        except Exception as e:
            errors.append(f"{name}: {e}")
            db_log("warning","market",errors[-1])
    raise RuntimeError("Live market data is temporarily unavailable. Please retry shortly.")

def provider_status():
    return [name for name,key in (("Finnhub",FINNHUB_API_KEY),("TwelveData",TWELVEDATA_API_KEY)) if key] + ["Yahoo fallback"]

# ---------------- Indicators / strategy ----------------

def ema(s,n): return s.ewm(span=n, adjust=False).mean()
def rsi(s,n=14):
    d=s.diff(); g=d.clip(lower=0); l=-d.clip(upper=0)
    ag=g.ewm(alpha=1/n,adjust=False).mean(); al=l.ewm(alpha=1/n,adjust=False).mean()
    rs=ag/al.replace(0,np.nan)
    return 100-100/(1+rs)
def atr(x,n=14):
    p=x.close.shift(); tr=pd.concat([x.high-x.low,(x.high-p).abs(),(x.low-p).abs()],axis=1).max(axis=1)
    return tr.ewm(alpha=1/n,adjust=False).mean()
def calc(x):
    x=x.copy()
    x["e9"]=ema(x.close,9); x["e21"]=ema(x.close,21); x["e50"]=ema(x.close,50); x["e200"]=ema(x.close,200)
    x["rsi"]=rsi(x.close); x["atr"]=atr(x)
    m=ema(x.close,12)-ema(x.close,26); x["macd"]=m; x["ms"]=ema(m,9)
    x["bb"]=x.close.rolling(20).mean(); sd=x.close.rolling(20).std(); x["bu"]=x.bb+2*sd; x["bl"]=x.bb-2*sd
    x["vol_ratio"]=((x.high-x.low)/x.close).replace([np.inf,-np.inf],np.nan)
    return x.dropna()

def analyse(x):
    x=calc(x)
    if len(x)<10: raise RuntimeError("Not enough validated candles")
    a=x.iloc[-1]; p=x.iloc[-2]; bull=bear=0; why=[]
    if a.e9>a.e21>a.e50: bull+=22; why.append("EMA structure bullish")
    elif a.e9<a.e21<a.e50: bear+=22; why.append("EMA structure bearish")
    else: why.append("EMA structure mixed")
    if a.close>a.e200: bull+=12; why.append("price above EMA200")
    else: bear+=12; why.append("price below EMA200")
    if a.macd>a.ms and a.macd>=p.macd: bull+=15; why.append("MACD momentum positive")
    elif a.macd<a.ms and a.macd<=p.macd: bear+=15; why.append("MACD momentum negative")
    else: why.append("MACD momentum mixed")
    if 52<=a.rsi<=68: bull+=13; why.append("RSI supports bullish continuation")
    elif 32<=a.rsi<=48: bear+=13; why.append("RSI supports bearish continuation")
    elif a.rsi>72 or a.rsi<28: why.append("RSI extreme - caution")
    if a.close>a.bb: bull+=8
    elif a.close<a.bb: bear+=8
    # recent candle structure
    body=abs(a.close-a.open); rng=max(a.high-a.low,1e-12)
    if a.close>a.open and body/rng>=0.55: bull+=8; why.append("strong bullish candle")
    elif a.close<a.open and body/rng>=0.55: bear+=8; why.append("strong bearish candle")
    # volatility sanity
    if np.isfinite(a.atr) and a.atr>0: 
        if 0.00005 <= float(a.atr/a.close) <= 0.02: 
            bull+=2; bear+=2
    gap=abs(bull-bear)
    if gap<18: direction="WAIT"
    else: direction="CALL" if bull>bear else "PUT"
    score=int(min(100, max(bull,bear)*1.05))
    return direction, score, why, x

def mtf(pair, entry_tf="5m"):
    # Confirmation uses the entry TF plus progressively higher TFs. 4H is never required for 1M.
    hierarchy={"1m":["1m","5m","15m","30m"],"5m":["5m","15m","30m","1h"],"15m":["15m","30m","1h","4h"],"30m":["30m","1h","4h"],"45m":["45m","1h","4h"],"1h":["1h","4h"],"4h":["4h"]}
    weights=[1.0,1.3,1.6,2.0]
    out={}; totals={"CALL":0.0,"PUT":0.0}; available=0
    for i,tf in enumerate(hierarchy[entry_tf]):
        try:
            d,s,why,_=analyse(candles(pair,tf)); out[tf]=(d,s); available+=1
            if d in totals: totals[d]+=s*weights[min(i,len(weights)-1)]
        except Exception:
            out[tf]=("UNAVAILABLE",0)
    if available==0: raise RuntimeError("No validated MTF data available")
    if totals["CALL"] > totals["PUT"]*1.14: final="CALL"
    elif totals["PUT"] > totals["CALL"]*1.14: final="PUT"
    else: final="WAIT"
    score=int(min(100, max(totals.values())/(sum(weights[:available])*0.95)))
    return final, score, out

def candle_boundary(tf):
    n=TF_SECONDS[tf]; now=int(time.time()); return n-(now%n)

def fmt_duration(seconds):
    seconds=max(0,int(seconds)); return f"{seconds//3600}h {(seconds%3600)//60}m {seconds%60}s" if seconds>=3600 else f"{seconds//60}m {seconds%60}s"

def market_meta(pair, tf, x=None):
    """Return freshness information for the last completed candle."""
    if x is None:
        x = candles(pair, tf)
    if x.empty:
        raise RuntimeError("No market candles available")
    interval = TF_SECONDS[tf]
    last_start = int(x.index[-1].timestamp())
    candle_close = last_start + interval
    age = max(0, int(time.time()) - candle_close)
    provider = MARKET_CACHE.get((pair, tf), (0, None, "unknown"))[2]
    fresh = age <= int(interval * STALE_MULTIPLIER)
    return {"age": age, "fresh": fresh, "provider": provider, "last_start": last_start, "candle_close": candle_close}

def entry_window(tf):
    interval = TF_SECONDS[tf]
    now = int(time.time())
    next_start = now - (now % interval) + interval
    start = next_start + ENTRY_CONFIRM_SECONDS
    end = min(next_start + ENTRY_WINDOW_SECONDS, next_start + interval - 5)
    if end < start:
        end = start
    return next_start, start, end

def fmt_clock(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%H:%M:%S UTC")

def fmt_signal_timing(s):
    return (f"🕐 Next candle: <b>{fmt_clock(s['next_candle'])}</b>\n"
            f"🎯 Entry window: <b>{fmt_clock(s['entry_start'])} → {fmt_clock(s['entry_end'])}</b>\n"
            f"⏳ Window duration: <b>{max(0, s['entry_end']-s['entry_start'])}s</b>")

def score_label(score, direction):
    if direction == "WAIT":
        return "NO TRADE"
    if score >= 88:
        return "VERY STRONG"
    if score >= 80:
        return "STRONG"
    if score >= 72:
        return "MODERATE"
    return "WEAK"

def make_signal(pair, tf):
    """Build one validated setup from completed candles only."""
    x = candles(pair, tf)
    meta = market_meta(pair, tf, x)
    if not meta["fresh"]:
        raise RuntimeError(f"Market data is stale ({fmt_duration(meta['age'])} old)")

    d, local_score, why, x = analyse(x)
    md, mtf_score, mtf_map = mtf(pair, tf)

    # Consensus is deliberately conservative: local and MTF must agree.
    final = d if d == md else "WAIT"
    score = int(min(100, max(0, round(local_score * 0.55 + mtf_score * 0.45))))

    # Penalise missing higher-timeframe confirmations rather than pretending
    # unavailable data is confirmation.
    unavailable = sum(1 for direction, _ in mtf_map.values() if direction == "UNAVAILABLE")
    if unavailable:
        score = max(0, score - unavailable * 6)

    if final == "WAIT" or score < MIN_SIGNAL_SCORE:
        final = "WAIT"

    entry = stop = target = rr = None
    next_candle, entry_start, entry_end = entry_window(tf)

    if final != "WAIT":
        e = float(x.close.iloc[-1])
        v = max(float(x.atr.iloc[-1]), e * 0.00008)
        risk = 1.15 * v
        reward = 1.85 * v
        if final == "CALL":
            stop, target = e - risk, e + reward
        else:
            stop, target = e + risk, e - reward
        entry, rr = e, reward / risk

    return {
        "pair": pair, "tf": tf, "direction": final, "score": score,
        "label": score_label(score, final), "why": why, "mtf": mtf_map,
        "wait": max(0, next_candle - int(time.time())),
        "entry": entry, "stop": stop, "target": target, "rr": rr,
        "candle": x.index[-1].isoformat(), "provider": meta["provider"],
        "data_age": meta["age"], "fresh": meta["fresh"],
        "next_candle": next_candle, "entry_start": entry_start,
        "entry_end": entry_end, "valid_until": entry_end
    }


def fmt_signal(s, detailed=True):
    lines = [
        "⚡ <b>NEXCANDLE AI — ADVANCED SIGNAL</b>",
        f"💱 <b>{html.escape(s['pair'])}</b> • ⏱ <b>{s['tf']}</b>",
        "",
        f"📊 Setup Quality: <b>{s['score']}/100 — {s['label']}</b>",
        f"📡 Data: <b>{html.escape(str(s['provider']))}</b> • Age: <b>{fmt_duration(s['data_age'])}</b>",
    ]

    if s["direction"] == "WAIT":
        lines += [
            "",
            "🟡 <b>NO TRADE / WAIT</b>",
            "The local setup and higher-timeframe confirmation are not aligned strongly enough.",
            "⛔ Do not force a CALL/PUT just because a signal was requested.",
            "",
            "🕐 <b>NEXT CANDLE TIMING</b>",
            fmt_signal_timing(s),
        ]
    else:
        lines += [
            "",
            f"🎯 <b>DIRECTION: {s['direction']}</b>",
            "",
            "🕐 <b>ENTRY PLAN</b>",
            fmt_signal_timing(s),
            f"⏳ Valid until: <b>{fmt_clock(s['valid_until'])}</b>",
            "",
            f"📍 Reference price: <code>{s['entry']:.6f}</code>",
            f"🛑 SL reference: <code>{s['stop']:.6f}</code>",
            f"🎯 TP reference: <code>{s['target']:.6f}</code>",
            f"⚖️ Risk/Reward: <b>1:{s['rr']:.2f}</b>",
            "",
            "✅ Take the setup only while the confirmation remains valid inside the entry window.",
            "🚫 If the window expires or direction/confirmation changes → SKIP and wait for a fresh signal.",
        ]

    if detailed:
        lines += ["", "🧭 <b>MTF CONFIRMATION</b>"]
        for tf, (direction, sc) in s["mtf"].items():
            icon = "✅" if direction == s["direction"] and direction != "WAIT" else ("⚪" if direction == "WAIT" else "❌")
            if direction == "UNAVAILABLE":
                icon = "⚠️"
            lines.append(f"{icon} {tf}: {direction} ({sc}/100)")
        lines += [
            "",
            "🧠 <b>ANALYSIS FACTORS</b>",
            "• " + "\n• ".join(s["why"][:8]),
            "",
            "⚠️ Setup Quality is a technical score, not a guaranteed probability or profit forecast."
        ]
    return "\n".join(lines)

# ---------------- UI ----------------

WELCOME = """⚡ <b>NexCandle AI</b>\n\nWelcome! 👋\n\nLive market analysis, multi-timeframe confirmation, entry timing, scanner, backtesting, performance tracking and premium alerts — all from the buttons below.\n\nYou do <b>not</b> need to type /start. Any first message will open this dashboard.\n\n⚠️ No system can guarantee the next candle or future profit."""

def menu(uid):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Get Signal",callback_data="signal"),InlineKeyboardButton("🧭 MTF",callback_data="mtf")],
        [InlineKeyboardButton("🔥 Best Setup",callback_data="best"),InlineKeyboardButton("🔎 Scan",callback_data="scan")],
        [InlineKeyboardButton("⏰ Entry Timing",callback_data="timing"),InlineKeyboardButton("🎯 Accuracy",callback_data="accuracy")],
        [InlineKeyboardButton("📈 Backtest",callback_data="backtest"),InlineKeyboardButton("📜 History",callback_data="history")],
        [InlineKeyboardButton("🔔 Alerts",callback_data="alerts"),InlineKeyboardButton("💎 Premium",callback_data="premium")],
        [InlineKeyboardButton("👤 My Account",callback_data="account"),InlineKeyboardButton("ℹ️ Help",callback_data="help")],
    ])

def back_menu(): return InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Dashboard",callback_data="menu")]])

def pairmenu(mode):
    ps=list(PAIRS); rows=[]
    for i in range(0,len(ps),2): rows.append([InlineKeyboardButton(p,callback_data=f"{mode}:{p.replace('/','~')}") for p in ps[i:i+2]])
    rows.append([InlineKeyboardButton("« Back",callback_data="menu")])
    return InlineKeyboardMarkup(rows)

def tfmenu(prefix, pair=None):
    rows=[]
    for group in (("1m","5m","15m"),("30m","45m","1h"),("4h",)):
        rows.append([InlineKeyboardButton(t,callback_data=f"{prefix}:{pair.replace('/','~')}:{t}" if pair else f"{prefix}:{t}") for t in group])
    rows.append([InlineKeyboardButton("« Back",callback_data="menu")])
    return InlineKeyboardMarkup(rows)

def admin_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Pending Payments",callback_data="admin:payments"),InlineKeyboardButton("👥 Users",callback_data="admin:users")],
        [InlineKeyboardButton("📊 Stats",callback_data="admin:stats"),InlineKeyboardButton("🩺 Health",callback_data="admin:health")],
        [InlineKeyboardButton("« Dashboard",callback_data="menu")]
    ])

async def send_welcome(update, uid=None):
    if update.message:
        await update.message.reply_text(WELCOME,parse_mode=ParseMode.HTML,reply_markup=menu(uid or update.effective_user.id))

async def notify_admin_payment(context, payment_id):
    if not ADMIN_ID:
        return
    try:
        with LOCK:
            c=con(); p=c.execute("SELECT * FROM payments WHERE id=?",(payment_id,)).fetchone(); c.close()
        if not p: return
        user_label=("@"+p["username"]) if p["username"] else str(p["user_id"])
        txt=(f"🔔 <b>NEW PAYMENT REQUEST</b>\n\n🧾 Payment ID: <code>{p['id']}</code>\n"
             f"👤 User: {html.escape(user_label)}\n🆔 User ID: <code>{p['user_id']}</code>\n"
             f"💳 Method: <b>{p['method']}</b>\n💰 Amount: <b>{p['amount'] or '—'}</b>\n"
             f"🔖 Reference: <code>{html.escape(p['reference'] or "—")}</code>\n🕐 Submitted: {p['created']}")
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("✅ APPROVE",callback_data=f"approve:{payment_id}"),InlineKeyboardButton("❌ REJECT",callback_data=f"reject:{payment_id}")],
                                 [InlineKeyboardButton("💳 Pending Payments",callback_data="admin:payments")]])
        await context.bot.send_message(chat_id=int(ADMIN_ID),text=txt,parse_mode=ParseMode.HTML,reply_markup=kb)
        if p["proof_file_id"]:
            await context.bot.send_photo(chat_id=int(ADMIN_ID),photo=p["proof_file_id"],caption=f"🧾 Payment proof #{p['id']}")
    except Exception as e:
        db_log("warning","payment_admin_notify",e)

async def any_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user: return
    ensure_user(update.effective_user)
    uid=update.effective_user.id
    text=(update.message.text or "").strip() if update.message else ""
    state=USER_STATE.get(uid,{})
    # Payment reference / amount / preference flows
    if state.get("action")=="payment_reference" and update.message and update.message.photo:
        method=state.get("method","unknown")
        proof=update.message.photo[-1].file_id
        with LOCK:
            c=con(); c.execute("INSERT INTO payments(user_id,username,amount,method,reference,proof_file_id,status,created) VALUES(?,?,?,?,?,?,?,?)",(uid,update.effective_user.username or "",PREMIUM_PRICE,method,"PHOTO_PROOF",proof,"pending",now_iso())); c.commit(); c.close()
        USER_STATE.pop(uid,None)
        with LOCK:
            cc=con(); rid=cc.execute("SELECT id FROM payments WHERE user_id=? ORDER BY id DESC LIMIT 1",(uid,)).fetchone(); cc.close()
        if rid: await notify_admin_payment(context,rid["id"])
        await update.message.reply_text("✅ <b>Payment proof submitted</b>\n\nYour payment has been sent to admin for verification. Admin approval will activate Premium.",parse_mode=ParseMode.HTML,reply_markup=menu(uid)); return
    if state.get("action")=="payment_reference" and text and not text.startswith("/"):
        method=state.get("method","unknown")
        with LOCK:
            c=con(); c.execute("INSERT INTO payments(user_id,username,amount,method,reference,status,created) VALUES(?,?,?,?,?,?,?)",(uid,update.effective_user.username or "",PREMIUM_PRICE,method,text[:120],"pending",now_iso())); c.commit(); c.close()
        USER_STATE.pop(uid,None)
        with LOCK:
            cc=con(); rid=cc.execute("SELECT id FROM payments WHERE user_id=? ORDER BY id DESC LIMIT 1",(uid,)).fetchone(); cc.close()
        if rid: await notify_admin_payment(context,rid["id"])
        await update.message.reply_text("✅ <b>Payment submitted</b>\n\nYour transaction reference has been sent to admin for verification. Admin approval will activate Premium.",parse_mode=ParseMode.HTML,reply_markup=menu(uid)); return
    # Any text including /start opens the same dashboard.
    await send_welcome(update,uid)

# ---------------- Feature handlers ----------------

async def cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer(); uid=q.from_user.id; ensure_user(q.from_user); d=q.data
    try:
        if d=="menu": return await q.message.reply_text(WELCOME,parse_mode=ParseMode.HTML,reply_markup=menu(uid))
        if d=="signal": return await q.message.reply_text("📊 <b>Choose currency pair</b>",parse_mode=ParseMode.HTML,reply_markup=pairmenu("signal"))
        if d.startswith("signal:"):
            p=d.split(":",1)[1].replace("~","/"); return await q.message.reply_text(f"⏱ <b>{p}</b> — choose entry timeframe",parse_mode=ParseMode.HTML,reply_markup=tfmenu("run",p))
        if d.startswith("run:"):
            _,p,t=d.split(":"); p=p.replace("~","/")
            if not free_ok(uid): return await q.message.reply_text("🔒 Your free signal allowance is finished. Upgrade to Premium for full access.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💎 Premium",callback_data="premium")],[InlineKeyboardButton("« Back",callback_data="menu")]]))
            await q.message.reply_text("⏳ <b>Analysing validated market data + MTF confirmation…</b>",parse_mode=ParseMode.HTML)
            s=await asyncio.to_thread(make_signal,p,t)
            # A free credit is consumed only when a real CALL/PUT setup is returned.
            if s["direction"] != "WAIT":
                consume(uid)
                created = now_iso()
                with LOCK:
                    c=con()
                    c.execute("""INSERT INTO signals(
                        user_id,pair,tf,direction,score,entry,stop,target,rr,candle,created,
                        entry_start,entry_end,valid_until,provider,data_age,confidence_label
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (uid,p,t,s["direction"],s["score"],s["entry"],s["stop"],s["target"],s["rr"],
                     s["candle"],created,fmt_clock(s["entry_start"]),fmt_clock(s["entry_end"]),
                     fmt_clock(s["valid_until"]),s["provider"],s["data_age"],s["label"]))
                    c.commit(); c.close()
            return await q.message.reply_text(fmt_signal(s),parse_mode=ParseMode.HTML,reply_markup=menu(uid))
        if d=="mtf": return await q.message.reply_text("🧭 <b>Choose pair</b>",parse_mode=ParseMode.HTML,reply_markup=pairmenu("mtf"))
        if d.startswith("mtf:"):
            p=d.split(":",1)[1].replace("~","/"); f,sc,m=await asyncio.to_thread(mtf,p,"15m"); out=[f"🧭 <b>{p} — MTF</b>",f"Final consensus: <b>{f}</b>",f"Quality: <b>{sc}/100</b>",""]+[f"{k}: {v[0]} ({v[1]})" for k,v in m.items()]; return await q.message.reply_text("\n".join(out),parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="timing": return await q.message.reply_text("⏰ <b>Choose currency pair for entry timing</b>",parse_mode=ParseMode.HTML,reply_markup=pairmenu("timing"))
        if d.startswith("timing:"):
            p=d.split(":",1)[1].replace("~","/")
            return await q.message.reply_text(f"⏰ <b>{p}</b> — choose timeframe",parse_mode=ParseMode.HTML,reply_markup=tfmenu("time",p))
        if d.startswith("time:"):
            _,p,t=d.split(":"); p=p.replace("~","/")
            try:
                s=await asyncio.to_thread(make_signal,p,t)
                text=(f"⏰ <b>ENTRY TIMING — {p} {t}</b>\n\n"+fmt_signal_timing(s)+
                      f"\n\n📊 Current setup quality: <b>{s['score']}/100</b>\n📌 Current direction: <b>{s['direction']}</b>\n"+
                      f"📡 Data: <b>{s['provider']}</b> ({fmt_duration(s['data_age'])} old)\n\n"+
                      "⚠️ Timing is a market-analysis window, not a guarantee. If confirmation changes, wait.")
                return await q.message.reply_text(text,parse_mode=ParseMode.HTML,reply_markup=menu(uid))
            except Exception as e:
                db_log("warning","timing",f"{p}/{t}: {e}")
                return await q.message.reply_text("⏳ <b>Entry timing unavailable</b>\n\nFresh validated market data is not available right now.\n\nPlease retry shortly.",parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="best":
            if not free_ok(uid): return await q.message.reply_text("🔒 Best Setup is available with Premium.",reply_markup=back_menu())
            await q.message.reply_text("🔥 <b>Scanning the market…</b>",parse_mode=ParseMode.HTML)
            results=[]
            for p in PAIRS:
                try:
                    s=await asyncio.to_thread(make_signal,p,"5m")
                    if s["direction"]!="WAIT": results.append(s)
                except Exception as e: db_log("warning","best",f"{p}: {e}")
            results.sort(key=lambda z:z["score"],reverse=True)
            if not results: return await q.message.reply_text("🟡 <b>No qualifying setup</b>\n\nMarket conditions are not strong enough right now.",parse_mode=ParseMode.HTML,reply_markup=back_menu())
            top=results[:5]; text="🔥 <b>BEST SETUPS — 5M</b>\n\n"+"\n".join(f"{i+1}. {s['pair']} — <b>{s['direction']}</b> — {s['score']}/100" for i,s in enumerate(top))+"\n\nTap Get Signal for full entry/SL/TP analysis."; return await q.message.reply_text(text,parse_mode=ParseMode.HTML,reply_markup=menu(uid))
        if d=="scan":
            if not premium(uid): return await q.message.reply_text("🔒 Full Market Scanner is Premium.",reply_markup=back_menu())
            await q.message.reply_text("🔎 <b>Scanning all supported pairs…</b>",parse_mode=ParseMode.HTML)
            results=[]
            for p in PAIRS:
                try: results.append(await asyncio.to_thread(make_signal,p,"5m"))
                except Exception as e: db_log("warning","scan",f"{p}: {e}")
            results=[s for s in results if s["direction"]!="WAIT"]; results.sort(key=lambda z:z["score"],reverse=True)
            text="🔎 <b>MARKET SCANNER</b>\n\n"+("\n".join(f"{s['pair']} — {s['direction']} — {s['score']}/100" for s in results[:10]) or "No qualifying setup."); return await q.message.reply_text(text,parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="accuracy":
            with LOCK:
                c=con()
                rows=c.execute("SELECT resolved,COUNT(*) n FROM signals WHERE user_id=? GROUP BY resolved",(uid,)).fetchall()
                pair_rows=c.execute("""SELECT pair,tf,
                    SUM(CASE WHEN resolved='WIN' THEN 1 ELSE 0 END) wins,
                    SUM(CASE WHEN resolved='LOSS' THEN 1 ELSE 0 END) losses
                    FROM signals WHERE user_id=? AND resolved IN ('WIN','LOSS')
                    GROUP BY pair,tf ORDER BY (wins+losses) DESC LIMIT 6""",(uid,)).fetchall()
                c.close()
            stats={r["resolved"]:r["n"] for r in rows}
            w,l=stats.get("WIN",0),stats.get("LOSS",0)
            resolved=w+l
            rate=100*w/resolved if resolved else None
            pending=stats.get("PENDING",0); expired=stats.get("EXPIRED",0); ambiguous=stats.get("AMBIGUOUS",0)
            rate_text=f"{rate:.1f}%" if rate is not None else "Not enough resolved data"
            lines=[
                "🎯 <b>NEXCANDLE PERFORMANCE</b>","",
                f"📌 Resolved: <b>{resolved}</b>",
                f"✅ Wins: <b>{w}</b>",
                f"❌ Losses: <b>{l}</b>",
                f"📊 Measured win rate: <b>{rate_text}</b>",
                f"⏳ Pending: {pending}",
                f"⌛ Expired: {expired}",
                f"⚠️ Ambiguous: {ambiguous}",
                ""
            ]
            if pair_rows:
                lines.append("<b>PAIR / TIMEFRAME</b>")
                for r in pair_rows:
                    n=int(r["wins"] or 0)+int(r["losses"] or 0)
                    rr=100*int(r["wins"] or 0)/n if n else 0
                    lines.append(f"• {html.escape(r['pair'])} {r['tf']}: {rr:.1f}% ({n} resolved)")
            lines += ["","⚠️ This is measured historical tracking, not a future guarantee."]
            return await q.message.reply_text("\n".join(lines),parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="history":
            with LOCK:
                c=con(); rs=c.execute("SELECT pair,tf,direction,score,resolved,created FROM signals WHERE user_id=? ORDER BY id DESC LIMIT 12",(uid,)).fetchall(); c.close()
            lines=["📜 <b>RECENT SIGNAL HISTORY</b>",""]
            lines += [f"{r['pair']} {r['tf']} — {r['direction']} — {r['score']}/100 — {r['resolved']}" for r in rs] or ["No signals yet."]
            return await q.message.reply_text("\n".join(lines),parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="backtest":
            if not premium(uid): return await q.message.reply_text("🔒 Backtest is Premium.",reply_markup=back_menu())
            return await q.message.reply_text("📈 <b>Choose pair for backtest</b>",parse_mode=ParseMode.HTML,reply_markup=pairmenu("back"))
        if d.startswith("back:"):
            p=d.split(":",1)[1].replace("~","/"); return await q.message.reply_text(f"📈 <b>{p}</b> — choose timeframe",parse_mode=ParseMode.HTML,reply_markup=tfmenu("bt",p))
        if d.startswith("bt:"):
            _,p,t=d.split(":"); p=p.replace("~","/"); await q.message.reply_text("🧪 Running historical simulation…",parse_mode=ParseMode.HTML)
            x=await asyncio.to_thread(candles,p,t,700); wins=losses=0; gross_win=gross_loss=0.0
            for i in range(220,len(x)-1):
                try: dd,sc,_,xx=analyse(x.iloc[:i+1])
                except Exception: continue
                if dd=="WAIT" or sc<MIN_SIGNAL_SCORE: continue
                e=float(xx.close.iloc[-1]); v=max(float(xx.atr.iloc[-1]),e*0.00008); risk=1.15*v; reward=1.85*v
                horizon = max(2, min(6, int(round(3))))
                future=x.iloc[i+1:min(i+1+horizon,len(x))]
                if dd=="CALL":
                    hit_tp=(future.high>=e+reward).any(); hit_sl=(future.low<=e-risk).any()
                else:
                    hit_tp=(future.low<=e-reward).any(); hit_sl=(future.high>=e+risk).any()
                if hit_tp and not hit_sl: wins+=1; gross_win+=reward
                elif hit_sl and not hit_tp: losses+=1; gross_loss+=risk
            n=wins+losses; rate=100*wins/n if n else 0; pf=(gross_win/gross_loss) if gross_loss else (gross_win if gross_win else 0)
            out=f"📈 <b>BACKTEST — {p} {t}</b>\n\nSignals: {n}\n✅ Wins: {wins}\n❌ Losses: {losses}\n🎯 Hit rate: <b>{rate:.1f}%</b>\n⚖️ Approx profit factor: <b>{pf:.2f}</b>\n\n⚠️ Historical simulation is not a guarantee of future results."; return await q.message.reply_text(out,parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="premium": return await premium_screen(q,uid)
        if d=="payment_status":
            with LOCK:
                c=con(); r=c.execute("SELECT status,method,reference,created,reviewed FROM payments WHERE user_id=? ORDER BY id DESC LIMIT 1",(uid,)).fetchone(); c.close()
            if not r: text_status="📋 No payment submission found."
            else: text_status=f"📋 <b>PAYMENT STATUS</b>\n\nStatus: <b>{r['status'].upper()}</b>\nMethod: {html.escape(r['method'] or '—')}\nReference: <code>{html.escape(r['reference'] or '—')}</code>\nSubmitted: {r['created']}"
            return await q.message.reply_text(text_status,parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d.startswith("pay:"):
            method=d.split(":",1)[1]; USER_STATE[uid]={"action":"payment_reference","method":method}
            detail=INDIA_UPI if method=="India UPI" else UAE_BOTIM if method=="UAE BOTIM" else "Configured payment method"
            return await q.message.reply_text(f"💳 <b>{method}</b>\n\nPayment detail: <code>{detail}</code>\n\nAfter payment, send your transaction/reference ID or a payment proof screenshot.\n\nDo not send card PINs, passwords or OTPs.",parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="account":
            r=user_row(uid); pu=premium_until(uid); until=pu.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if pu else "Not active"
            rows=[[InlineKeyboardButton("👑 Admin Panel",callback_data="admin")]] if is_admin(uid) else []
            rows.append([InlineKeyboardButton("« Back",callback_data="menu")])
            return await q.message.reply_text(f"👤 <b>MY ACCOUNT</b>\n\nUser ID: <code>{uid}</code>\nMembership: <b>{'PREMIUM' if premium(uid) else 'FREE'}</b>\nPremium until: {until}\nFree signals remaining: {r['free_signals'] if r else 0}\nAlerts: {'ON' if r and r['alerts'] else 'OFF'}",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows))
        if d=="alerts":
            if not premium(uid): return await q.message.reply_text("🔒 Auto Alerts are Premium.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💎 Upgrade",callback_data="premium")],[InlineKeyboardButton("« Back",callback_data="menu")]]))
            return await q.message.reply_text("🔔 <b>ALERT SETTINGS</b>\n\nChoose a minimum setup score:",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("70+",callback_data="alertscore:70"),InlineKeyboardButton("80+",callback_data="alertscore:80"),InlineKeyboardButton("90+",callback_data="alertscore:90")],[InlineKeyboardButton("Toggle ON/OFF",callback_data="alerttoggle")],[InlineKeyboardButton("« Back",callback_data="menu")]]))
        if d.startswith("alertscore:"):
            score=int(d.split(":")[1]); with_lock_update_alert(uid,score); return await q.message.reply_text(f"✅ Minimum alert score set to {score}/100.",reply_markup=menu(uid))
        if d=="alerttoggle":
            r=user_row(uid); on=0 if r and r["alerts"] else 1
            with LOCK:
                c=con(); c.execute("UPDATE users SET alerts=?,updated=? WHERE user_id=?",(on,now_iso(),uid)); c.commit(); c.close()
            return await q.message.reply_text(f"🔔 Auto Alerts <b>{'ON' if on else 'OFF'}</b>",parse_mode=ParseMode.HTML,reply_markup=menu(uid))
        if d=="help":
            helptext="""ℹ️ <b>NexCandle AI HELP</b>\n\n<b>📊 Get Signal</b> — Pair + timeframe analysis with technical indicators and MTF confirmation.\n\n<b>⏰ Entry Timing</b> — Shows the next candle boundary and explains when not to chase a late entry.\n\n<b>🧭 MTF</b> — Higher-timeframe agreement check.\n\n<b>🔥 Best Setup</b> — Ranks strong setups instead of forcing a trade.\n\n<b>🎯 Accuracy</b> — Measured historical results from tracked signals.\n\n<b>📈 Backtest</b> — Historical simulation using the same technical rules.\n\n<b>🔔 Alerts</b> — Premium automatic setup notifications.\n\n<b>💎 Premium</b> — Submit payment reference for admin approval.\n\n<b>🛡 Data safety</b> — Provider failures are handled internally; raw API errors are not shown to users.\n\n⚠️ No system can guarantee the next candle or future profit."""; return await q.message.reply_text(helptext,parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="admin": return await q.message.reply_text("👑 <b>ADMIN PANEL</b>",parse_mode=ParseMode.HTML,reply_markup=admin_menu())
        if d.startswith("admin:") and is_admin(uid): return await admin_callback(q,d)
        if d.startswith("approve:") and is_admin(uid): return await review_payment(q,d,True)
        if d.startswith("reject:") and is_admin(uid): return await review_payment(q,d,False)
    except Exception as e:
        db_log("error","callback",f"{d}: {e}")
        await q.message.reply_text("⚠️ <b>Request could not be completed</b>\n\nFresh market/service data was unavailable or the request timed out. No fake result was generated.\n\nPlease return to the dashboard and retry.",parse_mode=ParseMode.HTML,reply_markup=back_menu())

def with_lock_update_alert(uid,score):
    with LOCK:
        c=con(); c.execute("UPDATE users SET alert_score=?,updated=? WHERE user_id=?",(score,now_iso(),uid)); c.commit(); c.close()

async def premium_screen(q,uid):
    if premium(uid):
        pu=premium_until(uid); until=pu.strftime("%Y-%m-%d %H:%M UTC") if pu else "active"
        text=f"💎 <b>PREMIUM ACTIVE</b>\n\nValid until: <b>{until}</b>\n\nYou have access to advanced scanner, backtest and automatic alerts."
        return await q.message.reply_text(text,parse_mode=ParseMode.HTML,reply_markup=back_menu())
    paylines=[]
    if INDIA_UPI: paylines.append("🇮🇳 India UPI available")
    if UAE_BOTIM: paylines.append("🇦🇪 UAE BOTIM Pay available")
    price=f"\n💰 Plan price: <b>{PREMIUM_PRICE}</b>" if PREMIUM_PRICE else ""
    text=("💎 <b>NEXCANDLE PREMIUM</b>\n\n"
          "Advanced scanner • Next-candle analysis • MTF confirmation • Backtest • Auto alerts • Performance tracking"
          +price+
          "\n\n<b>PAYMENT INSTRUCTIONS</b>\n"
          "1️⃣ Select India UPI or UAE BOTIM.\n"
          "2️⃣ Complete the payment using the displayed number.\n"
          "3️⃣ Send the exact <b>Transaction ID / Reference ID</b>.\n"
          "4️⃣ You may also send a payment-proof screenshot.\n"
          "5️⃣ Admin verifies the payment in Telegram.\n"
          "6️⃣ After approval, Premium is activated automatically.\n\n"
          "⚠️ Never send OTP, PIN or password.")
    rows=[]
    if INDIA_UPI: rows.append([InlineKeyboardButton("🇮🇳 Pay via India UPI",callback_data="pay:India UPI")])
    if UAE_BOTIM: rows.append([InlineKeyboardButton("🇦🇪 Pay via UAE BOTIM",callback_data="pay:UAE BOTIM")])
    if not rows:
        text += "\n\n⚠️ No payment method is configured. Add INDIA_UPI or UAE_BOTIM in Render Environment."
    rows += [[InlineKeyboardButton("📋 Payment Status",callback_data="payment_status")],[InlineKeyboardButton("« Back",callback_data="menu")]]
    return await q.message.reply_text(text,parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(rows))

async def review_payment(q,d,approve):
    pid=int(d.split(":")[1])
    with LOCK:
        c=con(); p=c.execute("SELECT * FROM payments WHERE id=?",(pid,)).fetchone()
        if not p: c.close(); return await q.message.reply_text("Payment not found.",reply_markup=admin_menu())
        if p["status"] != "pending":
            c.close()
            return await q.message.reply_text(f"Payment #{pid} is already {p['status']}.",reply_markup=admin_menu())
        status="approved" if approve else "rejected"; c.execute("UPDATE payments SET status=?,reviewed=? WHERE id=? AND status='pending'",(status,now_iso(),pid))
        if approve:
            until=datetime.now(timezone.utc)+timedelta(days=PREMIUM_DAYS); c.execute("UPDATE users SET premium_until=?,updated=? WHERE user_id=?",(until.isoformat(),now_iso(),p["user_id"]))
        c.commit(); c.close()
    try:
        if approve: await q.get_bot().send_message(p["user_id"],f"💎 <b>Premium Activated</b>\n\nValid until: {(datetime.now(timezone.utc)+timedelta(days=PREMIUM_DAYS)).strftime('%Y-%m-%d %H:%M UTC')}",parse_mode=ParseMode.HTML,reply_markup=menu(p["user_id"]))
        else: await q.get_bot().send_message(p["user_id"],"❌ Your payment was not approved. Please contact admin with the correct transaction reference.",reply_markup=menu(p["user_id"]))
    except Exception as e: db_log("warning","payment",e)
    return await q.message.reply_text(f"Payment #{pid} {'approved' if approve else 'rejected'}.",reply_markup=admin_menu())

async def admin_callback(q,d):
    action=d.split(":",1)[1]
    if action=="payments":
        with LOCK:
            c=con(); rows=c.execute("SELECT * FROM payments WHERE status='pending' ORDER BY id DESC LIMIT 10").fetchall(); c.close()
        if not rows: return await q.message.reply_text("💳 No pending payments.",reply_markup=admin_menu())
        for p in rows:
            txt=f"💳 <b>Payment #{p['id']}</b>\nUser: <code>{p['user_id']}</code> @{html.escape(p['username'] or '—')}\nMethod: {html.escape(p['method'] or '—')}\nAmount: {html.escape(p['amount'] or '—')}\nReference: <code>{html.escape(p['reference'] or '—')}</code>\nCreated: {p['created']}"
            kb=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Approve",callback_data=f"approve:{p['id']}"),InlineKeyboardButton("❌ Reject",callback_data=f"reject:{p['id']}")]])
            await q.message.reply_text(txt,parse_mode=ParseMode.HTML,reply_markup=kb)
        return
    if action=="users":
        with LOCK:
            c=con(); n=c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]; pr=c.execute("SELECT COUNT(*) n FROM users WHERE premium_until IS NOT NULL AND premium_until>?",(now_iso(),)).fetchone()["n"]; c.close()
        return await q.message.reply_text(f"👥 Users: {n}\n💎 Active premium: {pr}",reply_markup=admin_menu())
    if action=="stats":
        with LOCK:
            c=con(); n=c.execute("SELECT COUNT(*) n FROM signals").fetchone()["n"]; w=c.execute("SELECT COUNT(*) n FROM signals WHERE resolved='WIN'").fetchone()["n"]; p=c.execute("SELECT COUNT(*) n FROM payments WHERE status='pending'").fetchone()["n"]; c.close()
        return await q.message.reply_text(f"📊 Signals: {n}\n✅ Wins tracked: {w}\n💳 Pending payments: {p}",reply_markup=admin_menu())
    if action=="health":
        return await q.message.reply_text("🩺 <b>SYSTEM HEALTH</b>\n\nBot: ✅\nDatabase: ✅\nProviders: "+", ".join(provider_status())+f"\nCache: {len(MARKET_CACHE)} entries\nAlerts interval: {ALERT_SEC}s",parse_mode=ParseMode.HTML,reply_markup=admin_menu())

# ---------------- Alert worker ----------------

async def alerts_loop(app: Application):
    while True:
        try:
            with LOCK:
                c=con(); users=c.execute("SELECT user_id,alert_score,alert_tf,alert_direction FROM users WHERE alerts=1").fetchall(); c.close()
            for row in users:
                uid=row["user_id"]
                if not premium(uid): continue
                tf=row["alert_tf"] or "5m"; minscore=int(row["alert_score"] or 80); direction=row["alert_direction"] or "BOTH"
                for p in PAIRS:
                    try:
                        s=make_signal(p,tf)
                        if s["direction"]=="WAIT" or s["score"]<minscore or (direction!="BOTH" and s["direction"]!=direction): continue
                        fresh=False
                        with LOCK:
                            c=con()
                            try:
                                c.execute("INSERT INTO alert_events(user_id,pair,tf,direction,candle) VALUES(?,?,?,?,?)",(uid,p,tf,s["direction"],s["candle"])); c.commit(); fresh=True
                            except sqlite3.IntegrityError: pass
                            c.close()
                        if fresh:
                            await app.bot.send_message(uid,"🔔 <b>PREMIUM AUTO ALERT</b>\n\n"+fmt_signal(s),parse_mode=ParseMode.HTML,reply_markup=menu(uid))
                    except Exception as e:
                        db_log("warning","alerts",f"{p}/{tf}: {e}")
        except Exception as e: db_log("error","alerts",e)
        await asyncio.sleep(ALERT_SEC)


async def signal_resolver_loop(app: Application):
    """Resolve signals using candles that formed AFTER the signal candle.

    This avoids the old bug where only the latest candle was checked, which
    could leave many signals permanently PENDING and make Accuracy look like 0.
    """
    while True:
        try:
            with LOCK:
                c=con()
                rows=c.execute(
                    "SELECT * FROM signals WHERE resolved='PENDING' AND entry IS NOT NULL ORDER BY id ASC LIMIT 100"
                ).fetchall()
                c.close()

            for r in rows:
                try:
                    created=datetime.fromisoformat(r["created"])
                    if created.tzinfo is None:
                        created=created.replace(tzinfo=timezone.utc)

                    tf=r["tf"]
                    # A signal is evaluated for a bounded validity period.
                    max_age=max(TF_SECONDS.get(tf,300)*3, 180)
                    age=(datetime.now(timezone.utc)-created).total_seconds()

                    x=candles(r["pair"],tf,120)
                    if x.empty:
                        continue

                    signal_candle=pd.to_datetime(r["candle"],utc=True,errors="coerce")
                    if pd.isna(signal_candle):
                        signal_candle=x.index[-1]

                    future=x[x.index > signal_candle].copy()
                    direction=r["direction"]
                    stop=float(r["stop"])
                    target=float(r["target"])

                    result=None
                    reason=""

                    # Evaluate every completed future candle, not just the latest one.
                    for _, row in future.iterrows():
                        if direction=="CALL":
                            hit_tp=float(row.high)>=target
                            hit_sl=float(row.low)<=stop
                        else:
                            hit_tp=float(row.low)<=target
                            hit_sl=float(row.high)>=stop

                        if hit_tp and hit_sl:
                            result="AMBIGUOUS"
                            reason="Both target and stop were touched in the same observed candle; intrabar order is unknown."
                            break
                        if hit_tp:
                            result="WIN"
                            reason="Target reached in a completed future candle."
                            break
                        if hit_sl:
                            result="LOSS"
                            reason="Stop level reached in a completed future candle."
                            break

                    if result is None and age > max_age:
                        result="EXPIRED"
                        reason="Entry validity period ended without target/stop confirmation."

                    if result:
                        with LOCK:
                            c=con()
                            c.execute(
                                "UPDATE signals SET resolved=?,resolved_at=?,result_reason=? WHERE id=? AND resolved='PENDING'",
                                (result,now_iso(),reason,r["id"])
                            )
                            c.commit()
                            c.close()

                except Exception as e:
                    db_log("warning","resolver",f"signal {r['id']}: {e}")

        except Exception as e:
            db_log("error","resolver",e)

        await asyncio.sleep(max(20, min(ALERT_SEC,60)))


async def post_init(app):
    app.create_task(alerts_loop(app))
    app.create_task(signal_resolver_loop(app))

# ---------------- Main ----------------

def main():
    if not BOT_TOKEN: raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    init()
    threading.Thread(target=run_web,daemon=True).start()
    app=Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    # No /start dependency. Commands and ordinary text/photos are handled by the same welcome handler.
    app.add_handler(CallbackQueryHandler(cb))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO, any_message))
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__": main()
