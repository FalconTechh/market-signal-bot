import os
import time
import sqlite3
import threading
import asyncio
import logging
import math
import html
import json
import statistics
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Tuple

BUILD_ID = "V40-PRO-FOREX-CRYPTO-BINANCE-HYPER-15FX-10CRYPTO-MAHIM-2026-09-01"

import numpy as np
import pandas as pd
import requests

# ============================================================
# EARLY NUMERIC NORMALIZER
# Must be defined BEFORE any candle helper can reference it.
# This prevents the deployed-runtime NameError seen in V31.
# ============================================================
def _co_num(x, d=0.0):
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except (TypeError, ValueError, OverflowError):
        return d

# Backward-compatible alias for any legacy helper references.
coin_num = _co_num

# V32 DEFINITIVE COMPLETED-CANDLE HELPER
def completed_candles_v30(candles):
    out=[]
    for c in candles or []:
        if c.get("isOpen") is True or c.get("open_candle") is True:
            continue
        o=_co_num(c.get("open")); h=_co_num(c.get("high"))
        l=_co_num(c.get("low")); cl=_co_num(c.get("close"))
        if h > l and l <= min(o,cl) <= max(o,cl) <= h:
            out.append(c)
    return out

try:
    import websocket
except Exception:
    websocket = None
from flask import Flask, request, jsonify, Response, session
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, MessageHandler, ContextTypes, filters

# ============================================================
# NexCandle AI PRO V2 Data Stability Patch - Next Candle Engine
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
TWELVEDATA_API_KEY = (os.getenv("TWELVEDATA_API_KEY") or os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TWELVE_DATA_API_K") or "").strip()
OANDA_API_TOKEN = (os.getenv("OANDA_API_TOKEN") or "").strip()
OANDA_ACCOUNT_ID = (os.getenv("OANDA_ACCOUNT_ID") or "").strip()
OANDA_ENVIRONMENT = (os.getenv("OANDA_ENVIRONMENT") or "live").strip().lower()
ALPHAVANTAGE_API_KEY = (os.getenv("ALPHAVANTAGE_API_KEY") or os.getenv("ALPHA_VANTAGE_API_KEY") or "").strip()
SIFTING_API_KEY = (os.getenv("SIFTING_API_KEY") or os.getenv("SIFTING_KEY") or "").strip()
# SiftingIO is opt-in because its free quota can be exhausted; never hammer a capped account.
SIFTING_API_ENABLED = (os.getenv("SIFTING_API_ENABLED", "false").strip().lower() not in ("0", "false", "no", "off"))
DUKASCOPY_ENABLED = (os.getenv("DUKASCOPY_ENABLED", "true").strip().lower()
                     not in ("0", "false", "no", "off"))
SIFTING_WS_ENABLED = (os.getenv("SIFTING_WS_ENABLED", "false").strip().lower() not in ("0", "false", "no", "off"))
SIFTING_WS_SYMBOLS = [x.strip().replace("/", "").upper() for x in (os.getenv("SIFTING_WS_SYMBOLS") or "EURUSD,GBPUSD,USDJPY,AUDUSD,USDCAD").split(",") if x.strip()][:5]
RENDER_EXTERNAL_HOSTNAME = (os.getenv("RENDER_EXTERNAL_HOSTNAME") or "").strip()
RENDER_EXTERNAL_URL = (RENDER_EXTERNAL_HOSTNAME or os.getenv("RENDER_EXTERNAL_URL") or "").strip()
# IMPORTANT: Render web services must NEVER use getUpdates/polling.
# A stale BOT_MODE=polling variable can otherwise recreate Telegram 409 conflicts.
_requested_bot_mode = (os.getenv("BOT_MODE") or "").strip().lower()
_ON_RENDER = bool(RENDER_EXTERNAL_HOSTNAME or RENDER_EXTERNAL_URL)
if _ON_RENDER:
    if _requested_bot_mode and _requested_bot_mode != "webhook":
        pass
    BOT_MODE = "webhook"
else:
    BOT_MODE = _requested_bot_mode or "polling"
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
ADMIN_ID = (os.getenv("ADMIN_TELEGRAM_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
DB = os.getenv("DATABASE_PATH", "nexcandle.db")
PREMIUM_DAYS = int(os.getenv("PREMIUM_DAYS", "30"))
ALERT_SEC = max(30, int(os.getenv("ALERT_INTERVAL_SECONDS") or os.getenv("SCAN_INTERVAL") or "60"))
INDIA_UPI = (os.getenv("INDIA_UPI") or os.getenv("UPI_ID") or os.getenv("PAYMENT_UPI") or "6361472511").strip()
UAE_BOTIM = (os.getenv("UAE_BOTIM") or os.getenv("BOTIM_NUMBER") or os.getenv("PAYMENT_BOTIM") or "0522445121").strip()
PREMIUM_PRICE = (os.getenv("PREMIUM_PRICE") or os.getenv("PREMIUM_SCAN_PRICE") or "").strip()
ACCESS_CODE = (os.getenv("NEXCANDLE_ACCESS_CODE") or os.getenv("ACCESS_CODE") or "2580").strip().strip('"').strip("'")
# Normalize accidental spaces/formatting while preserving leading zeroes.
if not (ACCESS_CODE.isdigit() and len(ACCESS_CODE) == 4):
    ACCESS_CODE = "2580"
FLASK_SECRET = os.getenv("FLASK_SECRET_KEY") or os.getenv("SECRET_KEY") or "nexcandle-pro-session-change-me"
MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", "72"))
ENTRY_CONFIRM_SECONDS = max(5, int(os.getenv("ENTRY_CONFIRM_SECONDS", "10")))
ENTRY_WINDOW_SECONDS = max(15, int(os.getenv("ENTRY_WINDOW_SECONDS", "45")))
STALE_MULTIPLIER = max(1.5, float(os.getenv("STALE_DATA_MULTIPLIER", "2.5")))
CACHE_SECONDS = int(os.getenv("MARKET_CACHE_SECONDS", "180"))
# Stable market feed controls
PROVIDER_RETRY_LIMIT = max(1, int(os.getenv("PROVIDER_RETRY_LIMIT", "3")))
PROVIDER_TIMEOUT_SECONDS = max(3, int(os.getenv("PROVIDER_TIMEOUT_SECONDS", "10")))

# Emergency recovery: use last validated candles during temporary provider outages.
# This never creates fake prices; it only reuses previously validated market data.
ALLOW_STALE_CANDLE_FALLBACK = (os.getenv("ALLOW_STALE_CANDLE_FALLBACK", "true").lower() not in ("0","false","no"))
# Shorter cache windows near the next-candle boundary keep signals fresh without
# hammering free providers. Higher timeframes can safely reuse candles longer.
CACHE_TTL_BY_TF = {
    "1m": max(3, int(os.getenv("CACHE_TTL_1M", "6"))),
    "5m": max(10, int(os.getenv("CACHE_TTL_5M", "30"))),
    "1h": max(30, int(os.getenv("CACHE_TTL_1H", "120"))),
}
# Safe default order: BiQuote is the primary no-key OHLC feed because its
# documented REST API supports 1m/5m/15m/30m/1h/4h directly. Credentialed
# providers are fallbacks; Dukascopy is deliberately later because its public
# historical endpoint does not expose every target timeframe directly.
PROVIDER_ORDER = [x.strip().lower() for x in
                  (os.getenv("DATA_PROVIDER_ORDER") or
                   "biquote,yahoo2,twelvedata,finnhub,alphavantage,dukascopy,yahoo").split(",")
                  if x.strip()]
# Timeframe-aware routing: some free feeds expose excellent 5m candles but
# are weak/limited on 1m or higher intervals. Keep 5m as the main public lane,
# prefer direct 1m sources for 1m, and allow deterministic local aggregation
# from validated lower/higher base candles when a direct interval fails.
TF_PROVIDER_PREFERENCE = {
    "1m": ["biquote", "yahoo2", "yahoo", "twelvedata", "finnhub", "dukascopy"],
    "5m": ["biquote", "yahoo2", "yahoo", "twelvedata", "finnhub", "dukascopy"],
    "1h": ["biquote", "dukascopy", "yahoo2", "twelvedata", "finnhub", "yahoo"],
}
ALLOW_YAHOO_FALLBACK = (os.getenv("ALLOW_YAHOO_FALLBACK", "true").strip().lower()
                        not in ("0", "false", "no", "off"))
MARKET_FETCH_LIMIT = max(120, int(os.getenv("MARKET_FETCH_LIMIT", "350")))
MTF_BASE_LIMIT = max(500, int(os.getenv("MTF_BASE_LIMIT", "1200")))
SIGNAL_CACHE_SECONDS = max(10, int(os.getenv("SIGNAL_CACHE_SECONDS", "20")))
SIGNAL_CACHE_TTL_BY_TF = {"1m": 8, "5m": 12, "1h": SIGNAL_CACHE_SECONDS}
PROVIDER_COOLDOWN_SECONDS = max(10, int(os.getenv("PROVIDER_COOLDOWN_SECONDS", "45")))
PROVIDER_ERROR_COOLDOWN_SECONDS = max(PROVIDER_COOLDOWN_SECONDS, int(os.getenv("PROVIDER_ERROR_COOLDOWN_SECONDS", "180")))
PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS = max(120, int(os.getenv("PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS", "300")))
MTF_CACHE_SECONDS = max(10, int(os.getenv("MTF_CACHE_SECONDS", "45")))
PROVIDER_MIN_INTERVAL_SECONDS = max(0.0, float(os.getenv("PROVIDER_MIN_INTERVAL_SECONDS", "0.75")))
TRADE_HORIZON_CANDLES = max(1, int(os.getenv("TRADE_HORIZON_CANDLES", "1")))

PUBLIC_PAIRS = [
    # Fixed 15-pair public universe. No dynamic/extra symbols.
    "GBP/JPY", "AUD/CAD", "AUD/CHF", "AUD/JPY", "AUD/USD",
    "CAD/CHF", "CAD/JPY", "EUR/CAD", "EUR/CHF", "EUR/GBP",
    "EUR/USD", "GBP/CAD", "GBP/CHF", "USD/CAD", "USD/JPY",
]

PAIRS = {
    "GBP/JPY": {"finnhub": "OANDA:GBP_JPY", "yahoo": "GBPJPY=X", "td": "GBP/JPY"},
    "AUD/CAD": {"finnhub": "OANDA:AUD_CAD", "yahoo": "AUDCAD=X", "td": "AUD/CAD"},
    "AUD/CHF": {"finnhub": "OANDA:AUD_CHF", "yahoo": "AUDCHF=X", "td": "AUD/CHF"},
    "AUD/JPY": {"finnhub": "OANDA:AUD_JPY", "yahoo": "AUDJPY=X", "td": "AUD/JPY"},
    "AUD/USD": {"finnhub": "OANDA:AUD_USD", "yahoo": "AUDUSD=X", "td": "AUD/USD"},
    "CAD/CHF": {"finnhub": "OANDA:CAD_CHF", "yahoo": "CADCHF=X", "td": "CAD/CHF"},
    "CAD/JPY": {"finnhub": "OANDA:CAD_JPY", "yahoo": "CADJPY=X", "td": "CAD/JPY"},
    "EUR/CAD": {"finnhub": "OANDA:EUR_CAD", "yahoo": "EURCAD=X", "td": "EUR/CAD"},
    "EUR/CHF": {"finnhub": "OANDA:EUR_CHF", "yahoo": "EURCHF=X", "td": "EUR/CHF"},
    "EUR/GBP": {"finnhub": "OANDA:EUR_GBP", "yahoo": "EURGBP=X", "td": "EUR/GBP"},
    "EUR/USD": {"finnhub": "OANDA:EUR_USD", "yahoo": "EURUSD=X", "td": "EUR/USD"},
    "GBP/CAD": {"finnhub": "OANDA:GBP_CAD", "yahoo": "GBPCAD=X", "td": "GBP/CAD"},
    "GBP/CHF": {"finnhub": "OANDA:GBP_CHF", "yahoo": "GBPCHF=X", "td": "GBP/CHF"},
    "USD/CAD": {"finnhub": "OANDA:USD_CAD", "yahoo": "USDCAD=X", "td": "USD/CAD"},
    "USD/JPY": {"finnhub": "OANDA:USD_JPY", "yahoo": "USDJPY=X", "td": "USD/JPY"},
    "CHF/JPY": {"finnhub": "OANDA:CHF_JPY", "yahoo": "CHFJPY=X", "td": "CHF/JPY"},
    "EUR/AUD": {"finnhub": "OANDA:EUR_AUD", "yahoo": "EURAUD=X", "td": "EUR/AUD"},
    "USD/CHF": {"finnhub": "OANDA:USD_CHF", "yahoo": "USDCHF=X", "td": "USD/CHF"},
    "EUR/JPY": {"finnhub": "OANDA:EUR_JPY", "yahoo": "EURJPY=X", "td": "EUR/JPY"},
    "GBP/USD": {"finnhub": "OANDA:GBP_USD", "yahoo": "GBPUSD=X", "td": "GBP/USD"},
}

# Dynamic Forex catalogue. Static pairs are only a safe bootstrap.
PAIR_CATALOG_LOCK = threading.RLock()
PAIR_CATALOG_CACHE = {"at": 0.0, "pairs": list(PAIRS.keys())}
PAIR_CATALOG_TTL = max(30, int(os.getenv("PAIR_CATALOG_TTL", "120")))

def _dynamic_pair_meta(pair):
    clean = str(pair or "").replace("/", "").replace("_", "").upper()
    if len(clean) != 6 or not clean.isalpha():
        return None
    return {"finnhub": f"OANDA:{clean[:3]}_{clean[3:]}",
            "yahoo": f"{clean}=X", "td": f"{clean[:3]}/{clean[3:]}"}

def refresh_forex_catalog(force=False):
    """Return the fixed public 20-pair universe.

    The previous build queried a dynamic catalogue and could silently add dozens
    of symbols. That made the website inconsistent with the supported/tested
    universe and could expose pairs whose provider mapping was not validated.
    V31 deliberately keeps the public surface to ten tested pairs.
    """
    with PAIR_CATALOG_LOCK:
        PAIR_CATALOG_CACHE["pairs"] = list(PUBLIC_PAIRS)
        PAIR_CATALOG_CACHE["at"] = time.time()
    return list(PUBLIC_PAIRS)

TF_MIN = {"1m": 1, "5m": 5, "1h": 60}
TF_SECONDS = {k: v * 60 for k, v in TF_MIN.items()}

# Shared market-data HTTP client.  Free/public providers can occasionally
# return transient 429/5xx responses, so the client retries those safely.
S = requests.Session()
S.headers.update({
    "User-Agent": "NexCandleAI/3.2 market-data-client",
    "Accept": "application/json",
})
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    # Provider failover/cooldowns handle retries explicitly. Avoid urllib3
    # retry storms that make a single signal wait through several timeouts.
    _retry = Retry(
        total=0,
        connect=0,
        read=0,
        status=0,
        allowed_methods=frozenset(["GET"]),
    )
    S.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=20, pool_maxsize=20))
    S.mount("http://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10))
except Exception:
    pass

DATA_TIMEOUT = max(3, int(os.getenv("MARKET_DATA_TIMEOUT_SECONDS", "5")))
LOCK = threading.RLock()
MARKET_CACHE: Dict[Tuple[str, str], Tuple[float, pd.DataFrame, str]] = {}
SIGNAL_CACHE: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}
MTF_CACHE: Dict[Tuple[str, str], Tuple[float, Tuple[str, int, Dict[str, Tuple[str, float]]]]] = {}
MARKET_FETCH_LOCKS: Dict[Tuple[str, str], threading.Lock] = {}
PROVIDER_COOLDOWN: Dict[str, float] = {}
PROVIDER_LAST_REQUEST: Dict[str, float] = {}
USER_STATE: Dict[int, Dict[str, Any]] = {}
# Last validated directional result per pair/timeframe. This is only used during
# transient provider outages; it never invents a price or a random direction.
LAST_DIRECTIONAL: Dict[Tuple[str, str], Tuple[float, str, Dict[str, Any]]] = {}
LAST_DIRECTIONAL_MAX_AGE = max(60, int(os.getenv("LAST_DIRECTIONAL_MAX_AGE", "900")))


# ============================================================
# V39 CRYPTO HYPER ENGINE — BINANCE REAL SPOT CANDLES
# No synthetic candles. Only Binance completed klines are eligible.
# ============================================================
CRYPTO_PAIRS = [
    "BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT",
    "DOGE/USDT","ADA/USDT","AVAX/USDT","LINK/USDT","TRX/USDT",
]
CRYPTO_SYMBOLS = {p:p.replace("/","").upper() for p in CRYPTO_PAIRS}
# Binance market-data routing.
# IMPORTANT: Render was receiving HTTP 451 from the normal api.binance.com
# cluster. Binance documents the dedicated public market-data host
# data-api.binance.vision for market data. Put it FIRST, then use official
# Binance redundancy hosts. No candle is generated locally.
_BINANCE_DEFAULT_HOSTS = (
    "https://data-api.binance.vision,"
    "https://api-gcp.binance.com,"
    "https://api.binance.com,"
    "https://api1.binance.com,"
    "https://api2.binance.com,"
    "https://api3.binance.com"
)
BINANCE_HOSTS = [h.strip().rstrip("/") for h in
                 (os.getenv("BINANCE_API_HOSTS") or _BINANCE_DEFAULT_HOSTS).split(",")
                 if h.strip()]
BINANCE_TIMEOUT = max(3, int(os.getenv("BINANCE_TIMEOUT_SECONDS","7")))
CRYPTO_FETCH_LIMIT = min(1000, max(120, int(os.getenv("CRYPTO_FETCH_LIMIT","250"))))
CRYPTO_CACHE_TTL = {"1m":8, "5m":20, "1h":120}
CRYPTO_SIGNAL_CACHE = {}
CRYPTO_FETCH_LOCKS = {}
CRYPTO_LAST_VALID = {}

def _crypto_key(pair, tf):
    return (f"CRYPTO:{pair}", tf)

def _binance_klines(pair, tf, limit=250):
    if pair not in CRYPTO_SYMBOLS or tf not in TF_MIN:
        raise ValueError("Unsupported crypto pair/timeframe")
    symbol = CRYPTO_SYMBOLS[pair]
    errors=[]
    for host in BINANCE_HOSTS:
        try:
            r = S.get(f"{host}/api/v3/klines",
                      params={"symbol":symbol,"interval":tf,"limit":min(int(limit),1000)},
                      timeout=BINANCE_TIMEOUT,
                      headers={"Accept":"application/json"})
            if r.status_code != 200:
                body = ""
                try:
                    body = r.text[:160].replace("\\n", " ")
                except Exception:
                    pass
                raise RuntimeError(f"Binance HTTP {r.status_code}" + (f" {body}" if body else ""))
            data=r.json()
            if not isinstance(data,list) or len(data)<20:
                raise RuntimeError("Binance returned insufficient klines")
            rows=[]
            now_ms=int(time.time()*1000)
            for k in data:
                if not isinstance(k,list) or len(k)<11:
                    continue
                open_ms=int(k[0]); close_ms=int(k[6])
                # Binance kline closeTime marks the end of the bar. Do not
                # include the still-forming candle.
                if close_ms >= now_ms:
                    continue
                o,h,l,c=map(float,k[1:5])
                vol=float(k[5]); qvol=float(k[7])
                trades=int(k[8])
                taker_base=float(k[9]); taker_quote=float(k[10])
                if not all(math.isfinite(v) for v in (o,h,l,c,vol,qvol,taker_base,taker_quote)):
                    continue
                if h <= l or min(o,c)<l or max(o,c)>h or o<=0 or c<=0:
                    continue
                rows.append({
                    "open_time":open_ms,"close_time":close_ms,
                    "open":o,"high":h,"low":l,"close":c,
                    "volume":vol,"quote_volume":qvol,"trades":trades,
                    "taker_buy_base":taker_base,"taker_buy_quote":taker_quote,
                })
            if len(rows)<20:
                raise RuntimeError(f"Only {len(rows)} completed real candles")
            x=pd.DataFrame(rows)
            x["datetime"]=pd.to_datetime(x["open_time"],unit="ms",utc=True)
            x=x.set_index("datetime").sort_index()
            x=x[~x.index.duplicated(keep="last")]
            # Exact requested cadence. This catches accidental wrong interval
            # responses before they reach the model.
            expected=TF_SECONDS[tf]
            diffs=x.index.to_series().diff().dt.total_seconds().dropna()
            if not diffs.empty:
                near=((diffs-expected).abs() <= max(1,expected*0.02)).mean()
                if near < 0.92:
                    raise RuntimeError(f"Binance candle cadence mismatch for {tf}")
            last_close_ms=int(x["close_time"].iloc[-1])
            age=max(0,int(time.time()*1000)-last_close_ms)/1000.0
            # A closed candle can naturally be almost one full interval old.
            # Anything beyond 2.25 intervals is considered stale.
            if age > TF_SECONDS[tf]*2.25 + 5:
                raise RuntimeError(f"Binance market data stale ({fmt_duration(age)} old)")
            return x.tail(min(int(limit),1000)).copy(), host, age
        except Exception as e:
            errors.append(f"{host}: {str(e)[:120]}")
    raise RuntimeError("Binance data unavailable • " + " | ".join(errors[-3:]))

def crypto_candles(pair, tf, limit=None):
    key=_crypto_key(pair,tf)
    limit=min(CRYPTO_FETCH_LIMIT,max(20,int(limit or CRYPTO_FETCH_LIMIT)))
    now=time.time()
    with LOCK:
        cached=CRYPTO_SIGNAL_CACHE.get(("data",key))
        if cached and now-cached[0] <= CRYPTO_CACHE_TTL.get(tf,20) and len(cached[1])>=min(limit,20):
            return cached[1].tail(limit).copy(), cached[2], cached[3]
    with LOCK:
        lock=CRYPTO_FETCH_LOCKS.setdefault(key,threading.Lock())
    with lock:
        with LOCK:
            cached=CRYPTO_SIGNAL_CACHE.get(("data",key))
            if cached and now-cached[0] <= CRYPTO_CACHE_TTL.get(tf,20) and len(cached[1])>=min(limit,20):
                return cached[1].tail(limit).copy(), cached[2], cached[3]
        x,host,age=_binance_klines(pair,tf,limit)
        with LOCK:
            CRYPTO_SIGNAL_CACHE[("data",key)]=(time.time(),x.copy(),host,age)
        return x.tail(limit).copy(),host,age

def _c_ema(s,n):
    return s.ewm(span=n,adjust=False,min_periods=n).mean()

def _c_rsi(s,n=14):
    d=s.diff()
    g=d.clip(lower=0).ewm(alpha=1/n,adjust=False,min_periods=n).mean()
    l=(-d.clip(upper=0)).ewm(alpha=1/n,adjust=False,min_periods=n).mean()
    rs=g/l.replace(0,np.nan)
    out=100-(100/(1+rs))
    return out.fillna(50).clip(0,100)

def _c_atr(x,n=14):
    prev=x["close"].shift(1)
    tr=pd.concat([(x["high"]-x["low"]),(x["high"]-prev).abs(),(x["low"]-prev).abs()],axis=1).max(axis=1)
    return tr.ewm(alpha=1/n,adjust=False,min_periods=n).mean()

def _c_adx(x,n=14):
    up=x["high"].diff(); down=-x["low"].diff()
    plus=up.where((up>down)&(up>0),0.0)
    minus=down.where((down>up)&(down>0),0.0)
    atr=_c_atr(x,n).replace(0,np.nan)
    pdi=100*plus.ewm(alpha=1/n,adjust=False,min_periods=n).mean()/atr
    mdi=100*minus.ewm(alpha=1/n,adjust=False,min_periods=n).mean()/atr
    dx=100*(pdi-mdi).abs()/(pdi+mdi).replace(0,np.nan)
    return dx.ewm(alpha=1/n,adjust=False,min_periods=n).mean().fillna(0).clip(0,100)

def _crypto_features(x):
    z=x.copy()
    c=z["close"]; h=z["high"]; l=z["low"]; o=z["open"]; v=z["volume"]
    z["ema9"]=_c_ema(c,9); z["ema21"]=_c_ema(c,21); z["ema50"]=_c_ema(c,50)
    z["rsi"]=_c_rsi(c,14)
    z["macd"]=z["ema9"] if False else _c_ema(c,12)-_c_ema(c,26)
    z["macd_signal"]=z["macd"].ewm(span=9,adjust=False,min_periods=9).mean()
    z["macd_hist"]=z["macd"]-z["macd_signal"]
    z["atr"]=_c_atr(z,14)
    z["adx"]=_c_adx(z,14)
    z["bb_mid"]=c.rolling(20,min_periods=20).mean()
    z["bb_std"]=c.rolling(20,min_periods=20).std(ddof=0)
    z["bb_up"]=z["bb_mid"]+2*z["bb_std"]; z["bb_dn"]=z["bb_mid"]-2*z["bb_std"]
    z["bb_pos"]=((c-z["bb_dn"])/(z["bb_up"]-z["bb_dn"]).replace(0,np.nan)).clip(0,1).fillna(.5)
    ll=l.rolling(14,min_periods=14).min(); hh=h.rolling(14,min_periods=14).max()
    z["stoch"]=100*(c-ll)/(hh-ll).replace(0,np.nan)
    z["roc5"]=c.pct_change(5)*100; z["roc10"]=c.pct_change(10)*100
    z["vol_mean"]=v.rolling(20,min_periods=20).mean()
    z["vol_std"]=v.rolling(20,min_periods=20).std(ddof=0)
    z["vol_z"]=((v-z["vol_mean"])/z["vol_std"].replace(0,np.nan)).fillna(0)
    z["buy_ratio"]=(z["taker_buy_base"]/v.replace(0,np.nan)).clip(0,1).fillna(.5)
    rng=(h-l).replace(0,np.nan)
    z["body"]=(c-o)/rng
    z["close_pos"]=((c-l)/rng).clip(0,1).fillna(.5)
    z["upper_wick"]=(h-np.maximum(o,c))/rng
    z["lower_wick"]=(np.minimum(o,c)-l)/rng
    z["range_pct"]=rng/c.replace(0,np.nan)*100
    z["atr_pct"]=z["atr"]/c.replace(0,np.nan)*100
    z["ema_spread"]=(z["ema9"]-z["ema21"])/c*100
    z["trend_spread"]=(z["ema21"]-z["ema50"])/c*100
    z["macd_pct"]=z["macd_hist"]/c*100
    z["bb_width"]=(z["bb_up"]-z["bb_dn"])/z["bb_mid"]*100
    return z

def _sgn(v, dead=0.0):
    return 1.0 if v>dead else -1.0 if v<-dead else 0.0

def _crypto_model_at(z, i):
    r=z.iloc[i]
    vals=[]
    # Trend structure
    trend=_sgn(float(r.ema9-r.ema21), abs(float(r.close))*0.00015)
    if r.ema21>r.ema50: trend += .35
    elif r.ema21<r.ema50: trend -= .35
    vals.append(("trend",float(np.clip(trend,-1,1)),1.45))
    # Momentum
    mom=_sgn(float(r.macd_hist), max(abs(float(r.close))*0.00003,1e-12))
    if r.rsi>55: mom+=.35
    elif r.rsi<45: mom-=.35
    vals.append(("momentum",float(np.clip(mom,-1,1)),1.30))
    # Mean reversion / Bollinger
    bb=.0
    if r.bb_pos<.18 and r.rsi<42: bb=.75
    elif r.bb_pos>.82 and r.rsi>58: bb=-.75
    elif r.bb_pos>.60: bb=.25
    elif r.bb_pos<.40: bb=-.25
    vals.append(("bollinger",bb,0.80))
    # Stochastic
    st=.0
    if r.stoch<20 and r.rsi<48: st=.55
    elif r.stoch>80 and r.rsi>52: st=-.55
    else: st=float(np.clip((r.stoch-50)/80,-.4,.4))
    vals.append(("stochastic",st,.65))
    # Candle pressure
    candle=float(np.clip(r.body*.70+(r.close_pos-.5)*.45+(r.lower_wick-r.upper_wick)*.25,-1,1))
    vals.append(("candle",candle,.95))
    # Volume/order-flow proxy from Binance taker-buy volume
    flow=float(np.clip((r.buy_ratio-.5)*3.2,-1,1))
    if r.vol_z>1.0: flow*=1.20
    vals.append(("volume_flow",float(np.clip(flow,-1,1)),1.05))
    # Recent breakout / momentum acceleration
    breakout=0.0
    look=z.iloc[max(0,i-20):i]
    if len(look)>=8:
        hi=float(look.high.max()); lo=float(look.low.min())
        if r.close>hi: breakout=.85
        elif r.close<lo: breakout=-.85
        else: breakout=float(np.clip((r.close-(hi+lo)/2)/(max(hi-lo,1e-12)*.75),-.45,.45))
    accel=float(np.clip((r.roc5-r.roc10)*0.12,-.8,.8))
    vals.append(("breakout",breakout,.85))
    vals.append(("acceleration",accel,.55))
    raw=sum(v*w for _,v,w in vals)/sum(w for _,_,w in vals)
    # Regime: ADX tells us whether trend signals deserve more weight.
    adx=float(r.adx)
    regime="TREND" if adx>=25 else "RANGE" if adx<18 else "TRANSITION"
    if regime=="RANGE":
        raw = raw*.82 + (bb*.18)
    elif regime=="TREND":
        raw = raw*1.06
    # Volatility guard: extreme one-bar expansion lowers confidence.
    vol_penalty=0.0
    if r.atr_pct>max(3.5, float(z["atr_pct"].rolling(30,min_periods=10).median().iloc[i] or 0)*2.2):
        vol_penalty=.10
    direction=1 if raw>0 else -1 if raw<0 else 0
    return direction,float(np.clip(abs(raw)*100,0,100)),vals,regime,vol_penalty

def _crypto_walk_forward(z, start=None, max_eval=60):
    n=len(z)
    start=max(60,n-max_eval-1) if start is None else max(60,start)
    hits=0; total=0
    for i in range(start,n-1):
        try:
            d,_,_,_,_= _crypto_model_at(z,i)
            nxt=float(z.iloc[i+1].close); op=float(z.iloc[i+1].open)
            actual=1 if nxt>op else -1 if nxt<op else 0
            if d and actual:
                total+=1; hits+=int(d==actual)
        except Exception:
            continue
    return (hits/total if total else .50),total

def make_crypto_signal(pair,tf):
    if pair not in CRYPTO_SYMBOLS or tf not in TF_MIN:
        raise RuntimeError("Unsupported crypto market")
    x,host,age=crypto_candles(pair,tf,CRYPTO_FETCH_LIMIT)
    z=_crypto_features(x)
    # Drop warm-up rows only for model calculations; candles remain untouched.
    z=z.replace([np.inf,-np.inf],np.nan)
    if len(z)<80:
        raise RuntimeError(f"Insufficient real Binance candles: {len(z)}")
    valid=z.dropna(subset=["ema50","rsi","macd_signal","atr","bb_mid","adx","stoch"])
    if len(valid)<60:
        raise RuntimeError(f"Insufficient indicator history: {len(valid)}")
    i=len(z)-1
    d,raw,vals,regime,vol_penalty=_crypto_model_at(z,i)
    wf,samples=_crypto_walk_forward(z,max_eval=60)
    # Historical calibration is a modifier, not a fake probability.
    calibration=(wf-.50)*32 if samples>=15 else 0.0
    confidence=float(np.clip(50+raw*.42+calibration-vol_penalty*25,50,94))
    # Require enough directional evidence; weak evidence is explicitly WAIT.
    if raw < 18:
        display="WAIT"
    else:
        display="UP" if d>0 else "DOWN"
    # Strong disagreement between trend and momentum lowers confidence.
    trend_vote=next(v for n,v,w in vals if n=="trend")
    mom_vote=next(v for n,v,w in vals if n=="momentum")
    if trend_vote*mom_vote<-.35:
        confidence=min(confidence,68)
    score=int(round(confidence)) if display!="WAIT" else 0
    last_close=float(x.close.iloc[-1])
    return {
        "market":"crypto","pair":pair,"tf":tf,
        "display_direction":display,"direction":"CALL" if display=="UP" else "PUT" if display=="DOWN" else "WAIT",
        "display_score":score,"confidence":score,
        "confirmation":"BINANCE COMPLETED CANDLE" if display!="WAIT" else "NO SIGNAL",
        "target_candle":"NEXT_CANDLE","candle_only":False,
        "data_age":round(age,1),"fresh":True,"provider":"BINANCE",
        "real_candles":int(len(x)),"last_close":last_close,
        "walk_forward_accuracy":round(wf*100,1),"walk_forward_samples":samples,
        "regime":regime,"model":"V39 Binance Hyper Confluence",
        "features":{n:round(float(v),4) for n,v,w in vals},
        "indicators":{
            "RSI":round(float(z.rsi.iloc[-1]),2),
            "ADX":round(float(z.adx.iloc[-1]),2),
            "MACD_HIST":float(z.macd_hist.iloc[-1]),
            "ATR_PCT":round(float(z.atr_pct.iloc[-1]),4),
            "BB_POS":round(float(z.bb_pos.iloc[-1]),4),
            "STOCH":round(float(z.stoch.iloc[-1]),2),
            "VOLUME_Z":round(float(z.vol_z.iloc[-1]),2),
            "TAKER_BUY_RATIO":round(float(z.buy_ratio.iloc[-1])*100,2),
            "EMA9":float(z.ema9.iloc[-1]),"EMA21":float(z.ema21.iloc[-1]),"EMA50":float(z.ema50.iloc[-1]),
        }
    }

def get_crypto_signal(pair,tf):
    key=_crypto_key(pair,tf); now=time.time()
    with LOCK:
        c=CRYPTO_SIGNAL_CACHE.get(("signal",key))
        if c and now-c[0] <= CRYPTO_CACHE_TTL.get(tf,20):
            return dict(c[1])
    s=make_crypto_signal(pair,tf)
    with LOCK:
        CRYPTO_SIGNAL_CACHE[("signal",key)]=(time.time(),dict(s))
        if s.get("display_direction") in {"UP","DOWN"}:
            CRYPTO_LAST_VALID[key]=(time.time(),dict(s))
    return s



# Persistent/diagnostic market-cache state. The cache is recovery-only: it never
# fabricates prices. A process restart can recover the last validated candles
# from SQLite when the deployment storage is persistent.
PROVIDER_LAST_SUCCESS: Dict[Tuple[str, str], str] = {}
PERSISTED_CACHE_MAX_AGE = max(300, int(os.getenv("PERSISTED_CACHE_MAX_AGE", "21600")))
RECOVERY_MAX_AGE = max(60, int(os.getenv("RECOVERY_MAX_AGE", "900")))


WELCOME = (
    "👋 <b>Welcome to NexCandle AI</b>\n\n"
    "📊 AI-powered market analysis and candle signals.\n"
    "Choose an option below to continue."
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nexcandle")

web = Flask(__name__)

# ============================================================
# V42 ALL ACCESS BLOCK — block the website UI for every browser/device.
# Telegram webhook and health remain available so the bot/deployment health is not broken.
@web.before_request
def _v42_all_access_lock():
    if request.path in ("/telegram/webhook", "/health"):
        return None

    return Response(
        """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>YOUR ACCOUNT IS TERMINATED</title>
<style>
html,body{margin:0;min-height:100%;background:#000;color:#fff;font-family:Arial,Helvetica,sans-serif}
body{display:flex;align-items:center;justify-content:center;padding:24px;box-sizing:border-box}
.box{max-width:720px;width:100%;text-align:center;border:1px solid #333;border-radius:18px;padding:42px 24px;background:linear-gradient(180deg,#0b0b0b,#000);box-shadow:0 0 45px rgba(255,0,0,.18)}
h1{margin:0 0 14px;font-size:clamp(30px,7vw,58px);letter-spacing:2px;color:#ff2020}
h2{margin:0 0 20px;font-size:clamp(20px,4vw,32px);letter-spacing:1px}
p{margin:10px 0;color:#aaa;font-size:16px;line-height:1.6}
.badge{display:inline-block;margin-top:18px;padding:9px 16px;border:1px solid #ff2020;border-radius:999px;color:#ff4040;font-weight:700;letter-spacing:1px}
</style>
</head>
<body>
<div class="box">
  <h1>YOUR ACCOUNT IS TERMINATED</h1>
  <h2>ACCESS BLOCKED</h2>
  <p>This service is currently unavailable.</p>
  <p>All website access has been blocked for this account.</p>
  <div class="badge">ACCESS DENIED</div>
</div>
</body>
</html>""",
        status=403,
        mimetype="text/html",
    )
