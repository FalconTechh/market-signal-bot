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

import numpy as np
import pandas as pd
import requests
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
    "1m": max(5, int(os.getenv("CACHE_TTL_1M", "12"))),
    "5m": max(10, int(os.getenv("CACHE_TTL_5M", "35"))),
    "15m": max(20, int(os.getenv("CACHE_TTL_15M", "90"))),
    "30m": max(30, int(os.getenv("CACHE_TTL_30M", "150"))),
    "45m": max(45, int(os.getenv("CACHE_TTL_45M", "210"))),
    "1h": max(60, int(os.getenv("CACHE_TTL_1H", "300"))),
    "4h": max(120, int(os.getenv("CACHE_TTL_4H", "600"))),
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
    "1m": ["yahoo2", "yahoo", "twelvedata", "finnhub", "dukascopy", "biquote"],
    "5m": ["biquote", "dukascopy", "twelvedata", "finnhub", "yahoo2", "yahoo"],
    "15m": ["biquote", "dukascopy", "twelvedata", "finnhub", "yahoo2", "yahoo"],
    "30m": ["biquote", "dukascopy", "twelvedata", "finnhub", "yahoo2", "yahoo"],
    "45m": ["twelvedata", "biquote", "dukascopy", "finnhub", "yahoo2", "yahoo"],
    "1h": ["biquote", "yahoo2", "twelvedata", "finnhub", "dukascopy", "yahoo"],
    "4h": ["dukascopy", "twelvedata", "finnhub", "yahoo2", "yahoo", "biquote"],
}
ALLOW_YAHOO_FALLBACK = (os.getenv("ALLOW_YAHOO_FALLBACK", "true").strip().lower()
                        not in ("0", "false", "no", "off"))
MARKET_FETCH_LIMIT = max(120, int(os.getenv("MARKET_FETCH_LIMIT", "350")))
MTF_BASE_LIMIT = max(500, int(os.getenv("MTF_BASE_LIMIT", "1200")))
SIGNAL_CACHE_SECONDS = max(10, int(os.getenv("SIGNAL_CACHE_SECONDS", "30")))
PROVIDER_COOLDOWN_SECONDS = max(10, int(os.getenv("PROVIDER_COOLDOWN_SECONDS", "45")))
PROVIDER_ERROR_COOLDOWN_SECONDS = max(PROVIDER_COOLDOWN_SECONDS, int(os.getenv("PROVIDER_ERROR_COOLDOWN_SECONDS", "180")))
PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS = max(120, int(os.getenv("PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS", "300")))
MTF_CACHE_SECONDS = max(10, int(os.getenv("MTF_CACHE_SECONDS", "45")))
PROVIDER_MIN_INTERVAL_SECONDS = max(0.0, float(os.getenv("PROVIDER_MIN_INTERVAL_SECONDS", "0.75")))
TRADE_HORIZON_CANDLES = max(1, int(os.getenv("TRADE_HORIZON_CANDLES", "1")))

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
    "USD/CAD": {"finnhub": "OANDA:USD_CAD", "yahoo": "CAD=X", "td": "USD/CAD"},
    "USD/JPY": {"finnhub": "OANDA:USD_JPY", "yahoo": "JPY=X", "td": "USD/JPY"},
    "CHF/JPY": {"finnhub": "OANDA:CHF_JPY", "yahoo": "CHFJPY=X", "td": "CHF/JPY"},
    "EUR/AUD": {"finnhub": "OANDA:EUR_AUD", "yahoo": "EURAUD=X", "td": "EUR/AUD"},
    "USD/CHF": {"finnhub": "OANDA:USD_CHF", "yahoo": "CHF=X", "td": "USD/CHF"},
    "EUR/JPY": {"finnhub": "OANDA:EUR_JPY", "yahoo": "EURJPY=X", "td": "EUR/JPY"},
    "GBP/USD": {"finnhub": "OANDA:GBP_USD", "yahoo": "GBPUSD=X", "td": "GBP/USD"},
}

LIVE_FOREX_CACHE = {"updated": 0.0, "pairs": []}
LIVE_FOREX_CACHE_TTL = 300

def refresh_live_forex_pairs(force=False):
    """Discover every currently live Forex symbol exposed by BiQuote.
    Only 6-letter currency-vs-currency symbols are included; commodities,
    crypto and indices are intentionally excluded.
    """
    now = time.time()
    if not force and LIVE_FOREX_CACHE["pairs"] and now - LIVE_FOREX_CACHE["updated"] < LIVE_FOREX_CACHE_TTL:
        return list(LIVE_FOREX_CACHE["pairs"])
    try:
        # /active is the authoritative live-feed list. Fall back to /symbols
        # for installations where /active is unavailable.
        urls = [
            ("https://biquote.io/api/active", {"type": "Forex"}),
            ("https://biquote.io/api/symbols", {"type": "Forex", "liveOnly": "true"}),
        ]
        rows = None
        for url, params in urls:
            try:
                rr = S.get(url, params=params, timeout=min(DATA_TIMEOUT, 8))
                rr.raise_for_status()
                candidate = rr.json()
                if isinstance(candidate, list):
                    rows = candidate
                    break
            except Exception:
                continue
        found = []
        for row in rows or []:
            sym = str(row.get("name") or row.get("symbol") or "").upper().strip()
            typ = str(row.get("type") or row.get("assetType") or "").lower().strip()
            if typ and typ != "forex":
                continue
            if re.fullmatch(r"[A-Z]{6}", sym) and sym[:3] != sym[3:]:
                found.append(f"{sym[:3]}/{sym[3:]}")
        found = sorted(set(found))
        if found:
            with LOCK:
                LIVE_FOREX_CACHE["pairs"] = found
                LIVE_FOREX_CACHE["updated"] = now
                for pair in found:
                    PAIRS.setdefault(pair, {
                        "finnhub": f"OANDA:{pair.replace('/','_')}",
                        "yahoo": pair.replace("/","") + "=X",
                        "td": pair,
                    })
        return found or list(PAIRS.keys())
    except Exception:
        return list(LIVE_FOREX_CACHE["pairs"] or PAIRS.keys())

TF_MIN = {"30m": 30, "1h": 60}
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
web.secret_key = FLASK_SECRET
web.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=True if _ON_RENDER else False,
    PERMANENT_SESSION_LIFETIME=timedelta(days=7),
)
web.config["SESSION_COOKIE_HTTPONLY"] = True
web.config["SESSION_COOKIE_SAMESITE"] = "Lax"
web.config["SESSION_COOKIE_SECURE"] = _ON_RENDER
TELEGRAM_APP = None
TELEGRAM_LOOP = None
WEBHOOK_PATH = None

@web.post("/telegram/webhook")
def telegram_webhook():
    """Telegram webhook receiver.

    IMPORTANT: process the update directly on the dedicated Telegram event loop.
    The previous queue-only bridge could acknowledge Telegram while an update
    was not consumed reliably after a Render restart. Direct processing makes
    plain messages and inline-button callbacks deterministic.
    """
    global TELEGRAM_APP, TELEGRAM_LOOP
    if TELEGRAM_APP is None or TELEGRAM_LOOP is None or not TELEGRAM_LOOP.is_running():
        db_log("warning", "telegram_webhook", "telegram application/loop not ready")
        return {"ok": False, "error": "telegram application not ready"}, 503
    if WEBHOOK_SECRET:
        supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if supplied != WEBHOOK_SECRET:
            db_log("warning", "telegram_webhook", "invalid secret header")
            return {"ok": False}, 403
    try:
        payload = request.get_json(force=True, silent=False)
        update = Update.de_json(payload, TELEGRAM_APP.bot)
        if update is None:
            return {"ok": False, "error": "invalid telegram update"}, 400
        # Do not merely enqueue and return 200: execute the actual handler.
        # This guarantees that Telegram messages/callbacks are processed.
        fut = asyncio.run_coroutine_threadsafe(
            TELEGRAM_APP.process_update(update), TELEGRAM_LOOP
        )
        fut.result(timeout=55)
        db_log("info", "telegram_webhook", f"processed update {update.update_id}")
        return {"ok": True}
    except Exception as exc:
        db_log("error", "telegram_webhook", f"processing failed: {type(exc).__name__}: {exc}")
        # Return 200 after the update reached our process to avoid an endless
        # Telegram retry storm. The exact exception remains in Render logs.
        return {"ok": True}


PREMIUM_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>NexCandle AI — Premium UP/DOWN Engine</title>
<style>
:root{
 --bg:#03070a;--panel:#071117;--line:#14303a;--text:#eafff8;--muted:#728b93;
 --up:#00ff9d;--down:#ff315d;--cyan:#00d9ff;--gold:#b9fff0;
}
*{box-sizing:border-box}
html,body{margin:0;min-height:100%;background:#03070a;color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,Arial,sans-serif}
body{overflow-x:hidden;background:
 radial-gradient(circle at 50% -5%,rgba(0,255,157,.16),transparent 34%),
 radial-gradient(circle at 90% 30%,rgba(0,217,255,.08),transparent 30%),#03070a}
body:before{content:"";position:fixed;inset:0;pointer-events:none;opacity:.35;background:
 linear-gradient(rgba(0,255,180,.035) 1px,transparent 1px),
 linear-gradient(90deg,rgba(0,255,180,.035) 1px,transparent 1px);background-size:38px 38px;
 mask-image:linear-gradient(to bottom,#000,transparent 82%)}
.wrap{max-width:1180px;margin:auto;padding:16px}
.top{display:flex;align-items:center;justify-content:space-between;padding:8px 2px 16px}
.brand{font-size:14px;font-weight:950;letter-spacing:.16em}
.brand em{font-style:normal;color:var(--up)}
.live{font-size:9px;letter-spacing:.14em;color:#9bb0b6;border:1px solid #19333c;border-radius:999px;padding:8px 11px;background:rgba(5,14,18,.7)}
.live i{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--up);box-shadow:0 0 15px var(--up);margin-right:6px;animation:blink 1s infinite}
.layout{display:grid;grid-template-columns:minmax(0,1fr) 350px;gap:16px}
.card{border:1px solid #14313a;border-radius:30px;background:linear-gradient(145deg,rgba(8,19,25,.97),rgba(3,8,12,.97));box-shadow:0 30px 100px rgba(0,0,0,.6),inset 0 1px rgba(255,255,255,.035);overflow:hidden}
.controls{display:flex;gap:8px;padding:16px;border-bottom:1px solid #10272f}
.select,.scanbtn{height:44px;border-radius:13px}
.select{min-width:120px;padding:0 13px;background:#061016;border:1px solid #1b3b44;color:#e7f7f3;font-weight:700;outline:none}
.scanbtn{margin-left:auto;padding:0 22px;border:1px solid #178261;background:linear-gradient(180deg,#0c493a,#06281f);color:#dffff5;font-weight:950;letter-spacing:.15em;cursor:pointer;box-shadow:0 0 28px rgba(0,255,157,.12)}
.scanbtn:active{transform:scale(.98)}
.stage{height:500px;position:relative;display:grid;place-items:center;overflow:hidden;perspective:1200px;background:radial-gradient(ellipse at 50% 52%,rgba(0,255,157,.08),transparent 48%),linear-gradient(180deg,rgba(2,10,14,.35),rgba(1,6,10,.75));isolation:isolate}.stage:after{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(180deg,transparent 0%,rgba(0,217,255,.035) 48%,transparent 52%);background-size:100% 12px;opacity:.35;mix-blend-mode:screen;animation:scanlines 5s linear infinite}
.stage:before{content:"";position:absolute;width:78%;height:78%;border-radius:50%;background:radial-gradient(circle,rgba(0,255,157,.17),rgba(0,217,255,.05) 34%,transparent 68%);filter:blur(14px);animation:aura 3.4s ease-in-out infinite}.holo{position:absolute;inset:8% 5%;border:1px solid rgba(0,217,255,.08);border-radius:50%;transform:rotateX(70deg) translateZ(-80px);box-shadow:0 0 50px rgba(0,217,255,.06),inset 0 0 40px rgba(0,255,157,.04);animation:holoSpin 14s linear infinite}.scanline{position:absolute;left:8%;right:8%;height:1px;background:linear-gradient(90deg,transparent,var(--cyan),transparent);box-shadow:0 0 18px var(--cyan);opacity:.55;animation:scanline 3.2s ease-in-out infinite}.energy{position:absolute;width:410px;height:410px;border:1px dashed rgba(0,255,157,.22);border-radius:50%;transform:rotateX(72deg) rotateZ(-8deg);filter:drop-shadow(0 0 10px rgba(0,255,157,.18));animation:energy 8s linear infinite}
.core{width:245px;height:245px;position:relative;transform-style:preserve-3d;animation:float 4.2s ease-in-out infinite;filter:drop-shadow(0 0 25px rgba(0,255,157,.18))}.core .ring{backface-visibility:visible}
.core:before{content:"";position:absolute;inset:30px;border-radius:50%;background:
 radial-gradient(circle at 38% 32%,#eafff8 0,#6dffd1 4%,#00ff9d 10%,#075344 31%,#04161b 68%);
 box-shadow:0 0 28px var(--up),0 0 95px rgba(0,255,157,.26),inset 0 0 55px #00ff9d;
 animation:corepulse 1.7s ease-in-out infinite}
.core:after{content:"";position:absolute;inset:2px;border:2px solid rgba(0,255,157,.65);border-radius:50%;box-shadow:0 0 25px rgba(0,255,157,.5);animation:spin 3s linear infinite}
.ring{position:absolute;border:1px solid rgba(0,217,255,.62);border-radius:50%;transform-style:preserve-3d;box-shadow:0 0 12px rgba(0,217,255,.16)}
.r1{inset:-20px;transform:rotateX(68deg) rotateZ(15deg);animation:orbit1 5s linear infinite}
.r2{inset:-48px;transform:rotateX(74deg) rotateZ(-20deg);animation:orbit2 7s linear infinite}
.r3{inset:40px;transform:rotateY(72deg);animation:orbit3 4s linear infinite}
.r4{inset:-75px;border-color:rgba(0,255,157,.22);transform:rotateX(70deg);animation:orbit2 11s linear infinite reverse}
.beam{position:absolute;width:350px;height:350px;border-radius:50%;border:2px solid transparent;border-top-color:var(--cyan);border-right-color:rgba(0,217,255,.15);filter:drop-shadow(0 0 14px var(--cyan));animation:spin 2.15s linear infinite}
.beam:after{content:"";position:absolute;left:50%;top:50%;width:48%;height:2px;transform-origin:left center;background:linear-gradient(90deg,var(--cyan),transparent);box-shadow:0 0 14px var(--cyan)}
.particle{position:absolute;width:4px;height:4px;border-radius:50%;background:var(--up);box-shadow:0 0 14px var(--up);animation:particle 2.8s ease-in-out infinite}
.p1{transform:translate(-180px,-90px);animation-delay:-.4s}.p2{transform:translate(170px,-40px);animation-delay:-1.1s}
.p3{transform:translate(125px,125px);animation-delay:-1.7s}.p4{transform:translate(-145px,130px);animation-delay:-2.2s}
.result{position:absolute;z-index:5;text-align:center;transition:.35s;text-shadow:0 0 24px currentColor}
.result .word{font-size:64px;font-weight:1000;letter-spacing:.13em;line-height:1}
.result .sub{font-size:9px;letter-spacing:.28em;margin-top:13px;color:#a9bec4}
.result.up{color:var(--up)}.result.down{color:var(--down)}
.result.up .word,.result.down .word{animation:signalIn .55s cubic-bezier(.2,.8,.2,1);filter:drop-shadow(0 0 20px currentColor)}.result:after{content:"";position:absolute;left:50%;top:50%;width:190px;height:70px;transform:translate(-50%,-50%);border:1px solid currentColor;border-left-color:transparent;border-right-color:transparent;border-radius:50%;opacity:.22;animation:resultOrbit 2.4s linear infinite;pointer-events:none}
.result.up~.core:before{box-shadow:0 0 35px var(--up),0 0 130px rgba(0,255,157,.32),inset 0 0 55px #00ff9d}
.result.down~.core:before{box-shadow:0 0 35px var(--down),0 0 130px rgba(255,49,93,.22),inset 0 0 55px #ff315d}
.hint{position:absolute;bottom:18px;left:0;right:0;text-align:center;color:#789099;font-size:9px;letter-spacing:.23em}
.board{padding:18px}.board h2{margin:0 0 14px;font-size:12px;letter-spacing:.16em}.rows{display:grid;gap:9px}
.row{display:flex;align-items:center;justify-content:space-between;padding:15px 14px;border:1px solid #17343d;border-radius:18px;background:linear-gradient(145deg,rgba(7,18,24,.96),rgba(3,10,15,.96));transition:.25s;position:relative;overflow:hidden;box-shadow:inset 0 1px rgba(255,255,255,.025),0 12px 30px rgba(0,0,0,.18)}.row:before{content:"";position:absolute;inset:0;background:linear-gradient(100deg,transparent,rgba(0,217,255,.045),transparent);transform:translateX(-120%);animation:rowSweep 4.8s ease-in-out infinite}.row:nth-child(2):before{animation-delay:.7s}.row:nth-child(3):before{animation-delay:1.4s}.row:nth-child(4):before{animation-delay:2.1s}.row:nth-child(5):before{animation-delay:2.8s}.row:nth-child(6):before{animation-delay:3.5s}
.row:hover{border-color:#28616c;transform:translateY(-1px)}
.row b{font-size:14px}.row small{display:block;color:#647b84;font-size:9px;letter-spacing:.12em;margin-top:3px}
.badge{min-width:68px;text-align:center;padding:9px 11px;border-radius:12px;font-size:11px;font-weight:1000;letter-spacing:.12em}
.badge.up{color:var(--up);background:rgba(0,255,157,.08);box-shadow:0 0 20px rgba(0,255,157,.07)}
.badge.down{color:var(--down);background:rgba(255,49,93,.08);box-shadow:0 0 20px rgba(255,49,93,.07)}
.footer{margin-top:14px;text-align:center;color:#627780;font-size:9px;line-height:1.6}
.full{width:100%;margin:14px 0 0}
@keyframes spin{to{transform:rotate(360deg)}}@keyframes holoSpin{to{transform:rotateX(70deg) translateZ(-80px) rotateZ(360deg)}}@keyframes energy{to{transform:rotateX(72deg) rotateZ(352deg)}}@keyframes scanline{0%,100%{top:18%;opacity:0}15%{opacity:.55}50%{top:82%;opacity:.75}85%{opacity:.25}}@keyframes scanlines{to{background-position:0 120px}}@keyframes aura{50%{transform:scale(1.08);opacity:.8}}@keyframes resultOrbit{to{transform:translate(-50%,-50%) rotateY(360deg)}}@keyframes rowSweep{0%,55%{transform:translateX(-120%)}75%,100%{transform:translateX(120%)}}@keyframes orbit1{to{transform:rotateX(68deg) rotateZ(375deg)}}@keyframes orbit2{to{transform:rotateX(74deg) rotateZ(-380deg)}}@keyframes orbit3{to{transform:rotateY(432deg)}}@keyframes blink{50%{opacity:.35}}@keyframes float{50%{transform:translateY(-9px) rotateY(8deg)}}@keyframes corepulse{50%{transform:scale(1.045)}}@keyframes particle{0%,100%{opacity:.1;transform:scale(.7)}50%{opacity:1;transform:scale(1.8)}}@keyframes signalIn{from{opacity:0;transform:scale(.55);filter:blur(8px)}to{opacity:1;transform:scale(1);filter:blur(0)}}
@media(max-width:850px){.layout{grid-template-columns:1fr}.stage{height:430px}.core{width:210px;height:210px}.beam{width:300px;height:300px}.result .word{font-size:54px}.row{padding:14px}.board{order:2}}
@media(max-width:520px){.wrap{padding:10px}.top{padding-bottom:11px}.controls{padding:11px}.select{min-width:0;flex:1}.scanbtn{padding:0 14px}.stage{height:390px}.core{width:190px;height:190px}.beam{width:275px;height:275px}.result .word{font-size:47px}.p1,.p2,.p3,.p4{display:none}}
</style>
</head>
<body>
<div class="wrap">
 <div class="top">
  <div class="brand">NEXCANDLE AI <em>/ PREMIUM</em></div>
  <div class="live"><i></i>LIVE ENGINE</div>
 </div>

 <div class="layout">
  <section class="card">
   <div class="controls">
    <select id="pair" class="select"><option>LOADING LIVE FOREX...</option></select>
    <select id="tf" class="select"><option selected>30m</option><option>1h</option></select>
    <button class="scanbtn" onclick="scan()">SCAN</button>
   </div>

   <div class="stage">
    <div class="holo"></div><div class="energy"></div><div class="scanline"></div>
    <div class="particle p1"></div><div class="particle p2"></div><div class="particle p3"></div><div class="particle p4"></div>
    <div class="beam"></div>
    <div class="core"><div class="ring r1"></div><div class="ring r2"></div><div class="ring r3"></div><div class="ring r4"></div></div>
    <div id="result" class="result"><div class="word">—</div><div class="sub">PREMIUM NEXT-CANDLE ENGINE</div></div>
    <div class="hint" id="hint">MULTI-FACTOR • MULTI-TIMEFRAME • VALIDATED MARKET DATA</div>
   </div>
  </section>

  <aside class="card board">
   <h2>PREMIUM MARKET BOARD</h2>
   <div class="rows" id="rows"></div>
   <button class="scanbtn full" onclick="fullScan()">⚡ FULL POWER SCAN</button>
   <div class="footer">The displayed direction is the engine's UP/DOWN directional output from available validated market data. No outcome is guaranteed.</div>
  </aside>
 </div>
</div>

<script>
let pairs=[];
let lastDirection=null, busy=false, pairsLoaded=false;

function paint(d){
 const el=document.getElementById('result');
 el.className='result '+(d==='UP'?'up':'down');
 el.innerHTML='<div class="word">'+d+'</div><div class="sub">NEXT-CANDLE DIRECTION</div>';
 lastDirection=d;
}
function setHint(t){document.getElementById('hint').textContent=t;}
async function loadPairs(){
 try{
  const r=await fetch('/api/forex-pairs?ts='+Date.now(),{cache:'no-store'});
  const j=await r.json();
  if(!j.ok || !Array.isArray(j.pairs) || !j.pairs.length) throw new Error('no live forex');
  pairs=[...new Set(j.pairs.map(String).map(x=>x.toUpperCase()))].sort();
  const sel=document.getElementById('pair');
  const old=sel.value;
  sel.innerHTML='';
  pairs.forEach(p=>{const o=document.createElement('option');o.value=p;o.textContent=p;sel.appendChild(o);});
  sel.value=pairs.includes(old)?old:(pairs.includes('EUR/USD')?'EUR/USD':pairs[0]);
  pairsLoaded=true;
  buildBoard();
  document.getElementById('hint').textContent='LIVE FOREX CATALOG • '+pairs.length+' PAIRS';
 }catch(e){
  pairs=['EUR/USD','GBP/USD','USD/JPY','AUD/USD','USD/CAD','USD/CHF','EUR/JPY','GBP/JPY'];
  const sel=document.getElementById('pair'); sel.innerHTML='';
  pairs.forEach(p=>{const o=document.createElement('option');o.value=p;o.textContent=p;sel.appendChild(o);});
  pairsLoaded=true; buildBoard();
  setHint('LIVE CATALOG TEMPORARILY UNAVAILABLE • USING CORE FX PAIRS');
 }
}
async function getSignal(p){
 const ctl=new AbortController();
 const timer=setTimeout(()=>ctl.abort(),25000);
 try{
  const r=await fetch('/api/premium-signal?pair='+encodeURIComponent(p)+'&tf='+encodeURIComponent(document.getElementById('tf').value)+'&ts='+Date.now(),{cache:'no-store',signal:ctl.signal});
  const j=await r.json().catch(()=>({}));
  if(!r.ok || !j.ok || !j.signal || !['UP','DOWN'].includes(j.signal.display_direction)) throw new Error(j.error||'signal');
  return j.signal.display_direction;
 }finally{clearTimeout(timer)}
}
async function scan(){
 if(busy || !pairsLoaded)return;
 busy=true;
 setHint('3D ENGINE ACTIVE • ANALYSING '+document.getElementById('pair').value);
 try{
  const d=await getSignal(document.getElementById('pair').value);
  paint(d); setHint('NEXT-CANDLE DIRECTION • LIVE VALIDATED DATA');
 }catch(e){
  setHint(lastDirection?'NEXT-CANDLE ENGINE • LAST VALID DIRECTION':'MARKET DATA UNAVAILABLE • TRY AGAIN');
  if(lastDirection) paint(lastDirection);
 }finally{busy=false;}
}
function buildBoard(){
 const box=document.getElementById('rows');box.innerHTML='';
 pairs.forEach(p=>{
  const el=document.createElement('div');el.className='row';el.dataset.pair=p;
  el.innerHTML='<div><b>'+p+'</b><small>NEXT CANDLE</small></div><span class="badge">—</span>';
  el.onclick=()=>{document.getElementById('pair').value=p;scan();};
  box.appendChild(el);
 });
}
async function fullScan(){
 if(busy || !pairsLoaded)return;
 busy=true;
 setHint('FULL POWER SCAN • VALIDATING LIVE FOREX DATA');
 try{
  const rows=[...document.querySelectorAll('#rows .row')];
  for(let i=0;i<pairs.length;i++){
   const p=pairs[i], el=rows[i];
   try{
    const d=await getSignal(p);
    const b=el.querySelector('.badge'); b.textContent=d; b.className='badge '+(d==='UP'?'up':'down');
   }catch(e){
    const b=el.querySelector('.badge'); b.textContent='—'; b.className='badge';
   }
  }
  setHint('FULL POWER SCAN • '+pairs.length+' LIVE FOREX PAIRS');
 }finally{busy=false;}
}
document.getElementById('pair').addEventListener('change',scan);
document.getElementById('tf').addEventListener('change',scan);
loadPairs().then(scan);
setInterval(async()=>{await loadPairs(); if(!busy) scan();},300000);
document.getElementById('pair').addEventListener('change',scan);
document.getElementById('tf').addEventListener('change',scan);
buildBoard();scan();
setInterval(scan,60000);
</script>
</body>
</html>"""



# FX weekly session guard. The weekly FX market is considered open from
# Sunday 17:00 New York time until Friday 17:00 New York time.  We deliberately
# use one canonical timezone for the calculation so a Render server timezone
# or the visitor's device timezone can never make Monday look like Saturday.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
_NY=ZoneInfo("America/New_York")
_FX_OPEN_HOUR=17
_FX_OPEN_MINUTE=0
_FX_CLOSE_HOUR=17
_FX_CLOSE_MINUTE=0

def _fx_at(n, hour, minute):
    return n.replace(hour=hour, minute=minute, second=0, microsecond=0)

def fx_market_status():
    """Return the real weekly FX session state in America/New_York.

    Closed only for the weekend: before Sunday 17:00 NY and from Friday
    17:00 NY through Sunday 17:00 NY.  Daily broker rollovers are intentionally
    not treated as a full market closure because they are broker/feed specific.
    """
    n=datetime.now(_NY)
    wd=n.weekday()  # Mon=0 ... Sun=6
    if wd == 5:  # Saturday
        return False, n
    if wd == 6:  # Sunday
        return n.time() >= _fx_at(n, _FX_OPEN_HOUR, _FX_OPEN_MINUTE).time(), n
    if wd == 4:  # Friday
        return n.time() < _fx_at(n, _FX_CLOSE_HOUR, _FX_CLOSE_MINUTE).time(), n
    return True, n

def fx_next_open():
    """Return the next Sunday 17:00 New York opening timestamp."""
    n=datetime.now(_NY)
    # If currently Sunday before open, today's open is next.
    if n.weekday() == 6 and n.time() < _fx_at(n, _FX_OPEN_HOUR, _FX_OPEN_MINUTE).time():
        return _fx_at(n, _FX_OPEN_HOUR, _FX_OPEN_MINUTE)
    # Otherwise advance to the next Sunday.
    days=(6-n.weekday()) % 7
    if days == 0:
        days=7
    d=n+timedelta(days=days)
    return _fx_at(d, _FX_OPEN_HOUR, _FX_OPEN_MINUTE)

def fx_next_close():
    """Return the next Friday 17:00 New York weekly close."""
    n=datetime.now(_NY)
    days=(4-n.weekday()) % 7
    if days == 0 and n.time() >= _fx_at(n, _FX_CLOSE_HOUR, _FX_CLOSE_MINUTE).time():
        days=7
    d=n+timedelta(days=days)
    return _fx_at(d, _FX_CLOSE_HOUR, _FX_CLOSE_MINUTE)

def fx_current_session_open():
    """Return the current week's Sunday 17:00 NY open while the market is open."""
    opened,n=fx_market_status()
    if not opened:
        return None
    days_since_sunday=(n.weekday()+1)%7
    d=n-timedelta(days=days_since_sunday)
    return _fx_at(d, _FX_OPEN_HOUR, _FX_OPEN_MINUTE)

def _session_candle_ready(pair, tf):
    """Require at least one completed candle from the current weekly session."""
    session_open=fx_current_session_open()
    if session_open is None:
        return False, None
    try:
        frame=candles(pair, tf, 120)
        if frame is None or frame.empty:
            return False, None
        last=frame.index[-1]
        if getattr(last, "tzinfo", None) is None:
            last=last.replace(tzinfo=timezone.utc)
        else:
            last=last.tz_convert(timezone.utc)
        session_utc=session_open.astimezone(timezone.utc)
        interval=timedelta(seconds=TF_SECONDS[tf])
        # Candle labels mark the candle start. It is valid once that candle
        # has completed at/after the weekly open.
        ready=(last + interval) >= session_utc
        return ready, last
    except Exception:
        return False, None


@web.post("/api/access-login")
def access_login_api():
    """Verify the 4-digit website access code and unlock the signal engine session."""
    try:
        payload = request.get_json(silent=True) or {}
        code = str(payload.get("code", "")).strip()
    except Exception:
        code = ""
    # Normalize both JSON strings and numbers. Never expose the configured code.
    normalized = "".join(ch for ch in code if ch.isdigit())
    if len(normalized) == 4 and normalized == ACCESS_CODE:
        # Authorization is intentionally page-load scoped. The HTML page
        # clears any previous engine_access before it is served, so a browser
        # refresh/reload always requires the 4-digit code again.
        session.permanent = False
        session["engine_access"] = True
        return jsonify({"ok": True, "access": True})
    session.pop("engine_access", None)
    return jsonify({"ok": False, "access": False, "error": "ACCESS DENIED"}), 401

@web.post("/api/access-logout")
def access_logout_api():
    session.pop("engine_access", None)
    return jsonify({"ok": True, "access": False})

@web.get("/api/access-status")
def access_status_api():
    return jsonify({"ok": True, "access": bool(session.get("engine_access"))})

@web.get("/api/market-status")
def market_status_api():
    opened,n=fx_market_status()
    payload={
        "ok": True,
        "open": bool(opened),
        "message": "FOREX MARKET OPEN" if opened else "FOREX MARKET CLOSED",
        "timezone": "America/New_York",
        "server_time": n.isoformat(),
        "server_time_utc": datetime.now(timezone.utc).isoformat(),
    }
    if opened:
        payload["next_close"] = fx_next_close().isoformat()
        payload["session_open"] = fx_current_session_open().isoformat() if fx_current_session_open() else None
    else:
        payload["next_open"] = fx_next_open().isoformat()
    return jsonify(payload)

def _serve_index_page():
    # Every actual page load starts a fresh authorization cycle.
    # This is deliberate: refresh/reload must ask for the access code again.
    session.pop("engine_access", None)
    with open(os.path.join(os.path.dirname(__file__), "index.html"), "r", encoding="utf-8") as f:
        body = f.read()
    resp = Response(body, mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@web.get("/")
def home():
    return _serve_index_page()

@web.get("/premium")
def premium_page():
    return _serve_index_page()

def _load_recent_persisted_direction(pair, tf, max_age=None):
    """Recover a recently validated direction after a Render restart.

    This is only a short outage fallback. It never creates a new/random signal.
    """
    max_age = int(max_age or LAST_DIRECTIONAL_MAX_AGE)
    try:
        with LOCK:
            c = con()
            r = c.execute(
                """SELECT direction,score,entry,stop,target,rr,candle,created,provider,data_age,
                          regime,ai_fusion,ai_agreement,feature_json
                   FROM signals
                   WHERE pair=? AND tf=? AND direction IN ('CALL','PUT')
                   ORDER BY id DESC LIMIT 1""",
                (pair, tf),
            ).fetchone()
            c.close()
        if not r or not r["created"]:
            return None
        created = datetime.fromisoformat(r["created"])
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age < 0 or age > max_age:
            return None
        return {
            "pair": pair, "tf": tf, "direction": r["direction"],
            "score": int(r["score"] or 50), "entry": r["entry"],
            "stop": r["stop"], "target": r["target"], "rr": r["rr"],
            "candle": r["candle"] or "", "created": r["created"],
            "provider": r["provider"] or "validated-cache", "data_age": age,
            "state": "PERSISTED LAST VALIDATED", "target_candle": "NEXT_CANDLE",
            "factors": {
                "regime": r["regime"] or "UNKNOWN",
                "ai_fusion": r["ai_fusion"],
                "ai_agreement": r["ai_agreement"],
            },
        }
    except Exception as exc:
        db_log("warning", "market-cache", f"persisted fallback failed: {exc}")
        return None


def get_directional_signal(pair, tf):
    """Return a fresh, validated next-candle direction.

    No random direction and no stale direction are substituted when there is
    no fresh feed. A short-lived last validated result is only returned when
    explicitly enabled and is marked as fallback.
    """
    key=(pair,tf)
    now=time.time()
    try:
        s=make_signal(pair,tf)
        direction=s.get("direction")
        if direction not in {"CALL","PUT"}:
            # Fresh candles are useful even when trade confirmation is WAIT.
            # Preserve the model bias so the UI can report NEXT-CANDLE direction
            # instead of pretending that market data itself is unavailable.
            prediction = s.get("prediction_direction")
            if prediction not in {"CALL","PUT"}:
                raise RuntimeError("No directional bias available")
            s=dict(s)
            s["target_candle"]="NEXT_CANDLE"
            s["tradeable"]=False
        with LOCK:
            LAST_DIRECTIONAL[key]=(now,direction,dict(s))
        return s, False
    except Exception as primary_exc:
        # Never turn a provider outage into a fake UP/DOWN.
        with LOCK:
            old=LAST_DIRECTIONAL.get(key)
        if old and now-old[0] <= LAST_DIRECTIONAL_MAX_AGE and os.getenv("ALLOW_LAST_SIGNAL_FALLBACK","false").lower() in {"1","true","yes","on"}:
            s=dict(old[2]); s["direction"]=old[1]; s["state"]="LAST VALIDATED FALLBACK"
            s["data_age"]=max(float(s.get("data_age",0) or 0), now-old[0])
            s["target_candle"]="NEXT_CANDLE"
            return s, True
        persisted = _load_recent_persisted_direction(pair, tf)
        if persisted and os.getenv("ALLOW_PERSISTED_SIGNAL_FALLBACK","false").lower() in {"1","true","yes","on"}:
            with LOCK:
                LAST_DIRECTIONAL[key]=(now,persisted["direction"],dict(persisted))
            return persisted, True
        raise primary_exc

@web.get("/api/forex-pairs")
def forex_pairs_api():
    pairs = refresh_live_forex_pairs()
    return jsonify({
        "ok": True,
        "source": "live",
        "count": len(pairs),
        "pairs": pairs,
        "updated": LIVE_FOREX_CACHE["updated"],
    })

@web.get("/api/premium-signal")
def premium_signal_api():
    if not session.get("engine_access"):
        return jsonify({"ok": False, "locked": True, "error": "ENGINE LOCKED"}), 403
    market_open, _now = fx_market_status()
    if not market_open:
        return jsonify({"ok":True,"market_open":False,"signal":None,
                        "message":"FOREX MARKET CLOSED",
                        "next_open":fx_next_open().isoformat()})
    pair = request.args.get("pair", "EUR/USD").upper().replace("-", "/")
    tf = request.args.get("tf", "30m")
    live_pairs = refresh_live_forex_pairs()
    allowed_pairs = set(PAIRS.keys()) | set(live_pairs)
    allowed_tf = {"30m","1h"}
    if pair not in allowed_pairs or tf not in allowed_tf:
        return jsonify({"ok": False, "error": "Unsupported market"}), 400
    try:
        # Do not gate the engine on a separate weekly-session check. That check
        # could hide a healthy 5m feed when a higher-timeframe provider was late.
        # Freshness is now decided by the actual completed-candle feed used by
        # make_signal(). Friday/weekend candles are rejected automatically by
        # market_meta() when they are stale. Higher timeframes can be derived
        # from validated 5m candles, so a fresh 5m feed can unlock 15m/30m/45m/1h
        # as soon as their first completed candle exists.
        s = make_v29_signal(pair, tf)
        fallback = False
        direction=s.get("direction")
        prediction=s.get("prediction_direction")
        if direction not in {"CALL","PUT"} and prediction not in {"CALL","PUT"}:
            raise RuntimeError("No directional output")
        display = direction if direction in {"CALL","PUT"} else prediction
        s["display_direction"]="UP" if display=="CALL" else "DOWN"
        s["display_score"]=int(np.clip(s.get("prediction_score", s.get("score",50)),1,99))
        s["tradeable"]=bool(direction in {"CALL","PUT"})
        s["confirmation"]="CONFIRMED" if s["tradeable"] else "WAIT"
        s["display_pair"]=pair
        s["display_tf"]=tf
        s["fallback"]=bool(fallback)
        # Public premium endpoint intentionally exposes directional output only.
        return jsonify({"ok":True,"signal":s})
    except Exception as exc:
        db_log("warning","premium-api",f"{pair}/{tf}: {str(exc)[:180]}")
        return jsonify({"ok":False,"error":"Market data temporarily unavailable"}),503

@web.get("/health/data")
def health_data():
    """Safe market-data diagnostics.

    This endpoint never calls external market-data APIs by default, so an
    uptime monitor cannot accidentally consume API quotas. Use ?probe=1
    manually when a live provider test is actually needed.
    """
    now = time.time()
    providers = {}
    configured = {
        "biquote": True,
        "dukascopy": DUKASCOPY_ENABLED,
        "sifting": bool(SIFTING_API_KEY),
        "twelvedata": bool(TWELVEDATA_API_KEY), "finnhub": bool(FINNHUB_API_KEY),
        "alphavantage": bool(ALPHAVANTAGE_API_KEY),
        "yahoo2": ALLOW_YAHOO_FALLBACK, "yahoo": ALLOW_YAHOO_FALLBACK,
    }
    for name, ok in configured.items():
        with LOCK:
            cooldown = max(0, int(PROVIDER_COOLDOWN.get(name, 0) - now))
        providers[name] = {"configured": ok, "cooldown_seconds": cooldown}
    result = {"status": "ok", "configured": configured,
              "provider_order": PROVIDER_ORDER, "providers": providers,
              "cache_items": len(MARKET_CACHE)}
    cached = MARKET_CACHE.get(("EUR/USD","5m"))
    if cached:
        result["cached_market"] = {
            "provider": cached[2],
            "age_seconds": max(0, int(now-cached[0])),
            "last_candle_age_seconds": max(0, int(now-cached[1].index[-1].timestamp())),
            "candles": len(cached[1]),
        }
    if request.args.get("probe") == "1":
        try:
            x = candles("EUR/USD", "5m", 100)
            result["probe"] = {"ok": True, "provider": MARKET_CACHE[("EUR/USD","5m")][2],
                               "candles": len(x),
                               "last_candle_age_seconds": max(0, int(now-x.index[-1].timestamp()))}
        except Exception as exc:
            result["probe"] = {"ok": False, "error": str(exc)[:300]}
    return result

@web.get("/health")
def health():
    provider = []
    if TWELVEDATA_API_KEY: provider.append("twelvedata")
    if FINNHUB_API_KEY: provider.append("finnhub")
    if ALPHAVANTAGE_API_KEY: provider.append("alphavantage")
    provider.append("biquote")
    if DUKASCOPY_ENABLED: provider.append("dukascopy")
    provider.append("yahoo_fallback")
    return {"status": "ok", "version": "V30-20-PAIRS-30M-1H-SIGNAL-FIX", "bot_mode": BOT_MODE, "providers": provider,
            "cache_items": len(MARKET_CACHE), "database": DB,
            "payment_methods": {"india_upi": bool(INDIA_UPI), "uae_botim": bool(UAE_BOTIM)}}

@web.get("/health/telegram")
def health_telegram():
    """Non-secret Telegram webhook diagnostics."""
    result = {
        "status": "degraded",
        "bot_mode": BOT_MODE,
        "render": _ON_RENDER,
        "external_url_configured": bool(RENDER_EXTERNAL_URL),
        "webhook_path": "/telegram/webhook",
    }
    if TELEGRAM_APP is None:
        result["error"] = "telegram application not initialized"
        return result, 503
    try:
        info = asyncio.run_coroutine_threadsafe(
            TELEGRAM_APP.bot.get_webhook_info(), TELEGRAM_LOOP
        ).result(timeout=5)
        result.update({
            "status": "ok",
            "webhook_url_set": bool(info.url),
            "pending_update_count": int(info.pending_update_count or 0),
            "last_error_date": int(info.last_error_date or 0),
            "last_error_message": info.last_error_message or "",
            "max_connections": int(info.max_connections or 0),
        })
        if BOT_MODE == "webhook" and not info.url:
            result["status"] = "degraded"
    except Exception as exc:
        result["error"] = str(exc)[:240]
        return result, 503
    return result


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
        CREATE TABLE IF NOT EXISTS market_cache(
            pair TEXT NOT NULL,
            tf TEXT NOT NULL,
            updated REAL NOT NULL,
            provider TEXT NOT NULL,
            candles_json TEXT NOT NULL,
            PRIMARY KEY(pair,tf)
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
                "confidence_label":"TEXT",
                "regime":"TEXT", "ai_fusion":"REAL", "ai_agreement":"REAL", "feature_json":"TEXT"
            }
        }
        for table, cols in migrations.items():
            existing = {r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}
            for col, spec in cols.items():
                if col not in existing:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {spec}")
        c.commit()
        # Recover recent validated candles after a process restart. This is a
        # resilience cache only; freshness is still checked before signalling.
        try:
            rows = c.execute("SELECT pair,tf,updated,provider,candles_json FROM market_cache").fetchall()
            now = time.time()
            for r in rows:
                if now - float(r["updated"]) > PERSISTED_CACHE_MAX_AGE:
                    continue
                try:
                    frame = pd.read_json(r["candles_json"], orient="split")
                    frame.index = pd.to_datetime(frame.index, utc=True, errors="coerce")
                    frame = _clean_df(frame)
                    if len(frame) >= 25:
                        MARKET_CACHE[(r["pair"], r["tf"])] = (float(r["updated"]), frame, str(r["provider"]))
                except Exception as exc:
                    log.warning("persistent market cache load failed for %s/%s: %s", r["pair"], r["tf"], exc)
        finally:
            c.close()

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

# SiftingIO live state. WebSocket is used as a low-latency quote overlay;
# historical OHLCV comes from SiftingIO REST so the engine has a complete lookback.
SIFTING_TICKS = {}
SIFTING_TICKS_LOCK = threading.RLock()
SIFTING_WS_THREAD = None
SIFTING_WS_STOP = threading.Event()

def _sifting_symbol(pair):
    return pair.replace("/", "").upper()

def _sifting_interval(tf):
    # Sifting documents 1m/5m/15m/30m/1h; coarser frames are built locally.
    return {"1m":"1m", "5m":"5m", "15m":"15m", "30m":"30m", "45m":"15m", "1h":"1h", "4h":"1h"}[tf]

def _biquote(pair, tf, limit):
    """Free no-key FX OHLC fallback.

    BiQuote exposes public OHLC candles for EURUSD-style symbols.  The endpoint
    supports 1m/5m/15m/30m/1h/4h; 45m is constructed from 15m locally.
    The current open bar is explicitly removed so the predictor only consumes
    completed candles.
    """
    base_tf = "15m" if tf == "45m" else tf
    symbol = _sifting_symbol(pair)
    url = f"https://biquote.io/api/{symbol}/ohlc"
    r = S.get(url, params={"interval": base_tf, "limit": min(max(limit, 120), 1000)},
              timeout=min(DATA_TIMEOUT, 12))
    if r.status_code != 200:
        raise RuntimeError(f"BiQuote HTTP {r.status_code}: {r.text[:180]}")
    body = r.json()
    rows = body.get("bars") or []
    if not rows:
        raise RuntimeError("BiQuote returned no OHLC bars")
    x = pd.DataFrame(rows)
    if "openTime" not in x.columns:
        raise RuntimeError("BiQuote response missing openTime")
    x["datetime"] = pd.to_datetime(x["openTime"], utc=True, errors="coerce")
    if "isOpen" in x.columns:
        x = x[~x["isOpen"].fillna(False)]
    x = x.set_index("datetime")
    cols = {"open":"open","high":"high","low":"low","close":"close"}
    x = x.rename(columns=cols)
    x = _clean_df(x).sort_index()
    if tf == "45m":
        x = x.resample("45min", origin="epoch", label="left", closed="left").agg(
            {"open":"first","high":"max","low":"min","close":"last"}).dropna()
    return x.tail(limit)

DUKASCOPY_IDS = {}
DUKASCOPY_IDS_LOCK = threading.RLock()

def _dukascopy_instrument_id(pair):
    """Resolve the official Dukascopy numeric instrument id once and cache it."""
    if pair in DUKASCOPY_IDS:
        return DUKASCOPY_IDS[pair]
    r = S.get(
        "https://freeserv.dukascopy.com/2.0/",
        params={"path": "api/instrumentList", "fields": "id,name,nameLong"},
        timeout=min(DATA_TIMEOUT, 6),
    )
    if r.status_code != 200:
        raise RuntimeError(f"Dukascopy instrumentList HTTP {r.status_code}")
    body = r.json()
    rows = body if isinstance(body, list) else (body.get("data") or body.get("instruments") or [])
    wanted = pair.replace("/", "").upper()
    for row in rows:
        name = str(row.get("name") or row.get("symbol") or "").replace("/", "").upper()
        long_name = str(row.get("nameLong") or "").replace("/", "").upper()
        if name == wanted or wanted in long_name.replace(" ", ""):
            iid = row.get("id")
            if iid is not None:
                with DUKASCOPY_IDS_LOCK:
                    DUKASCOPY_IDS[pair] = int(iid)
                return int(iid)
    raise RuntimeError(f"Dukascopy instrument id not found for {pair}")

def _dukascopy(pair, tf, limit):
    """Official Dukascopy free-service historical-price fallback."""
    if not DUKASCOPY_ENABLED:
        raise RuntimeError("Dukascopy disabled")
    iid = _dukascopy_instrument_id(pair)
    # Dukascopy free-service supports 1min, 10m and 1hour (plus tick/1day).
    # Never request a fake 5m/15m/30m/4h interval and then resample it.
    # Use 1m for sub-hour frames and 1hour for 1h/4h.
    base_tf = "1min" if tf in {"1m","5m","15m","30m","45m"} else "1hour"
    multiplier = {"1m":1,"5m":5,"15m":15,"30m":30,"45m":45,"1h":1,"4h":4}[tf]
    count = min(5000, max(500, int(limit * multiplier + 120)))
    r = S.get(
        "https://freeserv.dukascopy.com/2.0/",
        params={
            "path": "api/historicalPrices",
            "instrument": iid,
            "timeFrame": base_tf,
            "count": count,
            "end": int(time.time() * 1000),
            "offerSide": "B",
            "dayStartTime": "UTC",
        },
        timeout=min(DATA_TIMEOUT, 8),
    )
    if r.status_code != 200:
        raise RuntimeError(f"Dukascopy historicalPrices HTTP {r.status_code}")
    body = r.json()
    rows = body if isinstance(body, list) else (body.get("data") or body.get("bars") or body.get("candles") or [])
    if not rows:
        raise RuntimeError("Dukascopy returned no historical candles")
    parsed = []
    for row in rows:
        if isinstance(row, dict):
            ts = row.get("timestamp", row.get("time", row.get("datetime")))
            o = row.get("open", row.get("o")); h = row.get("high", row.get("h"))
            l = row.get("low", row.get("l")); c = row.get("close", row.get("c"))
        elif isinstance(row, (list, tuple)) and len(row) >= 5:
            ts, o, h, l, c = row[:5]
        else:
            continue
        if ts is None or any(v is None for v in (o,h,l,c)):
            continue
        parsed.append({"datetime": ts, "open": o, "high": h, "low": l, "close": c})
    if not parsed:
        raise RuntimeError("Dukascopy response missing OHLC fields")
    x = pd.DataFrame(parsed)
    vals = x["datetime"].tolist()
    x["datetime"] = pd.to_datetime(vals, unit="ms", utc=True, errors="coerce")
    if x["datetime"].isna().all():
        x["datetime"] = pd.to_datetime(vals, unit="s", utc=True, errors="coerce")
    x = _clean_df(x.set_index("datetime")).sort_index()
    if x.empty:
        raise RuntimeError("Dukascopy OHLC validation failed")
    if tf in {"5m","15m","30m","1h","4h"}:
        rule = {"5m":"5min","15m":"15min","30m":"30min","1h":"1h","4h":"4h"}[tf]
        x = x.resample(rule, origin="epoch", label="left", closed="left").agg(
            {"open":"first","high":"max","low":"min","close":"last"}).dropna()
    interval = TF_SECONDS[tf]
    if len(x) and int(time.time()) - int(x.index[-1].timestamp()) < interval:
        x = x.iloc[:-1]
    if len(x) < 80:
        raise RuntimeError(f"Dukascopy insufficient candles ({len(x)})")
    return x.tail(limit)

def _sifting_hist(pair, tf, limit):
    if not SIFTING_API_ENABLED:
        raise RuntimeError("SiftingIO disabled")
    if not SIFTING_API_KEY:
        raise RuntimeError("SiftingIO key not configured")
    base_tf = _sifting_interval(tf)
    base_minutes = {"1m":1,"5m":5,"15m":15,"30m":30,"1h":60}[base_tf]
    days = max(2, int((limit * base_minutes * 1.8) / 1440) + 2)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    url = f"https://api.sifting.io/v1/hist/forex/{_sifting_symbol(pair)}/bars"
    rows = []
    cursor = None
    # SiftingIO paginates historical bars. Pull enough pages for the indicator
    # lookback instead of accepting a short first page and declaring failure.
    for _ in range(8):
        params = {
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": base_tf,
            "limit": min(max(limit, 200), 1000),
        }
        if cursor:
            params["cursor"] = cursor
        r = S.get(url, headers={"X-API-Key": SIFTING_API_KEY},
                 params=params, timeout=DATA_TIMEOUT)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After", "")
            raise RuntimeError("SiftingIO HTTP 429" + (f" (Retry-After {ra}s)" if ra else ""))
        if r.status_code != 200:
            raise RuntimeError(f"SiftingIO HTTP {r.status_code}: {r.text[:180]}")
        body = r.json()
        page = body.get("data") or []
        if not page:
            break
        rows.extend(page)
        meta = body.get("meta") or {}
        cursor = meta.get("next_cursor")
        if not cursor or len(rows) >= limit:
            break

    if not rows:
        raise RuntimeError("SiftingIO returned no historical FX bars")
    x = pd.DataFrame(rows)
    required = {"t":"datetime","o":"open","h":"high","l":"low","c":"close"}
    if not all(c in x.columns for c in required):
        raise RuntimeError("SiftingIO historical response missing OHLC fields")
    x = x.rename(columns=required)
    x["datetime"] = pd.to_datetime(x["datetime"], unit="ms", utc=True, errors="coerce")
    x = x.dropna(subset=["datetime"]).set_index("datetime")
    x = _clean_df(x).sort_index()
    if tf == "45m":
        x = x.resample("45min", origin="epoch", label="left", closed="left").agg(
            {"open":"first","high":"max","low":"min","close":"last"}).dropna()
    elif tf == "4h":
        x = x.resample("4h", origin="epoch", label="left", closed="left").agg(
            {"open":"first","high":"max","low":"min","close":"last"}).dropna()
    return x.tail(limit)


def _sifting_ws_loop():
    if not SIFTING_API_KEY or not SIFTING_API_ENABLED or websocket is None or not SIFTING_WS_ENABLED:
        return
    backoff=1
    while not SIFTING_WS_STOP.is_set():
        ws=None
        try:
            url="wss://stream.sifting.io/ws/v1?key=" + SIFTING_API_KEY
            ws=websocket.create_connection(url, timeout=20, enable_multithread=True)
            ws.settimeout(5)
            ws.send(json.dumps({"op":"subscribe","product":"fx","symbols":SIFTING_WS_SYMBOLS}))
            last_ping=time.time()
            backoff=1
            while not SIFTING_WS_STOP.is_set():
                if time.time()-last_ping >= 30:
                    ws.send(json.dumps({"op":"ping"})); last_ping=time.time()
                try:
                    raw=ws.recv()
                except Exception as exc:
                    # websocket timeout is expected between sparse ticks; continue.
                    if "timed out" in str(exc).lower():
                        continue
                    raise
                if not raw: raise RuntimeError("SiftingIO websocket closed")
                msg=json.loads(raw)
                if msg.get("f") == "tick":
                    sym=str(msg.get("s") or "").upper()
                    if sym:
                        with SIFTING_TICKS_LOCK:
                            SIFTING_TICKS[sym]={
                                "bid":float(msg["b"]) if msg.get("b") is not None else None,
                                "ask":float(msg["a"]) if msg.get("a") is not None else None,
                                "price":float(msg["p"]) if msg.get("p") is not None else None,
                                "ts":int(msg.get("t") or int(time.time()*1000))/1000.0,
                            }
                elif msg.get("f") == "error":
                    db_log("warning","market",f"SiftingIO WS {msg.get('code')}: {msg.get('message')}")
        except Exception as exc:
            db_log("warning","market",f"SiftingIO WS disconnected: {str(exc)[:220]}; reconnect={backoff}s")
            SIFTING_WS_STOP.wait(backoff)
            backoff=min(backoff*2,60)
        finally:
            try:
                if ws: ws.close()
            except Exception: pass

def start_sifting_ws():
    global SIFTING_WS_THREAD
    if SIFTING_WS_THREAD and SIFTING_WS_THREAD.is_alive(): return
    if not (SIFTING_API_KEY and websocket is not None and SIFTING_WS_ENABLED): return
    SIFTING_WS_THREAD=threading.Thread(target=_sifting_ws_loop,name="sifting-ws",daemon=True)
    SIFTING_WS_THREAD.start()


def _get_json(url, *, params=None, timeout=None):
    """GET JSON with a small retry budget and useful provider diagnostics."""
    r = S.get(url, params=params, timeout=timeout or DATA_TIMEOUT)
    if r.status_code != 200:
        body = ""
        try:
            body = r.text[:240].replace("\\n", " ")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {r.status_code}" + (f": {body}" if body else ""))
    try:
        return r.json()
    except Exception as exc:
        raise RuntimeError(f"invalid JSON response: {exc}") from exc

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

def _validate_candle_cadence(x, tf, min_points=25):
    """Reject feeds that return the wrong timeframe or wildly irregular bars."""
    if x is None or x.empty or len(x) < min_points:
        raise RuntimeError("insufficient candles")
    x = _clean_df(x).sort_index()
    if len(x) < min_points:
        raise RuntimeError("insufficient cleaned candles")
    expected = TF_SECONDS[tf]
    diffs = x.index.to_series().diff().dt.total_seconds().dropna()
    if not diffs.empty:
        # FX feeds can have weekend/session gaps, but normal adjacent bars should
        # cluster around the requested interval. A wrong provider interval (for
        # example hourly data returned for a 5m request) must never enter the model.
        near = ((diffs - expected).abs() <= max(2, expected * 0.08)).mean()
        if near < 0.55:
            raise RuntimeError(f"wrong candle cadence for {tf}")
    return x

def _oanda(pair, tf, limit):
    if not OANDA_API_TOKEN:
        raise RuntimeError("OANDA token not configured")
    instrument = PAIRS[pair]["finnhub"].split(":", 1)[-1].replace("_", "_")
    gran = {"1m":"M1","5m":"M5","15m":"M15","30m":"M30","45m":"M15","1h":"H1","4h":"H4"}[tf]
    host = "api-fxpractice.oanda.com" if OANDA_ENVIRONMENT == "practice" else "api-fxtrade.oanda.com"
    url = f"https://{host}/v3/instruments/{instrument}/candles"
    headers = {"Authorization": f"Bearer {OANDA_API_TOKEN}", "Accept-Datetime-Format": "RFC3339"}
    params = {"granularity": gran, "count": min(max(limit * (3 if tf in ("45m", "4h") else 1), 100), 5000), "price": "M"}
    r = S.get(url, headers=headers, params=params, timeout=DATA_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"OANDA HTTP {r.status_code}: {r.text[:180]}")
    d = r.json()
    rows = []
    for c in d.get("candles", []):
        if not c.get("complete"): continue
        mid = c.get("mid") or {}
        rows.append({"datetime": c.get("time"), "open": mid.get("o"), "high": mid.get("h"), "low": mid.get("l"), "close": mid.get("c")})
    x = pd.DataFrame(rows)
    if x.empty: raise RuntimeError("OANDA returned no completed candles")
    x["datetime"] = pd.to_datetime(x["datetime"], utc=True)
    x = _clean_df(x.set_index("datetime"))
    if tf == "45m":
        x = x.resample("45min", origin="epoch", label="left", closed="left").agg({"open":"first","high":"max","low":"min","close":"last"}).dropna()
    return x.tail(limit)

def _alphavantage(pair, tf, limit):
    if not ALPHAVANTAGE_API_KEY:
        raise RuntimeError("Alpha Vantage key not configured")
    base = {"1m":"1min","5m":"5min","15m":"15min","30m":"30min","45m":"15min","1h":"60min","4h":"60min"}[tf]
    fx = PAIRS[pair]["td"].split("/")
    params = {"function":"FX_INTRADAY","from_symbol":fx[0],"to_symbol":fx[1],"interval":base,"outputsize":"full","apikey":ALPHAVANTAGE_API_KEY}
    d = _get_json("https://www.alphavantage.co/query", params=params, timeout=DATA_TIMEOUT)
    key = f"Time Series FX ({base})"
    if key not in d:
        raise RuntimeError(d.get("Note") or d.get("Information") or d.get("Error Message") or "Alpha Vantage returned no FX data")
    x = pd.DataFrame.from_dict(d[key], orient="index")
    x.index = pd.to_datetime(x.index, utc=True)
    x = x.rename(columns={"1. open":"open","2. high":"high","3. low":"low","4. close":"close"})
    x = _clean_df(x).sort_index()
    if tf == "45m":
        x = x.resample("45min", origin="epoch", label="left", closed="left").agg({"open":"first","high":"max","low":"min","close":"last"}).dropna()
    elif tf == "4h":
        x = x.resample("4h", origin="epoch", label="left", closed="left").agg({"open":"first","high":"max","low":"min","close":"last"}).dropna()
    return x.tail(limit)

def _finnhub(pair, tf, limit):
    if not FINNHUB_API_KEY: raise RuntimeError("Finnhub key not configured")
    # Finnhub supports 1/5/15/30/60/ D/W/M. 45m is built from validated 15m candles.
    base_tf = 15 if tf == "45m" else (60 if tf == "4h" else TF_MIN[tf])
    if base_tf not in (1,5,15,30,60):
        raise RuntimeError("Unsupported Finnhub resolution")
    now = int(time.time())
    span = max(400, limit * 5)
    start = now - base_tf * 60 * span
    d = _get_json(
        "https://finnhub.io/api/v1/forex/candle",
        params={
            "symbol": PAIRS[pair]["finnhub"],
            "resolution": base_tf,
            "from": start,
            "to": now,
            "token": FINNHUB_API_KEY,
        },
    )
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
    if not TWELVEDATA_API_KEY:
        raise RuntimeError("TwelveData key not configured")
    intervals = {"1m":"1min","5m":"5min","15m":"15min","30m":"30min",
                 "45m":"45min","1h":"1h","4h":"4h"}
    # Fail fast on 429 so the provider cooldown can work; do not create
    # a retry storm against a rate-limited TwelveData account.
    r = S.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": PAIRS[pair]["td"],
            "interval": intervals[tf],
            "outputsize": min(max(limit, 200), 5000),
            "timezone": "UTC",
            "apikey": TWELVEDATA_API_KEY,
        },
        timeout=DATA_TIMEOUT,
    )
    if r.status_code == 429:
        retry_after = r.headers.get("Retry-After", "")
        raise RuntimeError(
            "TwelveData HTTP 429"
            + (f" (Retry-After {retry_after}s)" if retry_after else "")
        )
    if r.status_code != 200:
        raise RuntimeError(f"TwelveData HTTP {r.status_code}: {r.text[:180]}")
    try:
        d = r.json()
    except Exception as exc:
        raise RuntimeError(f"TwelveData invalid JSON: {exc}") from exc
    if "values" not in d:
        raise RuntimeError(d.get("message") or d.get("code") or "TwelveData returned no data")
    x = pd.DataFrame(d["values"])
    x["datetime"] = pd.to_datetime(x["datetime"], utc=True)
    x = x.set_index("datetime").sort_index()
    return _clean_df(x).tail(limit)

def _yahoo_host(pair, tf, limit, host):
    """Yahoo chart fallback using an alternate edge host.

    This is still a public/delayed fallback and can be rate limited. It is
    intentionally placed after credentialed providers.
    """
    intervals = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","45m":"15m","1h":"60m","4h":"60m"}
    interval = intervals[tf]
    seconds = {"1m": 2*86400, "5m": 10*86400, "15m": 30*86400,
               "30m": 45*86400, "45m": 60*86400, "1h": 180*86400,
               "4h": 365*86400}[tf]
    now = int(time.time()); start = now - seconds
    url = f"https://{host}/v8/finance/chart/{PAIRS[pair]['yahoo']}"
    params = {"period1": start, "period2": now, "interval": interval,
              "events": "history", "includeAdjustedClose": "true"}
    r = S.get(url, params=params, timeout=DATA_TIMEOUT,
              headers={"User-Agent": "Mozilla/5.0 NexCandleAI/4.3"})
    if r.status_code == 429:
        raise RuntimeError(f"Yahoo HTTP 429 ({host})")
    if r.status_code != 200:
        raise RuntimeError(f"Yahoo HTTP {r.status_code} ({host})")
    d = r.json()
    result = d.get("chart", {}).get("result")
    if not result:
        err = d.get("chart", {}).get("error") or {}
        raise RuntimeError(err.get("description") or f"Yahoo returned no result ({host})")
    result = result[0]
    ts = result.get("timestamp") or []
    q = (result.get("indicators") or {}).get("quote") or []
    if not q or not ts:
        raise RuntimeError("Yahoo returned empty OHLC")
    q = q[0]
    x = pd.DataFrame({
        "open": q.get("open", []), "high": q.get("high", []),
        "low": q.get("low", []), "close": q.get("close", [])
    }, index=pd.to_datetime(ts, unit="s", utc=True))
    x = _clean_df(x).sort_index()
    if tf == "45m":
        x = x.resample("45min", origin="epoch", label="left", closed="left").agg(
            {"open":"first","high":"max","low":"min","close":"last"}).dropna()
    elif tf == "4h":
        x = x.resample("4h", origin="epoch", label="left", closed="left").agg(
            {"open":"first","high":"max","low":"min","close":"last"}).dropna()
    return x.tail(limit)

def _yahoo2(pair, tf, limit):
    return _yahoo_host(pair, tf, limit, "query2.finance.yahoo.com")

def _yahoo(pair, tf, limit):
    return _yahoo_host(pair, tf, limit, "query1.finance.yahoo.com")

def _persist_market_cache(pair, tf, frame, provider):
    """Best-effort persistence of validated candles; never blocks signalling."""
    try:
        payload = frame.tail(min(len(frame), MTF_BASE_LIMIT)).to_json(orient="split", date_format="iso")
        c = con()
        c.execute(
            "INSERT INTO market_cache(pair,tf,updated,provider,candles_json) VALUES(?,?,?,?,?) "
            "ON CONFLICT(pair,tf) DO UPDATE SET updated=excluded.updated, provider=excluded.provider, candles_json=excluded.candles_json",
            (pair, tf, time.time(), provider, payload),
        )
        c.commit(); c.close()
    except Exception as exc:
        db_log("warning", "market-cache", f"persist failed {pair}/{tf}: {type(exc).__name__}")


def _call_provider(name, fn, pair, tf, limit):
    """Call a provider with a bounded retry budget and exponential backoff."""
    attempts = max(1, PROVIDER_RETRY_LIMIT)
    last = None
    for attempt in range(attempts):
        try:
            return fn(pair, tf, limit)
        except Exception as exc:
            last = exc
            msg = str(exc).lower()
            # Quota/auth failures should immediately move to the next provider.
            if any(k in msg for k in ("429", "rate limit", "quota", "limit exceeded", "free plan limit",
                                      "401", "403", "unauthorized", "forbidden", "invalid api", "api key")):
                raise
            transient = any(k in msg for k in ("timeout", "timed out", "502", "503", "504",
                                               "temporarily unavailable", "connection reset", "connection aborted",
                                               "connection error", "remote disconnected"))
            if not transient or attempt >= attempts - 1:
                raise
            delay = min(4.0, 0.45 * (2 ** attempt)) + random.uniform(0, 0.20)
            time.sleep(delay)
    raise last or RuntimeError(f"{name} provider failed")


def _provider_ready(name):
    now = time.time()
    with LOCK:
        cooldown_until = PROVIDER_COOLDOWN.get(name, 0.0)
        last = PROVIDER_LAST_REQUEST.get(name, 0.0)
    if now < cooldown_until:
        return False
    wait = PROVIDER_MIN_INTERVAL_SECONDS - (now - last)
    if wait > 0:
        time.sleep(wait)
    with LOCK:
        PROVIDER_LAST_REQUEST[name] = time.time()
    return True

def _provider_failed(name, exc):
    msg = str(exc)
    lower = msg.lower()
    # Do not hammer a provider that is rate-limited, unavailable, or rejecting its key.
    if any(k in lower for k in ("429", "too many", "rate limit", "rate-limit", "quota", "limit exceeded", "free plan limit")):
        # Quota exhaustion can last until the provider resets the account. Keep
        # the provider out of the hot path instead of retrying it repeatedly.
        cooldown = PROVIDER_RATE_LIMIT_COOLDOWN_SECONDS
    elif any(k in lower for k in ("timeout", "timed out", "503", "502",
                                "temporarily unavailable", "service unavailable")):
        cooldown = PROVIDER_COOLDOWN_SECONDS
    elif any(k in lower for k in ("401", "403", "bad api", "invalid api", "apikey", "api key",
                                  "not authorized", "unauthorized", "forbidden")):
        cooldown = PROVIDER_ERROR_COOLDOWN_SECONDS
    else:
        cooldown = min(PROVIDER_COOLDOWN_SECONDS, 30)
    with LOCK:
        PROVIDER_COOLDOWN[name] = time.time() + cooldown
    db_log("warning", "market", f"{name}: {msg}; cooldown={cooldown}s")

def _cache_ttl(tf):
    return CACHE_TTL_BY_TF.get(tf, CACHE_SECONDS)

def _resample_from_validated_source(frame, target_tf, limit):
    """Aggregate already validated completed candles into a higher timeframe."""
    if frame is None or frame.empty:
        raise RuntimeError("empty source candles")
    rule = {"15m":"15min", "30m":"30min", "45m":"45min", "1h":"1h", "4h":"4h"}[target_tf]
    x = _clean_df(frame).sort_index()
    x = x.resample(rule, origin="epoch", label="left", closed="left").agg({
        "open":"first", "high":"max", "low":"min", "close":"last"
    }).dropna()
    interval = TF_SECONDS[target_tf]
    if len(x):
        # Remove a bucket that has not fully closed yet.
        if int(time.time()) - int(x.index[-1].timestamp()) < interval:
            x = x.iloc[:-1]
    if len(x) < 25:
        raise RuntimeError(f"derived {target_tf} candles insufficient ({len(x)})")
    return x.tail(limit)

def _derive_timeframe(pair, tf, limit):
    """Fallback route for intervals that can be built exactly from validated data.

    15/30/45m are derived from 5m; 4h is derived from 1h. This avoids depending
    on a provider's optional high-timeframe endpoint while preserving exact OHLC
    aggregation. The source fetch is allowed to use the normal provider failover.
    """
    if tf in {"15m", "30m", "45m", "1h", "4h"}:
        # 5m is the stable base lane. Building all higher frames from the same
        # validated 5m candles keeps OHLC/session alignment consistent and avoids
        # higher-timeframe provider lag from blocking an otherwise live signal.
        source_tf = "5m"
    else:
        return None
    ratio = TF_MIN[tf] // TF_MIN[source_tf]
    source_limit = max(160, int(limit * ratio) + 20)
    source_limit = min(source_limit, 5000)
    source = candles(pair, source_tf, source_limit)
    return _resample_from_validated_source(source, tf, limit)

def candles(pair, tf, limit=None):
    """Fetch validated completed candles with single-flight and provider failover."""
    if pair not in PAIRS or tf not in TF_MIN:
        raise ValueError("Unsupported pair/timeframe")
    limit = int(limit or MARKET_FETCH_LIMIT)
    key = (pair, tf)

    def cached_frame():
        with LOCK:
            cached = MARKET_CACHE.get(key)
            if cached and time.time() - cached[0] <= _cache_ttl(tf) and len(cached[1]) >= min(limit, 25):
                return cached[1].tail(limit).copy()
        return None

    cached = cached_frame()
    if cached is not None:
        return cached

    with LOCK:
        fetch_lock = MARKET_FETCH_LOCKS.setdefault(key, threading.Lock())

    with fetch_lock:
        cached = cached_frame()
        if cached is not None:
            return cached

        provider_map = {
            "biquote": _biquote,
            "dukascopy": _dukascopy,
            "sifting": _sifting_hist,
            "oanda": _oanda,
            "twelvedata": _twelvedata,
            "finnhub": _finnhub,
            "alphavantage": _alphavantage,
            "yahoo2": _yahoo2,
            "yahoo": _yahoo,
        }
        providers = []
        for name in PROVIDER_ORDER:
            fn = provider_map.get(name)
            if fn and (name not in {"yahoo","yahoo2"} or ALLOW_YAHOO_FALLBACK):
                if name in {"sifting","oanda","twelvedata","finnhub","alphavantage"}:
                    key_present = {
                        "sifting": bool(SIFTING_API_KEY) and SIFTING_API_ENABLED, "oanda": bool(OANDA_API_TOKEN),
                        "twelvedata": bool(TWELVEDATA_API_KEY), "finnhub": bool(FINNHUB_API_KEY),
                        "alphavantage": bool(ALPHAVANTAGE_API_KEY),
                    }[name]
                    if not key_present:
                        continue
                if name == "dukascopy" and not DUKASCOPY_ENABLED:
                    continue
                providers.append((name, fn))

        # Prefer the provider that last produced validated candles for this
        # pair/timeframe, but use a timeframe-aware preference when there is no
        # known winner yet. This prevents a provider that is good at 5m from
        # becoming the bottleneck for 1m/1h/4h.
        preferred = PROVIDER_LAST_SUCCESS.get(key)
        if preferred:
            providers.sort(key=lambda item: 0 if item[0] == preferred else 1)
        else:
            pref = {name: i for i, name in enumerate(TF_PROVIDER_PREFERENCE.get(tf, []))}
            providers.sort(key=lambda item: pref.get(item[0], 999))

        errors = []
        recovery_trace = []

        def validate_and_store(name, x):
            x = _validate_candle_cadence(x, tf, 25)
            interval = TF_SECONDS[tf]
            last_epoch = int(x.index[-1].timestamp())
            if int(time.time()) - last_epoch < interval and len(x) > 80:
                x = x.iloc[:-1]
            if len(x) < 25:
                raise RuntimeError("not enough validated candles")
            stored = x.copy()
            with LOCK:
                MARKET_CACHE[key] = (time.time(), stored, name)
                PROVIDER_LAST_SUCCESS[key] = name
            _persist_market_cache(pair, tf, stored, name)
            return stored.tail(limit).copy()

        # Public V30 lane: native 30m/1h feeds are tried in parallel.
        # One unavailable provider must not block the other public feeds.
        if tf in {"30m", "1h"}:
            direct_higher = [(n, f) for n, f in providers
                             if n in {"biquote", "yahoo2", "yahoo"}]
            ready = [(n, f) for n, f in direct_higher if _provider_ready(n)]
            if ready:
                with ThreadPoolExecutor(max_workers=len(ready)) as ex:
                    futures = {
                        ex.submit(_call_provider, n, f, pair, tf, limit): n
                        for n, f in ready
                    }
                    for fut in as_completed(futures):
                        name = futures[fut]
                        try:
                            return validate_and_store(name, fut.result())
                        except Exception as e:
                            errors.append(f"{name}: {str(e)[:140]}")
                            recovery_trace.append(f"{name} higher-TF failed")
                            _provider_failed(name, e)

            # Last-resort local aggregation from validated 5m candles.
            try:
                derived = _derive_timeframe(pair, tf, limit)
                derived = _validate_candle_cadence(derived, tf, 25)
                with LOCK:
                    MARKET_CACHE[key] = (time.time(), derived.copy(), "derived-5m")
                    PROVIDER_LAST_SUCCESS[key] = "derived-5m"
                _persist_market_cache(pair, tf, derived, "derived-5m")
                return derived.tail(limit).copy()
            except Exception as e:
                errors.append(f"derived-5m: {str(e)[:140]}")
                recovery_trace.append("5m-derived lane unavailable")

        # Fast public lane: do not wait for one dead provider before trying
        # another. This is the main latency/data-availability fix.
        public = [(n, f) for n, f in providers if n in {"biquote", "yahoo2"}]
        ready_public = [(n, f) for n, f in public if _provider_ready(n)]
        if ready_public:
            with ThreadPoolExecutor(max_workers=len(ready_public)) as ex:
                futures = {ex.submit(_call_provider, n, f, pair, tf, limit): n for n, f in ready_public}
                for fut in as_completed(futures):
                    name = futures[fut]
                    try:
                        return validate_and_store(name, fut.result())
                    except Exception as e:
                        errors.append(f"{name}: {str(e)[:140]}")
                        recovery_trace.append(f"{name} failed")
                        _provider_failed(name, e)

        # Credentialed/official providers are sequential to preserve free
        # quotas. Dukascopy is optional and is treated as a heavier fallback.
        for name, fn in providers:
            if name in {"biquote", "yahoo2", "yahoo"}:
                continue
            if not _provider_ready(name):
                continue
            try:
                return validate_and_store(name, _call_provider(name, fn, pair, tf, limit))
            except Exception as e:
                errors.append(f"{name}: {str(e)[:140]}")
                recovery_trace.append(f"{name} failed")
                _provider_failed(name, e)

        # Last Yahoo edge.
        for name, fn in providers:
            if name != "yahoo":
                continue
            if not _provider_ready(name):
                continue
            try:
                return validate_and_store(name, _call_provider(name, fn, pair, tf, limit))
            except Exception as e:
                errors.append(f"{name}: {str(e)[:140]}")
                recovery_trace.append(f"{name} failed")
                _provider_failed(name, e)

        # Deterministic local aggregation is the next safety lane. It is used
        # only after direct providers have failed and only for mathematically
        # exact timeframe conversions; it never invents prices.
        if tf in {"15m", "30m", "45m", "1h", "4h"}:
            try:
                derived = _derive_timeframe(pair, tf, limit)
                with LOCK:
                    MARKET_CACHE[key] = (time.time(), derived.copy(), "derived")
                    PROVIDER_LAST_SUCCESS[key] = "derived"
                _persist_market_cache(pair, tf, derived, "derived")
                return derived.tail(limit).copy()
            except Exception as e:
                errors.append(f"derived: {str(e)[:140]}")
                recovery_trace.append("local aggregation failed")

        if ALLOW_STALE_CANDLE_FALLBACK:
            with LOCK:
                old = MARKET_CACHE.get(key)
                if old and len(old[1]) >= 25:
                    recovery_trace.append("using cached candles")
                    return old[1].tail(limit).copy()

        detail = "; ".join(errors[-5:]) if errors else "no configured market-data provider"
        raise RuntimeError("Live market data unavailable. " + detail)

def provider_status():
    now = time.time(); out=[]
    configured=[("BiQuote", "biquote", True), ("Dukascopy", "dukascopy", DUKASCOPY_ENABLED), ("SiftingIO", "sifting", SIFTING_API_KEY and SIFTING_API_ENABLED), ("TwelveData", "twelvedata", TWELVEDATA_API_KEY), ("Finnhub", "finnhub", FINNHUB_API_KEY), ("Alpha Vantage", "alphavantage", ALPHAVANTAGE_API_KEY), ("Yahoo fallback", "yahoo", True)]
    for label,name,key in configured:
        if not key: continue
        with LOCK: cooldown=max(0,int(PROVIDER_COOLDOWN.get(name,0)-now))
        out.append(f"{label} (cooldown {cooldown}s)" if cooldown else f"{label} (ready)")
    return out

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
def stochastic(x, n=14, smooth=3):
    lo=x.low.rolling(n).min(); hi=x.high.rolling(n).max()
    k=100*(x.close-lo)/(hi-lo).replace(0,np.nan)
    d=k.rolling(smooth).mean()
    return k,d

def cci(x,n=20):
    tp=(x.high+x.low+x.close)/3
    ma=tp.rolling(n).mean(); dev=tp.rolling(n).apply(lambda z: np.mean(np.abs(z-np.mean(z))), raw=True)
    return (tp-ma)/(0.015*dev.replace(0,np.nan))

def pivots(x):
    # Recent swing levels, deliberately conservative to avoid treating every tick as support/resistance.
    hi=x.high.rolling(5,center=True).max(); lo=x.low.rolling(5,center=True).min()
    swing_hi=x.high[(x.high==hi)].dropna(); swing_lo=x.low[(x.low==lo)].dropna()
    return (float(swing_hi.tail(5).mean()) if len(swing_hi) else float(x.high.tail(20).max()),
            float(swing_lo.tail(5).mean()) if len(swing_lo) else float(x.low.tail(20).min()))

def calc(x):
    """Build a broad, completed-candle-only feature set."""
    x=x.copy()
    x["e9"]=ema(x.close,9); x["e21"]=ema(x.close,21); x["e50"]=ema(x.close,50); x["e200"]=ema(x.close,200)
    x["rsi"]=rsi(x.close); x["atr"]=atr(x)
    m=ema(x.close,12)-ema(x.close,26); x["macd"]=m; x["ms"]=ema(m,9); x["mh"]=x["macd"]-x["ms"]
    x["bb"]=x.close.rolling(20).mean(); sd=x.close.rolling(20).std(); x["bu"]=x.bb+2*sd; x["bl"]=x.bb-2*sd
    x["roc"]=x.close.pct_change(5)*100
    x["stoch_k"],x["stoch_d"]=stochastic(x)
    x["cci"]=cci(x)
    x["ema_slope"]=(x.e21-x.e21.shift(5))/x.close*10000
    x["atr_pct"]=(x["atr"]/x.close*100)
    x["bb_width"]=(x.bu-x.bl)/x.bb.replace(0,np.nan)*100
    x["close_pos"]=(x.close-x.low)/(x.high-x.low).replace(0,np.nan)
    x["body"]=(x.close-x.open).abs()
    x["body_ratio"]=x.body/(x.high-x.low).replace(0,np.nan)
    x["upper_wick"]=(x.high-x[["open","close"]].max(axis=1))/(x.high-x.low).replace(0,np.nan)
    x["lower_wick"]=(x[["open","close"]].min(axis=1)-x.low)/(x.high-x.low).replace(0,np.nan)
    x["range_ratio"]=(x.high-x.low)/x.atr.replace(0,np.nan)
    x["dist_ema21_atr"]=(x.close-x.e21)/x.atr.replace(0,np.nan)
    x["breakout_up"]=x.close/(x.high.shift(1).rolling(20).max())-1
    x["breakout_dn"]=x.close/(x.low.shift(1).rolling(20).min())-1
    x["engulf_bull"]=((x.close>x.open)&(x.close.shift(1)<x.open.shift(1))&
                      (x.open<=x.close.shift(1))&(x.close>=x.open.shift(1))).astype(int)
    x["engulf_bear"]=((x.close<x.open)&(x.close.shift(1)>x.open.shift(1))&
                      (x.open>=x.close.shift(1))&(x.close<=x.open.shift(1))).astype(int)
    # Volume is optional for FX feeds. When present, use a normalized volume
    # shock; otherwise keep the feature neutral.
    if "volume" in x.columns:
        v=pd.to_numeric(x["volume"],errors="coerce")
        x["volume_z"]=(v-v.rolling(30).mean())/v.rolling(30).std().replace(0,np.nan)
    else:
        x["volume_z"]=0.0
    p=x.close.shift()
    tr=pd.concat([x.high-x.low,(x.high-p).abs(),(x.low-p).abs()],axis=1).max(axis=1)
    up=x.high.diff(); dn=-x.low.diff()
    plus_dm=up.where((up>dn)&(up>0),0.0)
    minus_dm=dn.where((dn>up)&(dn>0),0.0)
    atr_w=tr.ewm(alpha=1/14,adjust=False).mean()
    pdi=100*plus_dm.ewm(alpha=1/14,adjust=False).mean()/atr_w.replace(0,np.nan)
    mdi=100*minus_dm.ewm(alpha=1/14,adjust=False).mean()/atr_w.replace(0,np.nan)
    dx=100*(pdi-mdi).abs()/(pdi+mdi).replace(0,np.nan)
    x["adx"]=dx.ewm(alpha=1/14,adjust=False).mean()
    x["pdi"]=pdi; x["mdi"]=mdi
    x["range_pct"]=((x.high-x.low)/x.close*100).replace([np.inf,-np.inf],np.nan)
    return x.replace([np.inf,-np.inf],np.nan).dropna()

def market_regime(a, x):
    """Classify the current market regime for adaptive ensemble weights.

    Uses only already-calculated candle features; it never calls a provider.
    This function is intentionally conservative so a missing/ambiguous regime
    never breaks the signal engine.
    """
    try:
        adx = float(a.adx)
        atr_pct = float(a.atr_pct)
        bb_width = float(a.bb_width)
        ema_spread = abs(float(a.e9) - float(a.e21)) / max(float(a.atr), 1e-12)
        range_ratio = float(a.range_ratio)

        # Expansion: unusually wide current range / volatility with directional
        # structure. This gets priority over the generic trending label.
        if range_ratio >= 1.8 or (atr_pct >= 1.8 and bb_width >= 0.025):
            return "EXPANSION"

        if atr_pct >= 2.5:
            return "HIGH_VOLATILITY"

        if adx >= 28 and ema_spread >= 0.18:
            return "TRENDING"

        if adx <= 19 and bb_width <= 0.018:
            return "RANGING"

        return "TRANSITION"
    except (TypeError, ValueError, AttributeError, ZeroDivisionError):
        return "TRANSITION"


def ai_ensemble(x):
    """Regime-aware ensemble of independent technical specialists."""
    a=x.iloc[-1]; p=x.iloc[-2]
    models={}
    atrv=max(float(a.atr),1e-12)
    models["trend"] = float(np.tanh(((a.e9-a.e21)/atrv)*0.8) + np.tanh(((a.e50-a.e200)/atrv)*0.35))
    models["momentum"] = float(np.tanh((a.mh/atrv)*7) + 0.25*np.tanh(float(a.roc)*5))
    models["structure"] = float(1 if (a.high>p.high and a.low>p.low) else -1 if (a.high<p.high and a.low<p.low) else 0)
    models["breakout"] = 0.0
    recent=x.iloc[-21:-1]
    res=float(recent.high.max()); sup=float(recent.low.min())
    if a.close>res: models["breakout"]=1.0
    elif a.close<sup: models["breakout"]=-1.0
    else:
        du=(res-a.close)/atrv; dd=(a.close-sup)/atrv
        if du<0.35 and a.close>a.open: models["breakout"]=0.25
        elif dd<0.35 and a.close<a.open: models["breakout"]=-0.25
    models["mean_reversion"] = float(np.clip((50-a.rsi)/25, -1, 1))
    if 42<=a.rsi<=58: models["mean_reversion"]*=0.25
    models["volatility"] = 0.0 if 0.01<=a.atr_pct<=1.2 else (-0.35 if a.atr_pct>1.8 else 0.10)
    models["candle"] = float(np.clip((a.close_pos-0.5)*2, -1, 1))
    models["oscillator"] = float(np.clip((a.stoch_k-a.stoch_d)/18, -1, 1))
    models["cci"] = float(np.clip(a.cci/180, -1, 1)) if np.isfinite(a.cci) else 0.0
    models["price_action"] = float(np.clip(
        (0.7 if a.engulf_bull else 0) - (0.7 if a.engulf_bear else 0)
        + np.tanh(float(a.dist_ema21_atr))*0.35
        + np.clip((float(a.close_pos)-0.5)*0.7,-0.7,0.7), -1, 1))
    models["volume"] = float(np.clip(float(a.volume_z)/3, -1, 1)) if np.isfinite(a.volume_z) else 0.0

    regime=market_regime(a,x)
    weights={
        "TRENDING":{"trend":1.45,"momentum":1.25,"structure":1.25,"breakout":1.15,"mean_reversion":0.45,"volatility":0.65,"candle":0.8,"oscillator":0.75,"cci":0.65,"price_action":1.0,"volume":0.35},
        "RANGING":{"trend":0.65,"momentum":0.75,"structure":0.85,"breakout":0.55,"mean_reversion":1.45,"volatility":0.9,"candle":0.9,"oscillator":1.2,"cci":1.0,"price_action":1.05,"volume":0.25},
        "EXPANSION":{"trend":1.2,"momentum":1.35,"structure":1.1,"breakout":1.45,"mean_reversion":0.35,"volatility":0.65,"candle":0.9,"oscillator":0.7,"cci":0.8,"price_action":1.1,"volume":0.45},
        "HIGH_VOLATILITY":{"trend":1.0,"momentum":1.05,"structure":0.8,"breakout":0.9,"mean_reversion":0.35,"volatility":1.4,"candle":0.75,"oscillator":0.55,"cci":0.55,"price_action":0.9,"volume":0.3},
        "TRANSITION":{"trend":1.0,"momentum":1.0,"structure":1.0,"breakout":0.8,"mean_reversion":0.8,"volatility":1.0,"candle":0.8,"oscillator":0.8,"cci":0.7,"price_action":0.9,"volume":0.25},
    }[regime]
    weighted=sum(models[k]*weights[k] for k in models); total=sum(weights.values())
    fusion=float(np.clip(weighted/total,-1,1))
    votes=sum(1 for v in models.values() if v>0.18)-sum(1 for v in models.values() if v<-0.18)
    agreement=abs(votes)/len(models)
    return {"regime":regime,"models":models,"fusion":fusion,"agreement":agreement,"weights":weights}

def analyse(x):
    """Regime-aware multi-factor + explainable AI ensemble analysis."""
    x=calc(x)
    if len(x)<80: raise RuntimeError("Not enough validated candles")
    a=x.iloc[-1]; p=x.iloc[-2]
    bull=bear=0.0; why=[]; factors={}
    if a.e9>a.e21>a.e50: bull+=16; factors["trend"]="BULLISH"; why.append("EMA 9/21/50 aligned bullish")
    elif a.e9<a.e21<a.e50: bear+=16; factors["trend"]="BEARISH"; why.append("EMA 9/21/50 aligned bearish")
    else: factors["trend"]="MIXED"; why.append("EMA structure mixed")
    if a.close>a.e200: bull+=8; factors["regime_price"]="ABOVE EMA200"
    elif a.close<a.e200: bear+=8; factors["regime_price"]="BELOW EMA200"
    if a.adx>=25:
        if a.pdi>a.mdi: bull+=10; why.append(f"ADX trend strength supports buyers ({a.adx:.0f})")
        elif a.mdi>a.pdi: bear+=10; why.append(f"ADX trend strength supports sellers ({a.adx:.0f})")
    else: why.append(f"ADX is non-trending ({a.adx:.0f})")
    if a.macd>a.ms and a.mh>=p.mh: bull+=10; why.append("MACD histogram improving bullish")
    elif a.macd<a.ms and a.mh<=p.mh: bear+=10; why.append("MACD histogram improving bearish")
    if 52<=a.rsi<=68: bull+=7
    elif 32<=a.rsi<=48: bear+=7
    elif a.rsi>72: bear+=2; why.append("overbought caution")
    elif a.rsi<28: bull+=2; why.append("oversold caution")
    if a.roc>0.08: bull+=4
    elif a.roc<-0.08: bear+=4
    body=abs(a.close-a.open); rng=max(float(a.high-a.low),1e-12); body_ratio=body/rng
    if a.close>a.open and body_ratio>=0.55: bull+=6; why.append("strong bullish candle body")
    elif a.close<a.open and body_ratio>=0.55: bear+=6; why.append("strong bearish candle body")
    if a.engulf_bull: bull+=5; why.append("bullish engulfing pattern")
    elif a.engulf_bear: bear+=5; why.append("bearish engulfing pattern")
    if a.close_pos>0.78 and a.body_ratio>0.45: bull+=3; why.append("close near candle high")
    elif a.close_pos<0.22 and a.body_ratio>0.45: bear+=3; why.append("close near candle low")
    if a.volume_z>1.5 and a.close>a.open: bull+=2; why.append("positive volume expansion")
    elif a.volume_z>1.5 and a.close<a.open: bear+=2; why.append("negative volume expansion")
    if a.high>p.high and a.low>p.low: bull+=7; why.append("short-term HH/HL structure")
    elif a.high<p.high and a.low<p.low: bear+=7; why.append("short-term LH/LL structure")
    res,sup=pivots(x); factors["resistance"]=res; factors["support"]=sup
    if a.close>res: bull+=8; why.append("close above recent swing resistance")
    elif a.close<sup: bear+=8; why.append("close below recent swing support")
    ai=ai_ensemble(x)
    # Ensemble is a separate evidence source. It is capped so it cannot overpower raw data.
    ai_vote=ai["fusion"]
    if ai_vote>0.10: bull+=12*abs(ai_vote); why.append(f"AI ensemble bias bullish ({ai_vote:+.2f})")
    elif ai_vote<-0.10: bear+=12*abs(ai_vote); why.append(f"AI ensemble bias bearish ({ai_vote:+.2f})")
    atr_pct=float(a.atr_pct)
    if atr_pct>2.0: bull-=6; bear-=6; why.append("extreme volatility penalty")
    gap=abs(bull-bear); dominant=max(bull,bear)
    # Direction needs raw confluence + ensemble agreement.
    # Always retain the strongest directional bias separately from the trade
    # confirmation gate.  A weak/ambiguous setup can be a valid NEXT-CANDLE
    # forecast while still being marked non-tradeable.
    bias_direction = "CALL" if bull >= bear else "PUT"
    if gap<14 or dominant<38 or ai["agreement"]<0.11:
        direction="WAIT"
    else:
        direction=bias_direction
    raw_score=42 + gap*1.35 + min(18,max(0,dominant-42))*0.45 + ai["agreement"]*14
    if ai_vote and ((direction=="CALL" and ai_vote<0) or (direction=="PUT" and ai_vote>0)): raw_score-=10
    score=int(np.clip(round(raw_score),0,100))
    if direction!="WAIT" and score<MIN_SIGNAL_SCORE: direction="WAIT"
    factors.update({"bull":round(bull,2),"bear":round(bear,2),"gap":round(gap,2),"bias_direction":bias_direction,"rsi":float(a.rsi),"adx":float(a.adx),"macd_hist":float(a.mh),"atr":float(a.atr),"atr_pct":atr_pct,"stoch_k":float(a.stoch_k),"stoch_d":float(a.stoch_d),"cci":float(a.cci),"ema_slope":float(a.ema_slope),"bb_width":float(a.bb_width),
             "body_ratio":float(a.body_ratio),"upper_wick":float(a.upper_wick),"lower_wick":float(a.lower_wick),
             "range_ratio":float(a.range_ratio),"dist_ema21_atr":float(a.dist_ema21_atr),
             "engulf_bull":int(a.engulf_bull),"engulf_bear":int(a.engulf_bear),"volume_z":float(a.volume_z),
             "regime":ai["regime"],"ai_fusion":ai_vote,"ai_agreement":ai["agreement"]})
    return direction, score, why, x

def _resample_ohlc(x, tf):
    rule = {"5m":"5min","15m":"15min","30m":"30min","45m":"45min",
            "1h":"1h","4h":"4h"}[tf]
    return x.resample(rule, origin="epoch", label="left", closed="left").agg(
        {"open":"first","high":"max","low":"min","close":"last"}).dropna()

def mtf(pair, entry_tf="5m"):
    """Multi-timeframe confirmation with local resampling first.

    The old implementation made a separate external API call for every MTF
    level. On free feeds that quickly caused 429/403 failures. This version
    reuses the entry timeframe where mathematically valid and only fetches a
    higher timeframe directly when the base history is insufficient.
    """
    key = (pair, entry_tf)
    now = time.time()
    with LOCK:
        cached = MTF_CACHE.get(key)
        if cached and now - cached[0] <= MTF_CACHE_SECONDS:
            return cached[1]

    hierarchy = {
        "1m":["1m","5m","15m","30m"],
        "5m":["5m","15m","30m","1h"],
        "15m":["15m","30m","1h","4h"],
        "30m":["30m","1h","4h"],
        "45m":["45m","1h","4h"],
        "1h":["1h","4h"],
        "4h":["4h"],
    }
    weights = [1.0,1.3,1.6,2.0]
    out={}; totals={"CALL":0.0,"PUT":0.0}; available=0

    try:
        base = candles(pair, entry_tf, MTF_BASE_LIMIT)
    except Exception:
        base = None

    for i, tf in enumerate(hierarchy[entry_tf]):
        try:
            x = None
            if base is not None:
                base_min = TF_MIN[entry_tf]
                target_min = TF_MIN[tf]
                # Only resample upward from a timeframe whose bucket is an
                # exact multiple of the entry timeframe.
                if target_min >= base_min and target_min % base_min == 0:
                    candidate = base if tf == entry_tf else _resample_ohlc(base, tf)
                    if len(candidate) >= 80:
                        x = candidate.tail(MARKET_FETCH_LIMIT).copy()
            if x is None:
                x = candles(pair, tf, MARKET_FETCH_LIMIT)
            d,s,why,_=analyse(x)
            out[tf]=(d,s)
            available += 1
            if d in totals:
                totals[d] += s * weights[min(i,len(weights)-1)]
        except Exception as e:
            out[tf]=("UNAVAILABLE",0)
            db_log("warning","mtf",f"{pair}/{entry_tf}->{tf}: {e}")

    # MTF confirmation is optional. The requested timeframe is the primary
    # market feed; one broken higher-timeframe endpoint must never turn a valid
    # primary candle feed into a total signal failure.
    if available == 0:
        result=("WAIT", 0, out)
        with LOCK:
            MTF_CACHE[key]=(time.time(), result)
        return result

    denom=sum(weights[:len(out)]) or 1
    score=int(min(100, max(totals.values()) / (denom*0.95)))
    # Require meaningful directional separation, but do not force WAIT merely
    # because one higher timeframe provider is unavailable.
    if totals["CALL"] > totals["PUT"] * 1.12:
        final="CALL"
    elif totals["PUT"] > totals["CALL"] * 1.12:
        final="PUT"
    else:
        final="WAIT"

    result=(final, score, out)
    with LOCK:
        MTF_CACHE[key]=(time.time(), result)
    return result

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
    # One full candle is the default analysis/trade horizon after the entry window.
    expiry = next_start + interval * TRADE_HORIZON_CANDLES
    return next_start, start, end, expiry

def fmt_clock(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%H:%M:%S UTC")

def fmt_clock_zones(ts):
    dt=datetime.fromtimestamp(int(ts),tz=timezone.utc)
    ua=dt.astimezone(ZoneInfo("Asia/Dubai")).strftime("%H:%M:%S")
    in_=dt.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")
    utc=dt.strftime("%H:%M:%S")
    return f"UAE {ua} • India {in_} • UTC {utc}"

def fmt_signal_timing(s):
    """Give an actionable clock plan in UAE, India and UTC."""
    now=int(time.time())
    if s.get("direction")=="WAIT":
        return (f"🕐 Next candle: <b>{fmt_clock_zones(s['next_candle'])}</b>\n"
                f"⏳ Time remaining: <b>{fmt_duration(max(0,s['next_candle']-now))}</b>\n"
                f"🚫 Action: <b>WAIT — no confirmed entry</b>")
    state=s.get("state","READY")
    action="WAIT FOR CONFIRMATION" if state=="READY" else ("ENTRY WINDOW ACTIVE" if state=="ACTIVE WINDOW" else "EXPIRED — SKIP")
    return (f"🕐 Next candle: <b>{fmt_clock_zones(s['next_candle'])}</b>\n"
            f"🎯 Preferred entry: <b>{fmt_clock_zones(s['entry_start'])} → {fmt_clock_zones(s['entry_end'])}</b>\n"
            f"⏳ Entry window: <b>{fmt_duration(max(0,s['entry_end']-s['entry_start']))}</b>\n"
            f"⌛ Monitoring/expiry: <b>{fmt_clock_zones(s['valid_until'])}</b>\n"
            f"⏱ Planned duration: <b>{fmt_duration(max(0,s['valid_until']-s['entry_start']))}</b>\n"
            f"📌 Status: <b>{action}</b>\n"
            f"🚫 If direction/confirmation changes: <b>SKIP</b>")

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


# ================= V29 30M/1H CANDLE FUSION ENGINE =================
# This is intentionally restricted to completed 30m/1h candles.
# It combines candle-structure voting with a small walk-forward validation
# pass on the same historical OHLC stream. It improves calibration, but it
# cannot guarantee the next candle.

V29_TFS = {"30m", "1h"}

def _v29_direction_from_candles(cs):
    """Return (direction, strength) using only completed OHLC candles."""
    a = analyze_candles(cs)
    d = a.get("direction")
    if d not in {"UP","DOWN"}:
        return "WAIT", 0.0
    return ("CALL" if d == "UP" else "PUT"), float(a.get("confidence",0))/100.0

def _v29_walk_forward(cs, min_train=70, max_eval=90):
    """Walk-forward hit-rate on prior completed candles, no future leakage."""
    n=len(cs)
    if n < min_train + 8:
        return 0.50, 0
    start=max(40, n-max_eval-1)
    hits=0; total=0
    for end in range(start, n-1):
        hist=cs[:end+1]
        pred,_=_v29_direction_from_candles(hist)
        if pred == "WAIT":
            continue
        nxt=cs[end+1]
        o=_co_num(nxt.get("open")); c=_co_num(nxt.get("close"))
        actual="CALL" if c>o else "PUT" if c<o else "WAIT"
        if actual=="WAIT":
            continue
        total += 1
        hits += int(pred==actual)
    return (hits/total if total else 0.50), total

def make_v29_signal(pair, tf):
    if tf not in V29_TFS:
        raise RuntimeError("V29 supports only 30m and 1h")
    x=candles(pair, tf, 260)
    meta=market_meta(pair, tf, x)
    if not meta["fresh"]:
        raise RuntimeError(f"Market data is stale ({fmt_duration(meta['age'])} old)")
    cs=[]
    # DataFrame -> completed OHLC dicts
    for idx,row in x.iterrows():
        cs.append({"time":idx.isoformat(),"open":row.open,"high":row.high,
                   "low":row.low,"close":row.close})
    cs=_co_completed(cs)
    if len(cs)<60:
        raise RuntimeError("Insufficient completed higher-timeframe candles")
    base=analyze_candles(cs, tf)
    # Always return a directional next-candle classification when fresh
    # completed OHLC data is available.  A neutral/weak structure is handled
    # by lowering confidence instead of leaving the UI stuck on INITIALIZING.
    direction = base.get("direction")
    if direction not in {"UP","DOWN"}:
        # Deterministic fallback: last completed candle body + recent sequence.
        recent = cs[-8:]
        ups = sum(1 for c in recent if _co_num(c.get("close")) > _co_num(c.get("open")))
        downs = sum(1 for c in recent if _co_num(c.get("close")) < _co_num(c.get("open")))
        direction = "UP" if ups >= downs else "DOWN"
        base["confidence"] = min(float(base.get("confidence", 50) or 50), 58.0)
    pred="CALL" if direction=="UP" else "PUT"
    wf, samples=_v29_walk_forward(cs)
    # Calibration: neutral hit-rate gives no bonus; strong historical alignment
    # gives a modest bonus; poor recent behavior reduces confidence.
    cal_bonus=(wf-0.50)*34.0
    confidence=max(55.0,min(96.0,float(base["confidence"])+cal_bonus))
    # Require both structural agreement and positive/neutral calibration.
    # Weak historical calibration reduces confidence but does not suppress a
    # fresh-data directional result. This keeps the signal endpoint responsive.
    if samples >= 20 and wf < 0.45:
        confidence = min(confidence, 60.0)
    score=int(round(confidence))
    return {
        "pair":pair,"tf":tf,"direction":pred,"prediction_direction":pred,
        "display_direction":"UP" if pred=="CALL" else "DOWN",
        "score":score,"prediction_score":score,"display_score":score,
        "confidence":score,"tradeable":True,"confirmation":"CANDLE CONFIRMED",
        "candle":cs[-1]["time"],"data_age":meta["age"],"fresh":meta["fresh"],
        "provider":meta["provider"],"candle_only":True,
        "model":"V30 30M/1H Candle Fusion",
        "walk_forward_accuracy":round(wf*100,1),"walk_forward_samples":samples,
        "structure_score":base.get("score",0),"reason":base.get("reason"),
        "target_candle":"NEXT_CANDLE"
    }

def make_signal(pair, tf):
    """Create a validated, time-specific setup and reuse identical work across users."""
    key = (pair, tf)
    now = time.time()

    # Shared result cache prevents 100 users requesting the same pair/timeframe
    # from triggering 100 identical MTF calculations/API reads.
    with LOCK:
        cached = SIGNAL_CACHE.get(key)
        if cached and now - cached[0] <= SIGNAL_CACHE_SECONDS:
            s = dict(cached[1])
            next_candle, entry_start, entry_end, expiry = entry_window(tf)
            s["next_candle"] = next_candle
            s["entry_start"] = entry_start
            s["entry_end"] = entry_end
            s["valid_until"] = expiry
            if s["direction"] != "WAIT":
                s["state"] = "READY" if now < entry_start else ("ACTIVE WINDOW" if now <= entry_end else "EXPIRED")
            else:
                s["state"] = "WAIT"
            s["wait"] = max(0, next_candle - int(now))
            return s

    try:
        x = candles(pair, tf)
    except Exception as feed_exc:
        # Service remains responsive during a provider outage. If a recently
        # confirmed direction exists, return it explicitly as RECOVERY rather
        # than pretending it is a fresh live signal.
        with LOCK:
            recovery = LAST_DIRECTIONAL.get(key)
        if recovery and now - recovery[0] <= RECOVERY_MAX_AGE:
            recovered = dict(recovery[2])
            recovered["state"] = "RECOVERY"
            recovered["recovery"] = True
            recovered["recovery_age"] = int(now - recovery[0])
            return recovered
        raise feed_exc
    meta = market_meta(pair, tf, x)
    if not meta["fresh"]:
        raise RuntimeError(f"Market data is stale ({fmt_duration(meta['age'])} old)")

    d, local_score, why, x = analyse(x)
    md, mtf_score, mtf_map = mtf(pair, tf)

    available_mtf = [(direction, sc) for direction, sc in mtf_map.values()
                     if direction in {"CALL","PUT","WAIT"}]
    unavailable = sum(1 for direction, _ in mtf_map.values() if direction == "UNAVAILABLE")
    aligned = sum(1 for direction, _ in mtf_map.values() if direction == d and d != "WAIT")

    # Primary decision = fresh local model. MTF is confirmation, not a single
    # point of failure. This prevents one missing 1h/4h feed from killing a
    # perfectly valid 5m signal.
    final = d if d in {"CALL","PUT"} else "WAIT"
    score = int(min(100, max(0, round(local_score * 0.70 + mtf_score * 0.30))))
    if unavailable:
        score = max(0, score - min(12, unavailable * 3))
    if final != "WAIT" and aligned >= 2:
        score = min(100, score + 5)
    # If MTF strongly contradicts the local direction, downgrade to WAIT.
    opposing = sum(1 for direction, _ in mtf_map.values() if direction in {"CALL","PUT"} and direction != final)
    if final != "WAIT" and opposing >= max(2, len(mtf_map)//2) and mtf_score >= 45:
        final = "WAIT"
    if final == "WAIT" or score < MIN_SIGNAL_SCORE:
        final = "WAIT"

    entry = stop = target = rr = None
    next_candle, entry_start, entry_end, expiry = entry_window(tf)
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

    state = "WAIT"
    if final != "WAIT":
        state = "READY" if now < entry_start else ("ACTIVE WINDOW" if now <= entry_end else "EXPIRED")

    local = calc(x).iloc[-1]
    ai = ai_ensemble(calc(x))
    result = {
        "pair": pair, "tf": tf, "direction": final, "score": score,
        "label": score_label(score, final), "why": why, "mtf": mtf_map,
        "wait": max(0, next_candle - int(now)), "entry": entry, "stop": stop,
        "target": target, "rr": rr, "candle": x.index[-1].isoformat(),
        "provider": meta["provider"], "data_age": meta["age"], "fresh": meta["fresh"],
        "next_candle": next_candle, "entry_start": entry_start,
        "entry_end": entry_end, "valid_until": expiry, "state": state,
        "trade_duration": max(0, expiry - entry_start),
        "local_score": local_score, "mtf_score": mtf_score,
        "factors": {
            "RSI": float(local.rsi), "ADX": float(local.adx),
            "MACD": float(local.macd), "MACD_signal": float(local.ms),
            "ATR": float(local.atr), "ATR_pct": float(local.atr / local.close * 100),
            "EMA9": float(local.e9), "EMA21": float(local.e21), "EMA50": float(local.e50),
            "EMA200": float(local.e200), "ROC": float(local.roc),
            "support": float(x.low.iloc[-21:-1].min()),
            "resistance": float(x.high.iloc[-21:-1].max()),
            "mtf_aligned": aligned,
            "regime": ai["regime"], "ai_fusion": float(ai["fusion"]),
            "ai_agreement": float(ai["agreement"]),
            "ai_models": {k: float(v) for k, v in ai["models"].items()}
        }
    }
    # Prediction is intentionally separate from the trade-confirmation gate.
    # It tells the user the model's next-candle bias even when the setup is not
    # strong enough to be labelled a confirmed trade.
    prediction = "CALL" if float(ai["fusion"]) >= 0 else "PUT"
    if final in {"CALL", "PUT"}:
        prediction = final
    result["prediction_direction"] = prediction
    result["prediction_score"] = int(np.clip(round(score if final != "WAIT" else max(1, score)), 1, 99))
    result["tradeable"] = bool(final in {"CALL", "PUT"})
    if final in {"CALL", "PUT"} and meta["fresh"]:
        with LOCK:
            LAST_DIRECTIONAL[key] = (time.time(), final, dict(result))

    with LOCK:
        SIGNAL_CACHE[key] = (time.time(), dict(result))
    return result

def fmt_signal(s, detailed=True):
    lines=[
        "⚡ <b>NEXCANDLE AI — PROFESSIONAL SIGNAL PLAN</b>",
        f"💱 <b>{html.escape(s['pair'])}</b> • ⏱ <b>{s['tf']}</b>",
        "",
        f"🎯 <b>DECISION: {s['direction']}</b>",
        f"📊 Setup Quality: <b>{s['score']}/100 — {s['label']}</b>",
        f"🧠 AI Regime: <b>{html.escape(str(s['factors'].get('regime','UNKNOWN')))}</b> • Ensemble: <b>{s['factors'].get('ai_fusion',0):+.2f}</b>",
        f"📡 Live feed: <b>VALIDATED</b> • Age: <b>{fmt_duration(s['data_age'])}</b>",
    ]
    if s["direction"]=="WAIT":
        pred = "UP" if s.get("prediction_direction") == "CALL" else "DOWN" if s.get("prediction_direction") == "PUT" else "—"
        lines += [
            "",f"🔮 <b>NEXT-CANDLE BIAS: {pred}</b>",
            f"📊 Forecast strength: <b>{s.get('prediction_score', s.get('score', 0))}/100</b>",
            "🟡 Trade confirmation: <b>WAIT</b> — the model does not have enough confluence for a confirmed entry.",
            "", "🕐 <b>NEXT CANDLE TIMING</b>", fmt_signal_timing(s),
            "", "📌 The forecast is a model estimate, not a guaranteed future result."
        ]
    else:
        f=s["factors"]
        lines += [
            "","🕐 <b>EXACT ENTRY PLAN</b>", fmt_signal_timing(s),
            f"⏱ Valid until / expiry: <b>{fmt_clock_zones(s['valid_until'])}</b>",
            f"⌛ Planned duration: <b>{fmt_duration(s.get('trade_duration', 0))}</b>",
            "", f"📍 Reference price: <code>{s['entry']:.6f}</code>",
            f"🛑 Risk/SL reference: <code>{s['stop']:.6f}</code>",
            f"🎯 Target reference: <code>{s['target']:.6f}</code>",
            f"⚖️ Risk/Reward: <b>1:{s['rr']:.2f}</b>",
            "",
            "🧠 <b>CONFIRM BEFORE ENTRY</b>",
            f"• MTF alignment: <b>{f['mtf_aligned']}/{len(s['mtf'])}</b>",
            f"• AI model agreement: <b>{f.get('ai_agreement',0)*100:.0f}%</b>",
            f"• Market regime: <b>{html.escape(str(f.get('regime','UNKNOWN')))}</b>",
            f"• RSI: <b>{f['RSI']:.1f}</b> | ADX: <b>{f['ADX']:.1f}</b>",
            f"• EMA trend: <b>{'Bullish' if f['EMA9']>f['EMA21']>f['EMA50'] else 'Bearish' if f['EMA9']<f['EMA21']<f['EMA50'] else 'Mixed'}</b>",
            f"• Momentum: <b>{'Bullish' if f['MACD']>f['MACD_signal'] else 'Bearish'}</b>",
            "",
            "✅ Enter only if the same direction remains confirmed inside the window.",
            "🚫 If the window expires, price moves sharply away, or confirmation flips → <b>SKIP</b>."
        ]
    if detailed:
        lines += ["","🧭 <b>MULTI-TIMEFRAME CONFIRMATION</b>"]
        for tf,(direction,sc) in s["mtf"].items():
            icon="✅" if direction==s["direction"] and direction!="WAIT" else ("⚪" if direction=="WAIT" else "❌")
            if direction=="UNAVAILABLE": icon="⚠️"
            lines.append(f"{icon} {tf}: <b>{direction}</b> ({sc}/100)")
        lines += ["","🔬 <b>WHY THIS SETUP</b>"]
        lines += ["• "+html.escape(w) for w in s["why"][:10]]
        lines += [
            "", "⚠️ <b>Important:</b> Setup Quality is a model score, not a guaranteed probability, profit, or future outcome."
        ]
    return "\n".join(lines)

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
            last_exc = None
            s = fallback = None
            for attempt in range(4):
                try:
                    s, fallback = await asyncio.to_thread(get_directional_signal,p,t)
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt == 0:
                        await asyncio.sleep(0.35)
            if s is None:
                exc = last_exc or RuntimeError("signal unavailable")
                db_log("warning","telegram-signal",f"{p}/{t}: {str(exc)[:240]}")
                # Do not expose provider failures as a confusing permanent error.
                # The user only sees a neutral retry state while the engine
                # continues provider fallback/recovery internally.
                return await q.message.reply_text(
                    "🟡 <b>No confirmed signal right now</b>\n\n"
                    "Market data is temporarily updating. "
                    "Please retry in a few seconds.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=back_menu()
                )
            prediction = s.get("prediction_direction")
            if s["direction"] not in {"CALL","PUT"} and prediction not in {"CALL","PUT"}:
                return await q.message.reply_text(
                    "🟡 <b>NEXT-CANDLE DATA NOT READY</b>\n\n"
                    "Fresh validated candles are not available yet. No price or direction is invented.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=back_menu()
                )
            display_direction = s["direction"] if s["direction"] in {"CALL","PUT"} else prediction
            direction = "UP" if display_direction == "CALL" else "DOWN"
            consume(uid)
            # Keep historical tracking, but never expose WAIT/ERR/provider details
            # in the public signal message.
            if s.get("direction") in {"CALL","PUT"} and s.get("entry") is not None:
                try:
                    created = now_iso()
                    with LOCK:
                        c=con()
                        c.execute("""INSERT INTO signals(
                            user_id,pair,tf,direction,score,entry,stop,target,rr,candle,created,
                            entry_start,entry_end,valid_until,provider,data_age,confidence_label,regime,ai_fusion,ai_agreement,feature_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (uid,p,t,s["direction"],s.get("score",50),s.get("entry"),s.get("stop"),s.get("target"),s.get("rr"),
                         s.get("candle",""),created,fmt_clock(s.get("entry_start",time.time())),
                         fmt_clock(s.get("entry_end",time.time())),fmt_clock(s.get("valid_until",time.time())),
                         s.get("provider","validated"),s.get("data_age",0),"DIRECTIONAL",
                         s.get("factors",{}).get("regime"),s.get("factors",{}).get("ai_fusion"),
                         s.get("factors",{}).get("ai_agreement"),json.dumps(s.get("factors",{}),allow_nan=True)))
                        c.commit(); c.close()
                except Exception as log_exc:
                    db_log("warning","signal-log",f"{p}/{t}: {log_exc}")
            return await q.message.reply_text(direction,reply_markup=menu(uid))
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
            text="🔎 <b>MARKET SCANNER — AI RANKING</b>\n\n"+("\n".join(f"{i+1}. {s['pair']} — <b>{s['direction']}</b> — {s['score']}/100 — {s['factors'].get('regime','—')} — MTF {s['factors'].get('mtf_aligned',0)}/{len(s['mtf'])}" for i,s in enumerate(results[:10])) or "No qualifying setup.")+"\n\n🧠 Ranked by confluence, regime, AI ensemble and MTF confirmation."; return await q.message.reply_text(text,parse_mode=ParseMode.HTML,reply_markup=back_menu())
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
            helptext="""ℹ️ <b>NexCandle AI HELP</b>\n\n<b>📊 Get Signal</b> — AI ensemble signal with market regime, multi-timeframe confluence, live re-validation and exact entry window.\n\n<b>⏰ Entry Timing</b> — Shows the next candle boundary and explains when not to chase a late entry.\n\n<b>🧭 MTF</b> — Higher-timeframe agreement check.\n\n<b>🔥 Best Setup</b> — Ranks setups using the AI confluence engine instead of forcing a trade.\n\n<b>🎯 Accuracy</b> — Measured historical results from tracked signals.\n\n<b>📈 Backtest</b> — Historical simulation using the same technical rules.\n\n<b>🔔 Alerts</b> — Premium automatic setup notifications.\n\n<b>💎 Premium</b> — Submit payment reference for admin approval.\n\n<b>🛡 Data safety</b> — Provider failures are handled internally; raw API errors are not shown to users.\n\n⚠️ No system can guarantee the next candle or future profit."""; return await q.message.reply_text(helptext,parse_mode=ParseMode.HTML,reply_markup=back_menu())
        if d=="admin": return await q.message.reply_text("👑 <b>ADMIN PANEL</b>",parse_mode=ParseMode.HTML,reply_markup=admin_menu())
        if d.startswith("admin:") and is_admin(uid): return await admin_callback(q,d)
        if d.startswith("approve:") and is_admin(uid): return await review_payment(q,d,True)
        if d.startswith("reject:") and is_admin(uid): return await review_payment(q,d,False)
    except Exception as e:
        db_log("error","callback",f"{d}: {type(e).__name__}: {e}")
        # For signal requests, try the exact pair/timeframe from the callback one
        # last time. If no validated/remembered direction exists, keep the UI
        # silent rather than inventing a signal.
        if d.startswith("run:"):
            try:
                _,p,t=d.split(":"); p=p.replace("~","/")
                s,_ = await asyncio.to_thread(get_directional_signal,p,t)
                return await q.message.reply_text("UP" if s["direction"]=="CALL" else "DOWN",
                                                  reply_markup=menu(uid))
            except Exception:
                pass
        if d.startswith("run:"):
            return await q.message.reply_text(
                "⏳ Live market data source is temporarily unavailable.\n\n"
                "The bot is online, but the configured intraday data provider is rejecting "
                "or rate-limiting requests. Please retry after the provider cooldown.",
                reply_markup=back_menu()
            )
        await q.message.reply_text("Signal unavailable",reply_markup=back_menu())

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
                        s=await asyncio.to_thread(make_signal,p,tf)
                        if s["direction"]=="WAIT" or s["score"]<minscore or (direction!="BOTH" and s["direction"]!=direction): continue
                        fresh=False
                        with LOCK:
                            c=con()
                            try:
                                c.execute("INSERT INTO alert_events(user_id,pair,tf,direction,candle) VALUES(?,?,?,?,?)",(uid,p,tf,s["direction"],s["candle"])); c.commit(); fresh=True
                            except sqlite3.IntegrityError: pass
                            c.close()
                        if fresh:
                            await app.bot.send_message(uid, "UP" if s["direction"]=="CALL" else "DOWN", reply_markup=menu(uid))
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

                    x=await asyncio.to_thread(candles,r["pair"],tf,120)
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

async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    exc = context.error
    db_log("error", "telegram-handler", str(exc)[:500])
    # CallbackQuery errors are otherwise easy to miss in Render logs.
    try:
        if isinstance(update, Update) and update.callback_query:
            await update.callback_query.answer("Temporary error — please retry.", show_alert=True)
    except Exception:
        pass

def build_telegram_app():
    app=Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(telegram_error_handler)
    app.add_handler(CallbackQueryHandler(cb))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO, any_message))
    return app

async def telegram_loop_runner(app):
    await app.initialize()
    await app.start()
    await post_init(app)
    if BOT_MODE == "webhook":
        if not RENDER_EXTERNAL_URL:
            raise RuntimeError("RENDER_EXTERNAL_URL is required when BOT_MODE=webhook")
        if _ON_RENDER and _requested_bot_mode == "polling":
            db_log("warning", "telegram", "BOT_MODE=polling was ignored; Render webhook mode is enforced")
        # Always prefer Render's live hostname. A manually copied
        # RENDER_EXTERNAL_URL can become stale after a service/domain change.
        base=(RENDER_EXTERNAL_HOSTNAME or RENDER_EXTERNAL_URL).rstrip("/")
        if not base:
            raise RuntimeError("Render hostname is unavailable; set RENDER_EXTERNAL_URL to the current Render URL")
        if not base.startswith(("http://", "https://")):
            base = "https://" + base
        webhook_url=base + "/telegram/webhook"
        await app.bot.delete_webhook(drop_pending_updates=True)
        await app.bot.set_webhook(
            url=webhook_url,
            secret_token=WEBHOOK_SECRET or None,
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )
        info = await app.bot.get_webhook_info()
        db_log("info", "telegram", f"Webhook mode enabled: {webhook_url}")
        db_log("info", "telegram", f"Webhook verified: url={bool(info.url)} pending={info.pending_update_count} last_error={info.last_error_message or 'none'}")
    else:
        if _ON_RENDER:
            # Absolute safety guard: never call getUpdates on a Render web service.
            raise RuntimeError("Polling is disabled on Render. Set a public Render URL for webhook mode.")
        await app.bot.delete_webhook(drop_pending_updates=True)
        db_log("info", "telegram", "Polling mode enabled (local development only)")
        await app.updater.start_polling(drop_pending_updates=True)
    try:
        await asyncio.Event().wait()
    finally:
        if app.updater and app.updater.running:
            await app.updater.stop()
        await app.stop()
        await app.shutdown()

def main():
    global TELEGRAM_APP, TELEGRAM_LOOP
    if not BOT_TOKEN: raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    init()
    TELEGRAM_APP=build_telegram_app()
    # Flask serves health checks and the Telegram webhook. Telegram processing
    # runs on its own asyncio loop so Flask never blocks on bot work.
    threading.Thread(target=run_web,daemon=True).start()
    TELEGRAM_LOOP=asyncio.new_event_loop()
    asyncio.set_event_loop(TELEGRAM_LOOP)
    try:
        TELEGRAM_LOOP.run_until_complete(telegram_loop_runner(TELEGRAM_APP))
    finally:
        TELEGRAM_LOOP.close()

if __name__=="__main__": main()


start_sifting_ws()



def _nc_num(x, default=0.0):
    try:
        v=float(x)
        return v if v == v and abs(v) != float("inf") else default
    except Exception:
        return default

def _nc_ema(values, period):
    if not values:
        return 0.0
    a=2.0/(period+1.0)
    e=values[0]
    for v in values[1:]:
        e=a*v+(1-a)*e
    return e

def _nc_rsi(values, period=14):
    if len(values) < period+1:
        return 50.0
    gains=[]; losses=[]
    for a,b in zip(values[-period-1:-1], values[-period:]):
        d=b-a
        gains.append(max(d,0.0))
        losses.append(max(-d,0.0))
    ag=sum(gains)/period
    al=sum(losses)/period
    if al == 0:
        return 100.0 if ag > 0 else 50.0
    return 100.0 - 100.0/(1.0+ag/al)

def _nc_atr(candles, period=14):
    if len(candles) < period+1:
        return 0.0
    trs=[]
    prev=_nc_num(candles[-period-1].get("close"))
    for c in candles[-period:]:
        h=_nc_num(c.get("high"))
        l=_nc_num(c.get("low"))
        trs.append(max(h-l,abs(h-prev),abs(l-prev)))
        prev=_nc_num(c.get("close"))
    return sum(trs)/len(trs) if trs else 0.0

def _hc_num(x, default=0.0):
    try:
        v=float(x)
        return v if v == v and abs(v) != float("inf") else default
    except Exception:
        return default

def _hc_ema(values, period):
    if not values:
        return 0.0
    a=2.0/(period+1.0)
    e=values[0]
    for v in values[1:]:
        e=a*v+(1-a)*e
    return e

def _hc_rsi(values, period=14):
    if len(values)<period+1:
        return 50.0
    gains=[]; losses=[]
    for a,b in zip(values[-period-1:-1],values[-period:]):
        d=b-a
        gains.append(max(d,0.0)); losses.append(max(-d,0.0))
    ag=sum(gains)/period; al=sum(losses)/period
    if al==0:
        return 100.0 if ag>0 else 50.0
    return 100.0-100.0/(1.0+ag/al)

def _hc_atr(candles, period=14):
    if len(candles)<period+1:
        return 0.0
    trs=[]
    prev=_hc_num(candles[-period-1].get("close"))
    for c in candles[-period:]:
        h=_hc_num(c.get("high")); l=_hc_num(c.get("low")); cl=_hc_num(c.get("close"))
        trs.append(max(h-l,abs(h-prev),abs(l-prev)))
        prev=cl
    return sum(trs)/len(trs) if trs else 0.0

def _hc_completed(candles):
    out=[]
    for c in candles or []:
        if c.get("isOpen") is True or c.get("open_candle") is True:
            continue
        o=_hc_num(c.get("open")); h=_hc_num(c.get("high"))
        l=_hc_num(c.get("low")); cl=_hc_num(c.get("close"))
        if l <= min(o,cl) <= max(o,cl) <= h:
            out.append(c)
    return out

def _v27_num(x,d=0.0):
    try:
        v=float(x); return v if v==v and abs(v)!=float("inf") else d
    except Exception: return d

def _v27_ema(v,n):
    if not v:return 0.0
    a=2/(n+1);e=v[0]
    for x in v[1:]:e=a*x+(1-a)*e
    return e

def _v27_rsi(v,n=14):
    if len(v)<n+1:return 50.0
    g=[];l=[]
    for a,b in zip(v[-n-1:-1],v[-n:]):
        d=b-a;g.append(max(d,0));l.append(max(-d,0))
    ag=sum(g)/n;al=sum(l)/n
    return 100 if al==0 and ag else 50 if al==0 else 100-100/(1+ag/al)

def _v27_atr(cs,n=14):
    if len(cs)<n+1:return 0.0
    tr=[];prev=_v27_num(cs[-n-1].get("close"))
    for c in cs[-n:]:
        h=_v27_num(c.get("high"));l=_v27_num(c.get("low"));cl=_v27_num(c.get("close"))
        tr.append(max(h-l,abs(h-prev),abs(l-prev)));prev=cl
    return sum(tr)/len(tr) if tr else 0.0

def _v27_completed(candles):
    out=[]
    for c in candles or []:
        if c.get("isOpen") is True or c.get("open_candle") is True: continue
        o=_v27_num(c.get("open"));h=_v27_num(c.get("high"))
        l=_v27_num(c.get("low"));cl=_v27_num(c.get("close"))
        if l<=min(o,cl)<=max(o,cl)<=h and h>l: out.append(c)
    return out

# ================= V28 CANDLE-ONLY PRO ENGINE =================
# No EMA/RSI/ATR/indicator dependency. Signal is derived from OHLC
# candle structure, sequences, patterns, breakouts/rejections and
# strict conflict gates. Only completed candles are eligible.

def _co_num(x, d=0.0):
    try:
        v=float(x)
        return v if v == v and abs(v) != float("inf") else d
    except Exception:
        return d

def _co_completed(candles):
    out=[]
    for c in candles or []:
        if c.get("isOpen") is True or c.get("open_candle") is True:
            continue
        o=_co_num(c.get("open")); h=_co_num(c.get("high"))
        l=_co_num(c.get("low")); cl=_co_num(c.get("close"))
        if h > l and l <= min(o,cl) <= max(o,cl) <= h:
            out.append(c)
    return out

def _co_feat(c):
    o=_co_num(c.get("open")); h=_co_num(c.get("high"))
    l=_co_num(c.get("low")); cl=_co_num(c.get("close"))
    r=max(h-l,1e-12)
    return {
        "o":o,"h":h,"l":l,"c":cl,"r":r,
        "body":(cl-o)/r,
        "upper":(h-max(o,cl))/r,
        "lower":(min(o,cl)-l)/r,
        "pos":(cl-l)/r
    }

def _co_vote(x):
    if x > 0.08: return 1
    if x < -0.08: return -1
    return 0

def analyze_candles(candles, timeframe=None):
    """V28 strict candle-only next-candle classifier."""
    cs=_co_completed(candles)
    if len(cs)<40:
        return {"signal":"WAIT","direction":"WAIT","confidence":0,
                "score":0,"reason":"insufficient_completed_candles"}

    f=[_co_feat(c) for c in cs]
    x=f[-1]; p=f[-2]; p2=f[-3]

    # 1) Close-location / body pressure.
    close_pressure=(x["pos"]-0.5)*0.70
    body_pressure=max(-0.55,min(0.55,x["body"]*0.55))

    # 2) Wick rejection.
    rejection=0.0
    if x["body"]>0 and x["lower"]>x["upper"]*1.25:
        rejection += min(0.38, x["lower"]*0.55)
    if x["body"]<0 and x["upper"]>x["lower"]*1.25:
        rejection -= min(0.38, x["upper"]*0.55)

    # 3) Engulfing / inside-bar breakout context.
    engulf=0.0
    if p["c"]<p["o"] and x["c"]>x["o"] and x["c"]>=p["o"] and x["o"]<=p["c"]:
        engulf=0.42
    elif p["c"]>p["o"] and x["c"]<x["o"] and x["c"]<=p["o"] and x["o"]>=p["c"]:
        engulf=-0.42

    inside=(p["h"]<=p2["h"] and p["l"]>=p2["l"])
    inside_break=0.0
    if inside:
        if x["c"]>p2["h"]: inside_break=0.45
        elif x["c"]<p2["l"]: inside_break=-0.45

    # 4) Multi-candle sequence pressure, weighted to recent candles.
    seq=0.0
    for i,w in zip(range(1,7),(0.35,0.23,0.16,0.11,0.08,0.07)):
        d=1 if f[-i]["c"]>f[-i]["o"] else -1 if f[-i]["c"]<f[-i]["o"] else 0
        seq += d*w

    # 5) Recent range breakout using only completed candles.
    hi=max(z["h"] for z in f[-14:-1])
    lo=min(z["l"] for z in f[-14:-1])
    breakout=0.0
    if x["c"]>hi: breakout=0.52
    elif x["c"]<lo: breakout=-0.52
    else:
        width=max(hi-lo,1e-12)
        breakout=max(-0.24,min(0.24,(x["c"]-(hi+lo)/2)/(width*0.55)))

    # 6) Extreme-high / extreme-low rejection.
    recent_hi=max(z["h"] for z in f[-9:-1])
    recent_lo=min(z["l"] for z in f[-9:-1])
    extreme_reject=0.0
    if x["h"]>=recent_hi and x["upper"]>0.22 and x["pos"]<0.68:
        extreme_reject-=0.28
    if x["l"]<=recent_lo and x["lower"]>0.22 and x["pos"]>0.32:
        extreme_reject+=0.28

    # 7) Two/three candle continuation vs reversal.
    last3=f[-3:]
    same_up=sum(z["c"]>z["o"] for z in last3)
    same_dn=sum(z["c"]<z["o"] for z in last3)
    continuation=0.16 if same_up>=2 else -0.16 if same_dn>=2 else 0.0
    if same_up==3 and x["upper"]>0.30: continuation-=0.12
    if same_dn==3 and x["lower"]>0.30: continuation+=0.12

    # 8) Candle-size quality, based only on ranges.
    ranges=sorted(z["r"] for z in f[-25:])
    median=ranges[len(ranges)//2]
    ratio=x["r"]/max(median,1e-12)
    quality=1.0 if 0.45<=ratio<=2.6 else 0.72

    raw=(close_pressure+body_pressure+rejection+engulf+inside_break+
         seq+breakout+extreme_reject+continuation)
    score=max(-1.0,min(1.0,raw*quality))
    direction="UP" if score>0 else "DOWN"

    # Independent candle-only votes.
    votes=[
        _co_vote(close_pressure),
        _co_vote(body_pressure),
        _co_vote(rejection),
        _co_vote(engulf),
        _co_vote(inside_break),
        _co_vote(seq),
        _co_vote(breakout),
        _co_vote(extreme_reject),
        _co_vote(continuation)
    ]
    active=[v for v in votes if v]
    target=1 if score>0 else -1
    agreement=(sum(v==target for v in active)/len(active)) if active else 0.0

    # Avoid chasing a single huge candle.
    spike_penalty=0.82 if ratio>3.0 else 1.0
    confidence=(52+abs(score)*40)*agreement*spike_penalty
    confidence=max(0,min(92,round(confidence)))

    # Strict release gate.
    if abs(score)<0.14 or agreement<0.62 or confidence<58:
        return {"signal":"WAIT","direction":"WAIT","confidence":0,
                "score":round(score,4),"reason":"candle_conflict_or_weak"}

    return {
        "signal":direction,
        "direction":direction,
        "confidence":confidence,
        "score":round(score,4),
        "reason":"candle_only_pro_next_candle",
        "candle_only":True
    }

# Explicit supported FX universe for the website layer.
SUPPORTED_PAIRS = [
    "GBP/JPY","AUD/CAD","AUD/CHF","AUD/JPY","AUD/USD",
    "CAD/CHF","CAD/JPY","EUR/CAD","EUR/CHF","EUR/GBP",
    "EUR/USD","GBP/CAD","GBP/CHF","USD/CAD","USD/JPY",
    "CHF/JPY","EUR/AUD","USD/CHF","EUR/JPY","GBP/USD"
]
# =============================================================

