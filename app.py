"""
Market Signal Telegram + Website Chatbot
Single-file MVP: app.py

Features:
- FastAPI website/API
- Telegram bot polling
- Twelve Data market candles
- RSI, EMA 9/21, MACD, Bollinger Bands, ADX, ATR
- UP / DOWN / WAIT scoring
- Signal history in SQLite
- Background scheduler that watches a symbol list and pushes Telegram alerts
- Outcome resolution job that grades past signals against real price action,
  so /api/accuracy reflects genuine historical hit-rate (not a guess)
- No automatic trading/order placement

Install:
    pip install -r requirements.txt

Create .env (copy from .env.example):
    TWELVE_DATA_API_KEY=YOUR_TWELVE_DATA_KEY
    TELEGRAM_BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID=YOUR_CHAT_ID_FOR_ALERTS   (optional, for scheduler push)
    WATCHLIST=AUD/NZD,EUR/USD                  (optional, for scheduler)
    SCAN_INTERVAL_SECONDS=300                  (optional, default 300)
    HOST=0.0.0.0
    PORT=8000

Run:
    python app.py

Website:
    http://YOUR_SERVER:8000/

API:
    /api/signal?symbol=EUR/USD&interval=1min
    /api/history
    /api/accuracy
    /api/assets
    /health

Telegram:
    /start
    /signal EURUSD 1m
    /signal GBPUSD 5m
    /history
    /accuracy
    /help

IMPORTANT / READ THIS:
This is a market-analysis tool, not a guaranteed predictor. Nothing in this
codebase, no matter how it's configured, can reliably predict the direction
of the next candle with high accuracy. "Confidence" is a score derived from
indicator agreement, not a statistical probability of being right. The
accuracy numbers shown by /api/accuracy are historical and calculated from
this bot's own past signals -- they are not, and cannot be, a promise about
future performance. This tool does not place trades in Quotex or any other
broker; it only informs.
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
                result TEXT DEFAULT 'PENDING'
            )
        """)
        # Backfill atr column for DBs created before this field existed.
        cols = [r[1] for r in con.execute("PRAGMA table_info(signals)").fetchall()]
        if "atr" not in cols:
            con.execute("ALTER TABLE signals ADD COLUMN atr REAL")
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

def make_signal(df: pd.DataFrame):
    close = df["close"]

    e9 = ema(close, 9)
    e21 = ema(close, 21)
    rv = rsi(close, 14)
    ml, ms = macd(close)
    bm, bu, bl = bollinger(close)
    ax = adx(df, 14)
    av = atr(df, 14)

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

    score = 0
    reasons = []

    # Trend
    if ema9_v > ema21_v:
        score += 20
        reasons.append("EMA bullish")
    elif ema9_v < ema21_v:
        score -= 20
        reasons.append("EMA bearish")

    # RSI: momentum confirmation, not blind overbought/oversold
    if 50 <= rsi_v <= 70:
        score += 15
        reasons.append("RSI bullish zone")
    elif 30 <= rsi_v < 50:
        score -= 15
        reasons.append("RSI bearish zone")
    elif rsi_v > 75:
        score -= 8
        reasons.append("RSI very high")
    elif rsi_v < 25:
        score += 8
        reasons.append("RSI very low")

    # MACD
    if macd_v > macd_s:
        score += 20
        reasons.append("MACD bullish")
    elif macd_v < macd_s:
        score -= 20
        reasons.append("MACD bearish")

    # Bollinger midline
    if price > bbm:
        score += 10
        reasons.append("Above BB mid")
    elif price < bbm:
        score -= 10
        reasons.append("Below BB mid")

    # ADX confirms trend strength, while DI direction is approximated by EMA/MACD
    if adx_v >= 25:
        if score > 0:
            score += 10
        elif score < 0:
            score -= 10
        reasons.append("ADX trend confirmation")

    # Short candle momentum
    if len(df) >= 4:
        c1 = float(close.iloc[-1])
        c3 = float(close.iloc[-3])
        if c1 > c3:
            score += 10
            reasons.append("Short-term momentum up")
        elif c1 < c3:
            score -= 10
            reasons.append("Short-term momentum down")

    # ATR-based volatility check. This does not predict direction; it flags
    # when price is barely moving, which makes any signal (from any tool)
    # less meaningful because the "trend" may just be noise.
    atr_pct = (atr_v / price * 100) if price else 0.0
    if atr_pct < 0.03:
        score = int(score * 0.6)
        reasons.append("Low volatility (ATR) - signal weakened")
    else:
        reasons.append(f"ATR volatility {round(atr_pct, 3)}%")

    score = int(max(-100, min(100, score)))

    if score >= 60:
        direction = "UP"
    elif score <= -60:
        direction = "DOWN"
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
        "reasons": reasons,
    }

def save_signal(symbol, interval, s):
    with DB_LOCK:
        con = db()
        cur = con.execute("""
            INSERT INTO signals
            (symbol, interval, timestamp, price, direction, score, confidence,
             rsi, ema9, ema21, macd, macd_signal, adx, atr, bb_mid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, interval, s["timestamp"], s["price"], s["direction"],
            s["score"], s["confidence"], s["rsi"], s["ema9"], s["ema21"],
            s["macd"], s["macd_signal"], s["adx"], s.get("atr"), s["bb_mid"]
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

        if row["direction"] == "UP":
            result = "WIN" if future_price > row["price"] else "LOSS" if future_price < row["price"] else "FLAT"
        else:  # DOWN
            result = "WIN" if future_price < row["price"] else "LOSS" if future_price > row["price"] else "FLAT"

        with DB_LOCK:
            con = db()
            con.execute("UPDATE signals SET result = ? WHERE id = ?", (result, row["id"]))
            con.commit()
            con.close()
        resolved_count += 1

    return resolved_count

def format_signal(symbol, interval, s):
    arrow = "📈" if s["direction"] == "UP" else "📉" if s["direction"] == "DOWN" else "⚪"
    return (
        f"{arrow} MARKET SIGNAL\n\n"
        f"Asset: {normalize_symbol(symbol)}\n"
        f"Timeframe: {normalize_interval(interval)}\n\n"
        f"Direction: {s['direction']}\n"
        f"Signal score: {s['score']:+d}\n"
        f"Score strength: {s['confidence']}/100\n\n"
        f"Price: {s['price']}\n"
        f"RSI: {s['rsi']}\n"
        f"EMA 9: {s['ema9']}\n"
        f"EMA 21: {s['ema21']}\n"
        f"MACD: {s['macd']}\n"
        f"MACD signal: {s['macd_signal']}\n"
        f"ADX: {s['adx']}\n\n"
        f"Reasons: {', '.join(s['reasons'][:5])}\n\n"
        "⚠️ Analysis only. No guarantee of future price movement. "
        "This bot does not place trades."
    )

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
 const cls = dir === 'UP' ? 'up' : dir === 'DOWN' ? 'down' : 'wait';
 return '<span class="badge '+cls+'">'+dir+'</span>';
}

function renderChart(labels, prices){
 const ctx = document.getElementById('chart');
 if(chartObj) chartObj.destroy();
 chartObj = new Chart(ctx, {
  type: 'line',
  data: { labels, datasets: [{ label: 'Close price', data: prices, borderColor: '#3a63ff', tension: 0.15, pointRadius: 0 }] },
  options: { scales: { x: { ticks: { color: '#aaa', maxTicksLimit: 8 } }, y: { ticks: { color: '#aaa' } } }, plugins: { legend: { labels: { color: '#fff' } } } }
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
   'ATR: '+d.atr+'\\n\\n'+
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
                s = await asyncio.to_thread(calculate_signal, raw_symbol, "5m")
                print(f"Scheduler: {normalize_symbol(raw_symbol)} -> {s['direction']} ({s['confidence']})")

                tg_app = _telegram_app_ref["app"]
                if tg_app and TELEGRAM_CHAT_ID and s["direction"] != "WAIT":
                    await tg_app.bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=format_signal(raw_symbol, "5m", s),
                    )
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
        "/signal EURUSD 1m\n"
        "/signal GBPUSD 5m\n"
        "/history\n"
        "/accuracy\n"
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
        s = await asyncio.to_thread(calculate_signal, symbol, interval)
        await update.message.reply_text(format_signal(symbol, interval, s))
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
    start_telegram_thread()
    start_scheduler_thread()
    uvicorn.run(app, host=HOST, port=PORT)
