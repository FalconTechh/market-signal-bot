
import os, time, sqlite3, threading, asyncio, logging
from datetime import datetime, timezone, timedelta
import numpy as np, pandas as pd, requests
from flask import Flask
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters

BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","")
FINNHUB_API_KEY=os.getenv("FINNHUB_API_KEY","")
ADMIN_ID=os.getenv("ADMIN_TELEGRAM_ID","")
DB=os.getenv("DATABASE_PATH","nexcandle.db")
PREMIUM_DAYS=int(os.getenv("PREMIUM_DAYS","30"))
ALERT_SEC=max(30,int(os.getenv("ALERT_INTERVAL_SECONDS","60")))
INDIA_UPI=os.getenv("INDIA_UPI","6361472511")
UAE_BOTIM=os.getenv("UAE_BOTIM","0522445121")
PAIRS={"EUR/USD":"OANDA:EUR_USD","GBP/USD":"OANDA:GBP_USD","USD/JPY":"OANDA:USD_JPY","USD/CHF":"OANDA:USD_CHF","AUD/USD":"OANDA:AUD_USD","USD/CAD":"OANDA:USD_CAD","NZD/USD":"OANDA:NZD_USD","EUR/GBP":"OANDA:EUR_GBP","EUR/JPY":"OANDA:EUR_JPY","GBP/JPY":"OANDA:GBP_JPY"}
TF={"1m":1,"5m":5,"15m":15,"30m":30,"45m":45,"1h":60,"4h":240}
S=requests.Session()
LOCK=threading.RLock()
logging.basicConfig(level=logging.INFO)

web=Flask(__name__)
@web.get("/")
def home(): return {"service":"NexCandle AI","status":"online"}
@web.get("/health")
def health(): return {"status":"ok"}
def run_web(): web.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")),debug=False,use_reloader=False)

def con():
    c=sqlite3.connect(DB,timeout=30,check_same_thread=False); c.row_factory=sqlite3.Row; return c
def init():
    with LOCK:
        c=con()
        c.executescript("""CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY,username TEXT,first_name TEXT,premium_until TEXT,free_signals INTEGER DEFAULT 3,alerts INTEGER DEFAULT 0,created TEXT);
        CREATE TABLE IF NOT EXISTS payments(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,username TEXT,reference TEXT,status TEXT DEFAULT 'pending',created TEXT);
        CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,pair TEXT,tf TEXT,direction TEXT,score INTEGER,entry REAL,stop REAL,target REAL,rr REAL,candle TEXT,created TEXT,resolved TEXT DEFAULT 'PENDING');
        CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,pair TEXT,tf TEXT,direction TEXT,candle TEXT,UNIQUE(user_id,pair,tf,direction,candle));""")
        c.commit(); c.close()
def user(u):
    with LOCK:
        c=con(); c.execute("""INSERT INTO users(user_id,username,first_name,free_signals,created) VALUES(?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name""",(u.id,u.username or "",u.first_name or "",3,datetime.now(timezone.utc).isoformat())); c.commit(); c.close()
def isadmin(uid): return bool(ADMIN_ID) and str(uid)==str(ADMIN_ID)
def premium(uid):
    if isadmin(uid): return True
    with LOCK:
        c=con(); r=c.execute("SELECT premium_until FROM users WHERE user_id=?",(uid,)).fetchone(); c.close()
    try: return bool(r and r["premium_until"] and datetime.fromisoformat(r["premium_until"])>datetime.now(timezone.utc))
    except: return False
def alerts_on(uid):
    with LOCK:
        c=con(); r=c.execute("SELECT alerts FROM users WHERE user_id=?",(uid,)).fetchone(); c.close()
    return bool(r and r["alerts"])
def free_ok(uid):
    if premium(uid): return True
    with LOCK:
        c=con(); r=c.execute("SELECT free_signals FROM users WHERE user_id=?",(uid,)).fetchone(); c.close()
    return bool(r and r["free_signals"]>0)
def consume(uid):
    if premium(uid): return
    with LOCK:
        c=con(); c.execute("UPDATE users SET free_signals=MAX(free_signals-1,0) WHERE user_id=?",(uid,)); c.commit(); c.close()

def candles(pair,tf,limit=300):
    if not FINNHUB_API_KEY: raise RuntimeError("FINNHUB_API_KEY is missing")
    res=15 if tf=="45m" else TF[tf]; now=int(time.time()); start=now-res*60*max(250,limit*3)
    r=S.get("https://finnhub.io/api/v1/forex/candle",params={"symbol":PAIRS[pair],"resolution":res,"from":start,"to":now,"token":FINNHUB_API_KEY},timeout=15)
    r.raise_for_status(); d=r.json()
    if d.get("s")!="ok": raise RuntimeError("Finnhub returned no candle data")
    x=pd.DataFrame({"open":d["o"],"high":d["h"],"low":d["l"],"close":d["c"]},index=pd.to_datetime(d["t"],unit="s",utc=True)).dropna()
    if tf=="45m": x=x.resample("45min",origin="epoch").agg({"open":"first","high":"max","low":"min","close":"last"}).dropna()
    return x.tail(limit)

def ema(s,n): return s.ewm(span=n,adjust=False).mean()
def rsi(s,n=14):
    d=s.diff(); g=d.clip(lower=0); l=-d.clip(upper=0)
    ag=g.ewm(alpha=1/n,adjust=False).mean(); al=l.ewm(alpha=1/n,adjust=False).mean()
    return 100-100/(1+ag/al.replace(0,np.nan))
def atr(x,n=14):
    p=x.close.shift(); tr=pd.concat([x.high-x.low,(x.high-p).abs(),(x.low-p).abs()],axis=1).max(axis=1)
    return tr.ewm(alpha=1/n,adjust=False).mean()
def calc(x):
    x=x.copy(); x["e9"]=ema(x.close,9); x["e21"]=ema(x.close,21); x["e50"]=ema(x.close,50); x["e200"]=ema(x.close,200); x["rsi"]=rsi(x.close); x["atr"]=atr(x)
    m=ema(x.close,12)-ema(x.close,26); x["macd"]=m; x["ms"]=ema(m,9)
    x["bb"]=x.close.rolling(20).mean(); sd=x.close.rolling(20).std(); x["bu"]=x.bb+2*sd; x["bl"]=x.bb-2*sd
    return x.dropna()
def analyse(x):
    x=calc(x); a=x.iloc[-1]; p=x.iloc[-2]; bull=bear=0; why=[]
    if a.e9>a.e21>a.e50: bull+=22; why.append("EMA trend bullish")
    elif a.e9<a.e21<a.e50: bear+=22; why.append("EMA trend bearish")
    if a.close>a.e200: bull+=10; why.append("above EMA200")
    else: bear+=10; why.append("below EMA200")
    if a.macd>a.ms and a.macd>=p.macd: bull+=15; why.append("MACD bullish")
    elif a.macd<a.ms and a.macd<=p.macd: bear+=15; why.append("MACD bearish")
    if 52<=a.rsi<=70: bull+=15; why.append("RSI bullish zone")
    elif 30<=a.rsi<=48: bear+=15; why.append("RSI bearish zone")
    if a.close>a.bb: bull+=8
    else: bear+=8
    gap=abs(bull-bear)
    d="WAIT" if gap<15 else ("CALL" if bull>bear else "PUT")
    return d,int(min(100,max(bull,bear))),why,x
def mtf(pair):
    out={}; total={"CALL":0,"PUT":0}
    for tf,w in [("5m",1),("15m",1.4),("30m",1.8),("45m",2),("1h",2.5),("4h",3)]:
        try:
            d,s,why,_=analyse(candles(pair,tf)); out[tf]=(d,s)
            if d in total: total[d]+=s*w
        except Exception as e: out[tf]=("ERROR",0)
    final="CALL" if total["CALL"]>total["PUT"]*1.12 else "PUT" if total["PUT"]>total["CALL"]*1.12 else "WAIT"
    return final,int(min(100,max(total.values())/11.7)),out
def wait(tf):
    n=TF[tf]*60; return n-(int(time.time())%n)
def signal(pair,tf):
    d,s,why,x=analyse(candles(pair,tf)); md,ms,m=mtf(pair)
    final=d if d==md else "WAIT"; score=int(min(100,s*.55+ms*.45)); entry=stop=target=rr=None
    if final!="WAIT":
        e=float(x.close.iloc[-1]); v=max(float(x.atr.iloc[-1]),e*.0001)
        stop=e-1.25*v if final=="CALL" else e+1.25*v; target=e+1.75*v if final=="CALL" else e-1.75*v
        entry=e; rr=abs(target-e)/abs(e-stop)
    return {"pair":pair,"tf":tf,"direction":final,"score":score,"why":why,"mtf":m,"wait":wait(tf),"entry":entry,"stop":stop,"target":target,"rr":rr,"candle":x.index[-1].isoformat()}
def fmt(s):
    z=[f"⚡ <b>NexCandle AI SIGNAL</b>","",f"💱 {s['pair']} | ⏱ {s['tf']}",f"📌 <b>{s['direction']}</b>",f"🎯 Score: <b>{s['score']}/100</b>",f"⏳ Next candle: <b>{s['wait']//60}m {s['wait']%60}s</b>"]
    if s["entry"] is not None: z += [f"📍 Entry reference: <code>{s['entry']:.6f}</code>",f"🛑 Stop reference: <code>{s['stop']:.6f}</code>",f"🎯 Target reference: <code>{s['target']:.6f}</code>",f"⚖️ R:R 1:{s['rr']:.2f}"]
    z += ["","🧭 <b>MTF</b>"]+[f"{k}: {v[0]} ({v[1]})" for k,v in s["mtf"].items()]+["","• "+" | ".join(s["why"]),"","⚠️ Score is indicator agreement, not a guaranteed win probability."]
    return "\n".join(z)

WELCOME="""⚡ <b>NexCandle AI</b>

Welcome! 👋
Choose a button below. You do not need to type /start after opening the bot.

📊 Real provider candle data + technical analysis
⏱ 1m / 5m / 15m / 30m / 45m / 1h / 4h

⚠️ No system can guarantee the next candle."""

def menu(uid):
    return InlineKeyboardMarkup([
      [InlineKeyboardButton("📊 Get Signal",callback_data="signal"),InlineKeyboardButton("🧭 MTF",callback_data="mtf")],
      [InlineKeyboardButton("🔥 Best Setup",callback_data="best"),InlineKeyboardButton("🔎 Scan",callback_data="scan")],
      [InlineKeyboardButton("⏰ Entry Timing",callback_data="timing"),InlineKeyboardButton("🎯 Accuracy",callback_data="accuracy")],
      [InlineKeyboardButton("📈 Backtest",callback_data="backtest"),InlineKeyboardButton("📜 History",callback_data="history")],
      [InlineKeyboardButton("💎 Premium",callback_data="premium"),InlineKeyboardButton("🔔 Alerts",callback_data="alerts")],
      [InlineKeyboardButton("ℹ️ Help",callback_data="help")]
    ])
def pairmenu(mode):
    ps=list(PAIRS); rows=[]
    for i in range(0,len(ps),2): rows.append([InlineKeyboardButton(p,callback_data=f"{mode}:{p.replace('/','~')}") for p in ps[i:i+2]])
    rows.append([InlineKeyboardButton("« Back",callback_data="menu")]); return InlineKeyboardMarkup(rows)

async def start(u,c): user(u.effective_user); await u.message.reply_text(WELCOME,parse_mode=ParseMode.HTML,reply_markup=menu(u.effective_user.id))
async def text(u,c): user(u.effective_user); await u.message.reply_text(WELCOME,parse_mode=ParseMode.HTML,reply_markup=menu(u.effective_user.id))

async def cb(u,c):
    q=u.callback_query; await q.answer(); uid=q.from_user.id; user(q.from_user); d=q.data
    if d=="menu": return await q.message.reply_text(WELCOME,parse_mode=ParseMode.HTML,reply_markup=menu(uid))
    if d=="signal": return await q.message.reply_text("📊 Choose pair:",reply_markup=pairmenu("signal"))
    if d.startswith("signal:"):
        p=d.split(":")[1].replace("~","/"); rows=[[InlineKeyboardButton(t,callback_data=f"run:{p.replace('/','~')}:{t}") for t in ["1m","5m","15m"]],[InlineKeyboardButton(t,callback_data=f"run:{p.replace('/','~')}:{t}") for t in ["30m","45m","1h","4h"]]]
        return await q.message.reply_text("⏱ Choose timeframe:",reply_markup=InlineKeyboardMarkup(rows))
    if d.startswith("run:"):
        _,p,t=d.split(":"); p=p.replace("~","/")
        if not free_ok(uid): return await q.message.reply_text("🔒 Premium required.",reply_markup=menu(uid))
        await q.message.reply_text("⏳ Analysing live provider data + MTF...")
        try:
            s=signal(p,t); consume(uid)
            with LOCK:
                x=con(); x.execute("INSERT INTO signals(user_id,pair,tf,direction,score,entry,stop,target,rr,candle,created) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(uid,p,t,s["direction"],s["score"],s["entry"],s["stop"],s["target"],s["rr"],s["candle"],datetime.now(timezone.utc).isoformat())); x.commit(); x.close()
            return await q.message.reply_text(fmt(s),parse_mode=ParseMode.HTML,reply_markup=menu(uid))
        except Exception as e: return await q.message.reply_text(f"⚠️ Data error: <code>{str(e)[:250]}</code>",parse_mode=ParseMode.HTML,reply_markup=menu(uid))
    if d=="mtf": return await q.message.reply_text("🧭 Choose pair:",reply_markup=pairmenu("mtf"))
    if d.startswith("mtf:"):
        p=d.split(":")[1].replace("~","/")
        try:
            f,s,m=mtf(p); out=f"🧭 <b>{p}</b>\nFinal: <b>{f}</b>\nScore: <b>{s}/100</b>\n\n"+"\n".join(f"{k}: {v[0]} ({v[1]})" for k,v in m.items())
        except Exception as e: out=f"⚠️ {e}"
        return await q.message.reply_text(out,parse_mode=ParseMode.HTML,reply_markup=menu(uid))
    if d=="timing":
        rows=[[InlineKeyboardButton(t,callback_data=f"time:{t}") for t in ["1m","5m","15m","30m"]],[InlineKeyboardButton(t,callback_data=f"time:{t}") for t in ["45m","1h","4h"]]]
        return await q.message.reply_text("⏰ Select timeframe:",reply_markup=InlineKeyboardMarkup(rows))
    if d.startswith("time:"):
        t=d.split(":")[1]; w=wait(t); return await q.message.reply_text(f"⏰ <b>{t}</b>\nNext candle boundary in <b>{w//60}m {w%60}s</b>",parse_mode=ParseMode.HTML,reply_markup=menu(uid))
    if d=="premium":
        if premium(uid): out="💎 <b>PREMIUM ACTIVE</b>"
        else: out=f"💎 <b>PREMIUM</b>\n\n🇮🇳 India UPI: <code>{INDIA_UPI}</code>\n🇦🇪 UAE BOTIM Pay: <code>{UAE_BOTIM}</code>\n\nPay and use the button below."
        kb=[[InlineKeyboardButton("📋 Status",callback_data="status")],[InlineKeyboardButton("« Back",callback_data="menu")]]
        return await q.message.reply_text(out,parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(kb))
    if d=="status":
        with LOCK:
            x=con(); r=x.execute("SELECT premium_until,free_signals FROM users WHERE user_id=?",(uid,)).fetchone(); x.close()
        return await q.message.reply_text(f"📋 Premium: {'✅' if premium(uid) else '❌'}\nValid until: {r['premium_until'] if r else '—'}\nFree signals: {r['free_signals'] if r else 0}",reply_markup=menu(uid))
    if d=="alerts":
        if not premium(uid): return await q.message.reply_text("🔒 Auto Alerts are Premium.",reply_markup=menu(uid))
        with LOCK:
            x=con(); x.execute("UPDATE users SET alerts=? WHERE user_id=?",(0 if alerts_on(uid) else 1,uid)); x.commit(); x.close()
        return await q.message.reply_text(f"🔔 Auto Alerts {'ON' if alerts_on(uid) else 'OFF'}",reply_markup=menu(uid))
    if d=="accuracy":
        with LOCK:
            x=con(); r=x.execute("SELECT COUNT(*) n FROM signals WHERE user_id=? AND resolved IN ('WIN','LOSS')",(uid,)).fetchone(); x.close()
        return await q.message.reply_text(f"🎯 Resolved signals: {r['n']}\n\nNo guaranteed accuracy is claimed.",reply_markup=menu(uid))
    if d=="history":
        with LOCK:
            x=con(); rs=x.execute("SELECT pair,tf,direction,score,resolved FROM signals WHERE user_id=? ORDER BY id DESC LIMIT 10",(uid,)).fetchall(); x.close()
        out="📜 <b>History</b>\n\n"+"\n".join(f"{r['pair']} {r['tf']} — {r['direction']} ({r['score']}) — {r['resolved']}" for r in rs)
        return await q.message.reply_text(out,parse_mode=ParseMode.HTML,reply_markup=menu(uid))
    if d=="best":
        await q.message.reply_text("🔥 Scanning...")
        best=None
        for p in PAIRS:
            try:
                s=signal(p,"5m")
                if s["direction"]!="WAIT" and (best is None or s["score"]>best["score"]): best=s
            except: pass
        return await q.message.reply_text(fmt(best) if best else "🟡 No qualifying setup.",parse_mode=ParseMode.HTML,reply_markup=menu(uid))
    if d=="scan":
        await q.message.reply_text("🔎 Scanning supported pairs...")
        r=[]
        for p in PAIRS:
            try:
                s=signal(p,"5m")
                if s["direction"]!="WAIT": r.append(s)
            except: pass
        r.sort(key=lambda z:z["score"],reverse=True)
        return await q.message.reply_text("🔎 <b>Top Scan</b>\n\n"+("\n".join(f"{s['pair']} — {s['direction']} — {s['score']}/100" for s in r[:7]) or "No setup."),parse_mode=ParseMode.HTML,reply_markup=menu(uid))
    if d=="backtest": return await q.message.reply_text("📈 Choose pair:",reply_markup=pairmenu("back"))
    if d.startswith("back:"):
        p=d.split(":")[1].replace("~","/")
        try:
            x=candles(p,"5m",400); w=l=0
            for i in range(80,len(x)-1):
                z=analyse(x.iloc[:i+1]); d0=z[0]
                if d0=="WAIT": continue
                ok=(x.close.iloc[i+1]>x.close.iloc[i]) if d0=="CALL" else (x.close.iloc[i+1]<x.close.iloc[i]); w+=ok; l+=not ok
            n=w+l; hit=100*w/n if n else 0; out=f"📈 <b>Backtest {p}</b>\nSignals: {n}\nWins: {w}\nLosses: {l}\nHit-rate: {hit:.1f}%"
        except Exception as e: out=f"⚠️ {e}"
        return await q.message.reply_text(out,parse_mode=ParseMode.HTML,reply_markup=menu(uid))
    if d=="help":
        return await q.message.reply_text("ℹ️ <b>Help</b>\n\nButtons are direct-use. Get Signal combines indicators and MTF confirmation. Entry Timing shows the next candle boundary. Premium enables advanced access and auto alerts.\n\n⚠️ No system guarantees future candles.",parse_mode=ParseMode.HTML,reply_markup=menu(uid))

async def alerts_loop(app):
    while True:
        try:
            with LOCK:
                x=con(); us=x.execute("SELECT user_id FROM users WHERE alerts=1").fetchall(); x.close()
            for row in us:
                uid=row["user_id"]
                if not premium(uid): continue
                for p in PAIRS:
                    try:
                        s=signal(p,"5m")
                        if s["direction"]=="WAIT" or s["score"]<75: continue
                        with LOCK:
                            x=con()
                            try:
                                x.execute("INSERT INTO alerts(user_id,pair,tf,direction,candle) VALUES(?,?,?,?,?)",(uid,p,"5m",s["direction"],s["candle"])); x.commit(); fresh=True
                            except sqlite3.IntegrityError: fresh=False
                            x.close()
                        if fresh: await app.bot.send_message(uid,"🔔 <b>PREMIUM AUTO ALERT</b>\n\n"+fmt(s),parse_mode=ParseMode.HTML,reply_markup=menu(uid))
                    except: pass
        except: pass
        await asyncio.sleep(ALERT_SEC)

async def post(app): app.create_task(alerts_loop(app))
def main():
    if not BOT_TOKEN: raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    init(); threading.Thread(target=run_web,daemon=True).start()
    a=Application.builder().token(BOT_TOKEN).post_init(post).build()
    a.add_handler(CommandHandler("start",start)); a.add_handler(CallbackQueryHandler(cb)); a.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,text))
    a.run_polling(drop_pending_updates=True)
if __name__=="__main__": main()
