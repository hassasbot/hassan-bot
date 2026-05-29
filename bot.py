#!/usr/bin/env python3
# ============================================
# hassan_final_scalp_bot_v2.py
# Ultra-Premium Institutional Scalper
# Smart Exit + Balanced Settings
# ============================================

import asyncio, json, time, ssl, logging, os, random, traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque, defaultdict
import aiohttp
from websockets.legacy.client import connect
import numpy as np
from logging.handlers import RotatingFileHandler

# ============================================
# LOGGING SYSTEM
# ============================================
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[96m', 'INFO': '\033[92m', 'WARNING': '\033[93m',
        'ERROR': '\033[91m', 'CRITICAL': '\033[97;41m', 'RESET': '\033[0m'
    }
    def format(self, record):
        t = datetime.now().strftime('%H:%M:%S')
        c = self.COLORS.get(record.levelname, '')
        return f"{c}[{t}] {record.levelname}: {record.getMessage()}{self.COLORS['RESET']}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('HassanBot')
logger.handlers.clear()
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColoredFormatter())
logger.addHandler(console_handler)
file_handler = RotatingFileHandler('hassan_bot.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
logger.addHandler(file_handler)

# ============================================
# CONFIGURATION (BALANCED)
# ============================================
CONFIG = {
    # === API ===
    "BINANCE_PUBLIC_API": "https://api.binance.com",
    "BINANCE_WS_PUBLIC": "wss://stream.binance.com:9443/ws",
    
    # === TELEGRAM (FILL THESE) ===
    "TELEGRAM_BOT_TOKEN": "8294766234:AAFA7tsO7IPCLligqOiY_XNo9-rPy-m1chs",
    "SIGNAL_CHANNEL": "-1003881031372",

    # === SYMBOLS ===
    "SYMBOLS": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT"],
    "BASE_SYMBOL": "BTCUSDT",

    # === TIMEFRAMES ===
    "TIMEFRAMES": ["1m", "3m", "5m", "15m"],
    "CANDLE_LIMITS": {"1m": 150, "3m": 100, "5m": 100, "15m": 80},

    # === ENGINES PARAMS (BALANCED) ===
    "WHALE_MIN_VOLUME_USD": 15000,
    "WHALE_WINDOW_SECONDS": 120,
    "WHALE_MIN_TRADES": 2,
    "ORDERBOOK_IMBALANCE_THRESHOLD": 0.60,
    "ORDERBOOK_DEPTH_LIMIT": 20,
    "VOLUME_SPIKE_MULTIPLIER": 1.8,
    "LIQ_SWEEP_VOLUME_SPIKE": 1.6,
    "LIQ_SWEEP_WICK_RATIO": 0.70,
    "ORDERFLOW_WINDOW_SECONDS": 120,
    "MIN_DELTA_RATIO": 0.25,
    "TEMP_MIN_FOR_SCALP": 30,
    "TEMP_MAX_FOR_SCALP": 85,

    # === MOMENTUM (BALANCED) ===
    "MOMENTUM_LOOKBACK_CANDLES": 3,
    "MOMENTUM_ATR_MULTIPLIER": 1.5,
    "MOMENTUM_VOLUME_SPIKE_MULTIPLIER": 1.3,

    # === MARKET STRUCTURE ===
    "STRUCTURE_LOOKBACK_CANDLES": 50,
    "STRUCTURE_SWING_STRENGTH": 3,

    # === CORRELATION ===
    "CORRELATION_WINDOW": 20,
    "MAX_CORRELATION_DIVERGENCE": 0.25,

    # === ADAPTIVE LEARNING ===
    "LEARNING_WINDOW": 10,
    "DEFAULT_ENGINE_WEIGHTS": {
        "orderbook": 1.0, "whale": 1.3,
        "volume_spike": 1.2, "liquidity": 1.1,
        "rsi": 0.9, "pattern": 1.0,
        "orderflow": 1.3, "momentum": 1.5
    },

    # === ENTRY / EXIT (BALANCED) ===
    "MIN_CONFIDENCE": 70,
    "MAX_SPREAD_PCT": 0.05,
    "MIN_LIQUIDITY_USD": 2000000,
    "ATR_MULTIPLIER_SL": 0.7,
    "ATR_MULTIPLIER_TP1": 1.3,
    "ATR_MULTIPLIER_TP2": 2.0,
    "ATR_MULTIPLIER_TP3": 3.5,

    # === RISK & SIZE (BALANCED) ===
    "RISK_PER_TRADE": 0.004,
    "BASE_CAPITAL": 1000,
    "TP1_SIZE": 0.4, "TP2_SIZE": 0.35, "TP3_SIZE": 0.25,
    "MAX_SCALP_MINUTES": 10,
    "MAX_OPEN_TRADES": 2,
    "MAX_OPEN_PER_SYMBOL": 1,
    "COOLDOWN_SECONDS": 120,

    # === SMART EXIT ===
    "EXTENDED_DURATION": 15,
    "MIN_PROGRESS_PCT": 0.25,

    # === SAFEGUARDS ===
    "DAILY_LOSS_CIRCUIT_BREAKER": 2.5,
    "VOLATILITY_CIRCUIT_BREAKER": 2.0,
    "ATR_SPIKE_FILTER": 3.0,
    "TRAILING_ATR_MULTIPLIER": 0.4,
    "ACTIVE_HOURS_START": 7,
    "ACTIVE_HOURS_END": 22,
}

# ============================================
# DATA MODELS
# ============================================
@dataclass
class Candle:
    timestamp: datetime; open: float; high: float; low: float; close: float
    volume: float; symbol: str; timeframe: str; trades: int = 0

    def __post_init__(self):
        if self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)
        if self.high < self.low:
            self.high, self.low = self.low, self.high
        self.open = max(self.low, min(self.open, self.high))
        self.close = max(self.low, min(self.close, self.high))
        self.volume = max(0, self.volume)

    @property
    def body_size(self): return abs(self.close - self.open)
    @property
    def total_range(self): return self.high - self.low
    @property
    def upper_shadow(self): return self.high - max(self.open, self.close)
    @property
    def lower_shadow(self): return min(self.open, self.close) - self.low
    @property
    def is_bullish(self): return self.close > self.open

@dataclass
class ScalpSignal:
    id: str; symbol: str; direction: str; entry_price: float; stop_loss: float
    take_profit: List[float]; confidence: float; star_rating: str
    momentum_strength: str; whale_status: str; market_structure: str
    risk_reward: float; reason_summary: str; timestamp: datetime
    risk_percent: float; position_size: float = 0.0

@dataclass
class ActiveTrade:
    signal_id: str; symbol: str; direction: str; entry_price: float; stop_loss: float
    tp1: float; tp2: float; tp3: float; position_size: float; entry_time: datetime
    remaining_size: float; tp1_hit: bool = False; tp2_hit: bool = False
    status: str = "OPEN"; exit_price: float = 0.0; pnl_pct: float = 0.0

# ============================================
# MATH UTILS
# ============================================
class MathUtils:
    @staticmethod
    def mean(v): return sum(v)/len(v) if v else 0.0
    @staticmethod
    def std(v):
        if len(v)<2: return 0.0
        m=MathUtils.mean(v)
        return (sum((x-m)**2 for x in v)/(len(v)-1))**0.5
    @staticmethod
    def ema(v, p):
        if len(v)<p: return []
        k=2/(p+1); e=[v[0]]
        for i in range(1,len(v)): e.append(v[i]*k+e[-1]*(1-k))
        return e
    @staticmethod
    def rsi(prices, period=14):
        if len(prices)<period+1: return 50.0
        deltas=[prices[i]-prices[i-1] for i in range(1,len(prices))]
        gains=[d if d>0 else 0 for d in deltas]
        losses=[-d if d<0 else 0 for d in deltas]
        ag=MathUtils.mean(gains[:period]); al=MathUtils.mean(losses[:period])
        return 100-(100/(1+ag/al)) if al else 100.0
    @staticmethod
    def atr(highs, lows, closes, period=14):
        if len(highs)<period: return 0.0
        tr=[max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(highs))]
        return MathUtils.mean(tr[-period:])
    @staticmethod
    def correlation(x, y):
        if len(x)!=len(y) or len(x)<2: return 0.0
        mx=MathUtils.mean(x); my=MathUtils.mean(y)
        num=sum((x[i]-mx)*(y[i]-my) for i in range(len(x)))
        dx=(sum((xi-mx)**2 for xi in x))**0.5
        dy=(sum((yi-my)**2 for yi in y))**0.5
        return num/(dx*dy) if dx*dy else 0.0

# ============================================
# RATE LIMITER
# ============================================
class RateLimiter:
    def __init__(self):
        self.sem=asyncio.Semaphore(3)
        self.lock=asyncio.Lock()
        self.last=0.0
    async def acquire(self):
        await self.sem.acquire()
        async with self.lock:
            now=time.time()
            if self.last and now-self.last<0.3:
                await asyncio.sleep(0.3-(now-self.last))
            self.last=time.time()
    def release(self): self.sem.release()

def rate_limited(func):
    async def wrapper(self, *a, **kw):
        await self.rate_limiter.acquire()
        try: return await func(self, *a, **kw)
        finally: self.rate_limiter.release()
    return wrapper

# ============================================
# MARKET DATA HUB (WITH AUTO-RECONNECT)
# ============================================
class MarketDataHub:
    def __init__(self):
        self.base=CONFIG["BINANCE_PUBLIC_API"]; self.ws=CONFIG["BINANCE_WS_PUBLIC"]
        self.sess=None; self.ssl=ssl.create_default_context()
        self.rate_limiter=RateLimiter()
        self.candles={s:{tf:deque(maxlen=CONFIG["CANDLE_LIMITS"][tf]) for tf in CONFIG["TIMEFRAMES"]} for s in CONFIG["SYMBOLS"]}
        self.orderbooks={s:{"bids":[],"asks":[]} for s in CONFIG["SYMBOLS"]}
        self.trades={s:deque(maxlen=1000) for s in CONFIG["SYMBOLS"]}
        self.req=0; self.err=0

    async def ensure_session(self):
        try:
            if not self.sess or self.sess.closed:
                self.sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30, sock_read=15))
            else:
                async with self.sess.get(f"{self.base}/api/v3/ping", ssl=self.ssl) as r:
                    if r.status != 200:
                        await self.sess.close()
                        self.sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30, sock_read=15))
        except:
            try: await self.sess.close()
            except: pass
            self.sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30, sock_read=15))

    @rate_limited
    async def fetch(self, endpoint, params=None):
        await self.ensure_session()
        url=f"{self.base}{endpoint}"
        for attempt in range(3):
            try:
                self.req+=1
                async with self.sess.get(url, params=params, ssl=self.ssl) as r:
                    if r.status==200: return await r.json()
                    elif r.status==429: await asyncio.sleep(int(r.headers.get('Retry-After',3)))
                    elif r.status>=500: await asyncio.sleep(2*(attempt+1))
                    else: break
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                self.err+=1
                logger.warning(f"Fetch error {endpoint} (attempt {attempt+1}): {e}")
                await self.ensure_session()
                await asyncio.sleep(2*(attempt+1))
        return None

    async def price(self, sym):
        d=await self.fetch("/api/v3/ticker/price",{"symbol":sym})
        return float(d["price"]) if d and "price" in d else None

    async def spread(self, sym):
        d=await self.fetch("/api/v3/ticker/bookTicker",{"symbol":sym})
        if d:
            b=float(d["bidPrice"]); a=float(d["askPrice"])
            sp=(a-b)/b*100 if b else 0
            return {"bid":b,"ask":a,"spread_pct":sp}
        return {"bid":0,"ask":0,"spread_pct":100}

    async def orderbook(self, sym):
        d=await self.fetch("/api/v3/depth",{"symbol":sym,"limit":50})
        if d:
            self.orderbooks[sym]={
                "bids":[[float(p),float(q)] for p,q in d.get("bids",[])],
                "asks":[[float(p),float(q)] for p,q in d.get("asks",[])]
            }

    async def agg_trades(self, sym):
        d=await self.fetch("/api/v3/aggTrades",{"symbol":sym,"limit":100})
        if d:
            for t in reversed(d):
                self.trades[sym].append({
                    "price":float(t["p"]),"quantity":float(t["q"]),
                    "time":datetime.fromtimestamp(t["T"]/1000,tz=timezone.utc),
                    "is_buyer_maker":t["m"]
                })

    async def klines(self, sym, interval, limit=100):
        d=await self.fetch("/api/v3/klines",{"symbol":sym,"interval":interval,"limit":limit})
        res=[]
        if d:
            for k in d:
                try:
                    res.append(Candle(
                        timestamp=datetime.fromtimestamp(k[0]/1000,tz=timezone.utc),
                        open=float(k[1]),high=float(k[2]),low=float(k[3]),
                        close=float(k[4]),volume=float(k[5]),symbol=sym,
                        timeframe=interval,trades=k[8]
                    ))
                except: pass
        return res

    async def ws_klines(self, sym, interval):
        stream=f"{sym.lower()}@kline_{interval}"
        url=f"{self.ws}/{stream}"
        delay=1
        while True:
            try:
                async with connect(url, ssl=self.ssl, ping_interval=15, ping_timeout=8) as ws:
                    logger.info(f"✅ WS {sym} {interval}")
                    delay=1
                    while True:
                        try:
                            msg=await asyncio.wait_for(ws.recv(), timeout=20)
                            data=json.loads(msg)
                            if 'k' not in data: continue
                            k=data['k']
                            if not k.get('x'): continue
                            c=Candle(
                                timestamp=datetime.fromtimestamp(k['t']/1000,tz=timezone.utc),
                                open=float(k['o']),high=float(k['h']),low=float(k['l']),
                                close=float(k['c']),volume=float(k['v']),symbol=sym,
                                timeframe=interval,trades=k.get('n',0)
                            )
                            self.candles[sym][interval].append(c)
                            if interval=="1m": yield c
                        except asyncio.TimeoutError: continue
                        except Exception: break
            except Exception as e:
                logger.error(f"WS {sym} {interval}: {e}")
                await asyncio.sleep(delay)
                delay=min(delay*2,30)

# ============================================
# ENGINES
# ============================================
class OrderbookImbalanceEngine:
    def __init__(self, hub): self.hub=hub
    def analyze(self, sym):
        ob=self.hub.orderbooks[sym]
        bids=ob.get("bids",[]); asks=ob.get("asks",[])
        if not bids or not asks: return {"direction":"NEUTRAL","score":0}
        d=20; bv=sum(q for _,q in bids[:d]); av=sum(q for _,q in asks[:d])
        if bv+av==0: return {"direction":"NEUTRAL","score":0}
        ratio=bv/(bv+av)
        if ratio>0.60: return {"direction":"LONG","score":min(100,(ratio-0.5)*400)}
        if ratio<0.40: return {"direction":"SHORT","score":min(100,(0.5-ratio)*400)}
        return {"direction":"NEUTRAL","score":0}

class WhaleDetectorEngine:
    def __init__(self, hub): self.hub=hub
    def analyze(self, sym):
        trades=list(self.hub.trades[sym])
        if not trades: return {"direction":"NEUTRAL","score":0,"details":"ساکت"}
        cutoff=datetime.now(timezone.utc)-timedelta(seconds=120)
        large=[t for t in trades if t["time"]>=cutoff and t["price"]*t["quantity"]>=15000]
        if len(large)<2: return {"direction":"NEUTRAL","score":0,"details":"ساکت"}
        total=sum(t["price"]*t["quantity"] for t in large)
        buy=sum(t["price"]*t["quantity"] for t in large if not t["is_buyer_maker"])
        if buy>total*0.6: return {"direction":"LONG","score":min(100,len(large)*20),"details":"خرید سنگین 🐋"}
        if buy<total*0.4: return {"direction":"SHORT","score":min(100,len(large)*20),"details":"فروش سنگین 🐋"}
        return {"direction":"NEUTRAL","score":min(50,len(large)*10),"details":"فعال متعادل"}

class MomentumBreakoutEngine:
    @staticmethod
    def analyze(candles):
        n=3
        if len(candles)<n+10: return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        recent=candles[-n:]
        start=recent[0].open; end=recent[-1].close
        change=end-start
        atr=MathUtils.atr([c.high for c in candles],[c.low for c in candles],[c.close for c in candles])
        if atr==0: return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        ratio=abs(change)/(atr*1.5)
        if ratio<1: return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        direction="LONG" if change>0 else "SHORT"
        avg_vol_recent=MathUtils.mean([c.volume for c in recent])
        avg_vol_hist=MathUtils.mean([c.volume for c in candles[-(n+20):-n]]) or 1
        if avg_vol_recent<avg_vol_hist*1.3:
            return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        if direction=="LONG" and sum(1 for c in recent if c.is_bullish)<2:
            return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        if direction=="SHORT" and sum(1 for c in recent if not c.is_bullish)<2:
            return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        score=min(100,ratio*35)
        if ratio<1.5: strength="ضعیف"
        elif ratio<2.5: strength="متوسط"
        elif ratio<4: strength="قوی"
        else: strength="انفجاری 🚀"
        return {"direction":direction,"score":score,"strength":strength}

class MarketStructureEngine:
    @staticmethod
    def analyze(candles):
        if len(candles)<50: return {"position":"نامشخص","bias":"NEUTRAL","score":0}
        highs=[c.high for c in candles]; lows=[c.low for c in candles]
        swings=[]
        for i in range(5, len(highs)-5):
            if highs[i]==max(highs[i-5:i+6]): swings.append(("HIGH", highs[i]))
            if lows[i]==min(lows[i-5:i+6]): swings.append(("LOW", lows[i]))
        if len(swings)<2: return {"position":"نامشخص","bias":"NEUTRAL","score":0}
        current=candles[-1].close
        last_high=max(s[1] for s in swings if s[0]=="HIGH") if any(s[0]=="HIGH" for s in swings) else current
        last_low=min(s[1] for s in swings if s[0]=="LOW") if any(s[0]=="LOW" for s in swings) else current
        if current<last_low*1.02 and current>last_low*0.98:
            return {"position":"کف ساختاری 🟢","bias":"LONG","score":80}
        if current>last_high*0.98 and current<last_high*1.02:
            return {"position":"سقف ساختاری 🔴","bias":"SHORT","score":80}
        if current>last_high*1.02:
            return {"position":"شکست صعودی 🚀","bias":"LONG","score":90}
        if current<last_low*0.98:
            return {"position":"شکست نزولی 💥","bias":"SHORT","score":90}
        return {"position":"میانه روند ↔️","bias":"NEUTRAL","score":30}

class OrderflowDeltaEngine:
    def __init__(self, hub): self.hub=hub
    def analyze(self, sym, direction):
        trades=list(self.hub.trades[sym])
        if not trades: return {"direction":"NEUTRAL","score":0}
        cutoff=datetime.now(timezone.utc)-timedelta(seconds=120)
        recent=[t for t in trades if t["time"]>=cutoff]
        if not recent: return {"direction":"NEUTRAL","score":0}
        buy=sum(t["price"]*t["quantity"] for t in recent if not t["is_buyer_maker"])
        sell=sum(t["price"]*t["quantity"] for t in recent if t["is_buyer_maker"])
        total=buy+sell
        if total==0: return {"direction":"NEUTRAL","score":0}
        delta=(buy-sell)/total
        if direction=="LONG" and delta>0.25: return {"direction":"LONG","score":min(100,delta*120)}
        if direction=="SHORT" and delta<-0.25: return {"direction":"SHORT","score":min(100,-delta*120)}
        return {"direction":"NEUTRAL","score":0}

class CorrelationFilter:
    def __init__(self, hub): self.hub=hub
    def check(self, sym, signal_direction):
        if sym==CONFIG["BASE_SYMBOL"]: return True
        btc_candles=list(self.hub.candles[CONFIG["BASE_SYMBOL"]]["1m"])
        sym_candles=list(self.hub.candles[sym]["1m"])
        if len(btc_candles)<20 or len(sym_candles)<20: return True
        btc_returns=[(btc_candles[i].close-btc_candles[i-1].close)/btc_candles[i-1].close for i in range(-20,0)]
        sym_returns=[(sym_candles[i].close-sym_candles[i-1].close)/sym_candles[i-1].close for i in range(-20,0)]
        corr=MathUtils.correlation(btc_returns, sym_returns)
        btc_direction="LONG" if btc_returns[-1]>0 else "SHORT"
        if corr>0.5 and signal_direction!=btc_direction:
            return False
        return True

class AdaptiveWeightEngine:
    def __init__(self):
        self.weights=CONFIG["DEFAULT_ENGINE_WEIGHTS"].copy()
        self.recent_performance=defaultdict(list)
    def update(self, engine_name, pnl):
        self.recent_performance[engine_name].append(pnl)
        if len(self.recent_performance[engine_name])>10:
            self.recent_performance[engine_name].pop(0)
        if len(self.recent_performance[engine_name])>=5:
            avg=MathUtils.mean(self.recent_performance[engine_name])
            self.weights[engine_name]=max(0.5, min(2.5, 1.0+avg*10))
    def get_weight(self, engine_name):
        return self.weights.get(engine_name, 1.0)

# ============================================
# CIRCUIT BREAKER
# ============================================
class CircuitBreaker:
    def __init__(self):
        self.daily_pnl=0.0
        self.last_reset=datetime.now(timezone.utc).date()
        self.is_triggered=False
    def check(self, candles_5m):
        today=datetime.now(timezone.utc).date()
        if today!=self.last_reset:
            self.daily_pnl=0.0; self.last_reset=today; self.is_triggered=False
        if self.daily_pnl<=-2.5:
            self.is_triggered=True; return True
        if len(candles_5m)>=6:
            change=abs(candles_5m[-1].close-candles_5m[-6].close)/candles_5m[-6].close*100
            if change>=2.0:
                self.is_triggered=True; return True
        return False
    def add_pnl(self, pnl): self.daily_pnl+=pnl

# ============================================
# ULTIMATE SIGNAL GENERATOR
# ============================================
class UltimateSignalGenerator:
    def __init__(self, hub, breaker):
        self.hub=hub; self.breaker=breaker
        self.ob=OrderbookImbalanceEngine(hub)
        self.wh=WhaleDetectorEngine(hub)
        self.mom=MomentumBreakoutEngine()
        self.ms=MarketStructureEngine()
        self.delta=OrderflowDeltaEngine(hub)
        self.corr=CorrelationFilter(hub)
        self.weights=AdaptiveWeightEngine()
        self.last_sig={}; self.daily_trades=0

    def _star_rating(self, confidence):
        if confidence>=85: return "⭐⭐⭐⭐⭐"
        if confidence>=78: return "⭐⭐⭐⭐"
        if confidence>=70: return "⭐⭐⭐"
        return "⭐⭐"

    async def generate(self, sym):
        now=datetime.now(timezone.utc)
        if not (7<=now.hour<22): return None
        c5m=list(self.hub.candles[sym]["5m"])
        if self.breaker.check(c5m):
            logger.warning(f"⛔ Circuit Breaker Active for {sym}")
            return None
        if sym in self.last_sig and (now-self.last_sig[sym]).total_seconds()<120:
            return None
        if self.daily_trades>=25: return None

        sp=await self.hub.spread(sym)
        if sp["spread_pct"]>0.05: return None
        await self.hub.orderbook(sym); await self.hub.agg_trades(sym)

        c1m=list(self.hub.candles[sym]["1m"])
        if len(c1m)<20 or len(c5m)<10: return None

        ob_r=self.ob.analyze(sym); wh_r=self.wh.analyze(sym)
        mom_r=self.mom.analyze(c5m); ms_r=self.ms.analyze(c5m)

        pre=[("orderbook",ob_r),("whale",wh_r),("momentum",mom_r),("structure",ms_r)]
        lv=sum(1 for _,r in pre if r.get("direction")=="LONG")
        sv=sum(1 for _,r in pre if r.get("direction")=="SHORT")
        init="LONG" if lv>sv else "SHORT" if sv>lv else "NEUTRAL"
        delta_r=self.delta.analyze(sym, init)
        engines=pre+[("orderflow",delta_r)]

        total_score=0; total_weight=0
        for name, r in engines:
            d=r.get("direction","NEUTRAL"); sc=r.get("score",0)
            w=self.weights.get_weight(name)
            if d=="LONG": total_score+=sc*w; total_weight+=w
            elif d=="SHORT": total_score-=sc*w; total_weight+=w

        if total_weight==0: return None
        direction="LONG" if total_score>0 else "SHORT"
        confidence=min(95, abs(total_score/total_weight)*0.85)

        if not self.corr.check(sym, direction):
            confidence*=0.7

        if confidence<70: return None

        price=await self.hub.price(sym)
        if not price: return None
        atr=MathUtils.atr([c.high for c in c5m],[c.low for c in c5m],[c.close for c in c5m])
        if atr==0: atr=price*0.002

        if direction=="LONG":
            sl=price-atr*0.7
            tp1=price+atr*1.3; tp2=price+atr*2.0; tp3=price+atr*3.5
        else:
            sl=price+atr*0.7
            tp1=price-atr*1.3; tp2=price-atr*2.0; tp3=price-atr*3.5

        rr=abs(tp1-price)/abs(price-sl) if price!=sl else 0
        pos_size=(1000*0.004)/abs(price-sl) if price!=sl else 0

        sig=ScalpSignal(
            id=f"{sym}_{now.strftime('%H%M%S')}_{random.randint(100,999)}",
            symbol=sym, direction=direction, entry_price=price, stop_loss=sl,
            take_profit=[tp1,tp2,tp3], confidence=confidence,
            star_rating=self._star_rating(confidence),
            momentum_strength=mom_r.get("strength","نامشخص"),
            whale_status=wh_r.get("details","نامشخص"),
            market_structure=ms_r.get("position","نامشخص"),
            risk_reward=round(rr,2),
            reason_summary=f"{mom_r.get('strength','')} | {ms_r.get('position','')} | نهنگ:{wh_r.get('details','')}",
            timestamp=now, risk_percent=0.004, position_size=pos_size
        )
        self.last_sig[sym]=now; self.daily_trades+=1
        logger.info(f"🎯 SIGNAL: {sym} {direction} | Confidence:{confidence:.0f}% | {sig.star_rating}")
        return sig

    def report_result(self, engine_scores, pnl):
        for name in engine_scores:
            self.weights.update(name, pnl)

# ============================================
# 🧠 SMART TRADE MANAGER (NEW - NO DELETIONS)
# ============================================
class TradeManager:
    def __init__(self, hub, breaker):
        self.hub=hub; self.breaker=breaker
        self.active={}; self.closed=[]
        self.sym_count={}
        # Smart exit settings
        self.max_duration=CONFIG["MAX_SCALP_MINUTES"]  # 10 min
        self.extended_duration=CONFIG["EXTENDED_DURATION"]  # 15 min
        self.min_progress_pct=CONFIG["MIN_PROGRESS_PCT"]  # 25%

    def open(self, sig):
        if len(self.active)>=CONFIG["MAX_OPEN_TRADES"] or self.sym_count.get(sig.symbol,0)>=CONFIG["MAX_OPEN_PER_SYMBOL"]:
            return None
        t=ActiveTrade(
            signal_id=sig.id, symbol=sig.symbol, direction=sig.direction,
            entry_price=sig.entry_price, stop_loss=sig.stop_loss,
            tp1=sig.take_profit[0], tp2=sig.take_profit[1], tp3=sig.take_profit[2],
            position_size=sig.position_size, entry_time=sig.timestamp,
            remaining_size=sig.position_size
        )
        self.active[sig.id]=t
        self.sym_count[sig.symbol]=self.sym_count.get(sig.symbol,0)+1
        return t

    async def monitor(self, cb=None):
        while True:
            await asyncio.sleep(5)
            closed=[]
            
            for tid,t in list(self.active.items()):
                try:
                    p=await self.hub.price(t.symbol)
                    if not p: continue
                    
                    dur=(datetime.now(timezone.utc)-t.entry_time).total_seconds()/60
                    
                    # ====== SMART EXIT LOGIC ======
                    if t.direction=="LONG":
                        progress_to_tp = (p - t.entry_price) / (t.tp1 - t.entry_price) if t.tp1 != t.entry_price else 0
                        is_favorable = p > t.entry_price
                        is_near_entry = abs(p - t.entry_price) / t.entry_price < 0.001
                    else:
                        progress_to_tp = (t.entry_price - p) / (t.entry_price - t.tp1) if t.entry_price != t.tp1 else 0
                        is_favorable = p < t.entry_price
                        is_near_entry = abs(p - t.entry_price) / t.entry_price < 0.001
                    
                    should_close = False
                    close_reason = ""
                    
                    # ۱. Check SL (always priority)
                    if (t.direction=="LONG" and p<=t.stop_loss) or (t.direction=="SHORT" and p>=t.stop_loss):
                        should_close = True
                        close_reason = "STOP_LOSS"
                    
                    # ۲. Smart time exit
                    elif dur >= self.max_duration:
                        if progress_to_tp >= self.min_progress_pct and is_favorable:
                            # Moving toward TP1 → extend time
                            if dur < self.extended_duration:
                                should_close = False
                            else:
                                should_close = True
                                close_reason = "TIME_EXIT_EXTENDED"
                        elif is_near_entry:
                            # Near entry → give more time
                            if dur < self.extended_duration:
                                should_close = False
                            else:
                                should_close = True
                                close_reason = "TIME_EXIT"
                        else:
                            # Going against or no progress → close
                            should_close = True
                            close_reason = "TIME_EXIT"
                    
                    if should_close and close_reason:
                        t.status=close_reason
                        t.exit_price=p
                        t.pnl_pct=(p-t.entry_price)/t.entry_price*100 if t.direction=="LONG" else (t.entry_price-p)/t.entry_price*100
                        closed.append(tid)
                        continue
                    
                    # ====== TRAILING STOP ======
                    if t.tp2_hit:
                        c5=list(self.hub.candles[t.symbol]["5m"])
                        if len(c5)>=14:
                            atr=MathUtils.atr([c.high for c in c5],[c.low for c in c5],[c.close for c in c5])
                            if t.direction=="LONG":
                                ns=p-atr*CONFIG["TRAILING_ATR_MULTIPLIER"]
                                if ns>t.stop_loss: t.stop_loss=ns
                            else:
                                ns=p+atr*CONFIG["TRAILING_ATR_MULTIPLIER"]
                                if ns<t.stop_loss: t.stop_loss=ns
                    
                    # ====== CHECK TPs ======
                    if not t.tp1_hit:
                        if (t.direction=="LONG" and p>=t.tp1) or (t.direction=="SHORT" and p<=t.tp1):
                            t.tp1_hit=True
                            t.remaining_size*=(1-CONFIG["TP1_SIZE"])
                            if cb: asyncio.create_task(cb(t,"TP1"))
                            logger.info(f"✅ TP1 HIT: {t.symbol} @ {p:.4f}")
                    elif not t.tp2_hit:
                        if (t.direction=="LONG" and p>=t.tp2) or (t.direction=="SHORT" and p<=t.tp2):
                            t.tp2_hit=True
                            t.remaining_size*=(1-CONFIG["TP2_SIZE"])
                            if cb: asyncio.create_task(cb(t,"TP2"))
                            logger.info(f"✅✅ TP2 HIT: {t.symbol} @ {p:.4f}")
                    elif t.tp2_hit:
                        if (t.direction=="LONG" and p>=t.tp3) or (t.direction=="SHORT" and p<=t.tp3):
                            t.status="TP3_FULL"
                            t.exit_price=p
                            t.pnl_pct=(p-t.entry_price)/t.entry_price*100 if t.direction=="LONG" else (t.entry_price-p)/t.entry_price*100
                            closed.append(tid)
                            logger.info(f"🏁 TP3 FULL: {t.symbol} @ {p:.4f} | PnL: {t.pnl_pct:.2f}%")
                            
                except Exception as e:
                    logger.error(f"Monitor error {tid}: {traceback.format_exc()}")
            
            for tid in closed:
                t=self.active.pop(tid)
                self.closed.append(t)
                self.sym_count[t.symbol]=max(0,self.sym_count.get(t.symbol,1)-1)
                self.breaker.add_pnl(t.pnl_pct)
                if cb:
                    try: asyncio.create_task(cb(t,t.status))
                    except: pass

# ============================================
# TELEGRAM BOT
# ============================================
try:
    from telegram import Bot, ReplyKeyboardMarkup
    from telegram.ext import Application, CommandHandler, MessageHandler, filters
    from telegram.constants import ParseMode
    TELE=True
except:
    TELE=False
    logger.warning("python-telegram-bot not installed. Telegram disabled.")

if TELE:
    class TelegramBot:
        def __init__(self, token, channel, hub, tm, gen, breaker):
            self.bot=Bot(token=token); self.channel=channel; self.hub=hub
            self.tm=tm; self.gen=gen; self.breaker=breaker
            self.app=Application.builder().token(token).build()
            self.app.add_handler(CommandHandler("start", self._start))
            self.app.add_handler(CommandHandler("scalp", self._scalp))
            self.app.add_handler(CommandHandler("health", self._health))
            self.app.add_handler(CommandHandler("trades", self._trades))
            self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._btn))

        async def startup(self):
            try:
                await self.bot.send_message(
                    self.channel,
                    "🚀 *ربات فوق‌پیشرفته حسن فعال شد*\n"
                    "✅ خروج هوشمند (تمدید در صورت حرکت)\n"
                    "✅ تنظیمات متعادل\n"
                    "✅ سیگنال‌های باکیفیت",
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.info("✅ Startup message sent to Telegram")
            except Exception as e:
                logger.error(f"Startup failed: {e}")

        async def _start(self, update, context):
            kb=[
                ["⚡ تحلیل اسکالپ", "📊 سلامت ربات"],
                ["📋 معاملات باز", "📈 گزارش روزانه"]
            ]
            await update.message.reply_text(
                "🤖 *ربات اسکالپ حسن*\nخروج هوشمند + سیگنال‌های طلایی",
                reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True),
                parse_mode=ParseMode.MARKDOWN
            )

        async def _btn(self, update, context):
            t=update.message.text
            if "اسکالپ" in t: await self._scalp(update, context)
            elif "سلامت" in t: await self._health(update, context)
            elif "معاملات" in t: await self._trades(update, context)
            elif "گزارش" in t: await self._daily_report(update, context)

        async def _scalp(self, update, context):
            try:
                sym="SOLUSDT"
                p=None
                for attempt in range(3):
                    try:
                        p=await self.hub.price(sym)
                        if p: break
                        await asyncio.sleep(1)
                    except: await asyncio.sleep(1)

                if not p:
                    await update.message.reply_text(
                        "⚠️ *مشکل در دریافت قیمت*\n\n"
                        "• اینترنت رو چک کن\n"
                        "• ۳۰ ثانیه صبر کن\n"
                        "• اگه حل نشد، ربات رو ری‌استارت کن",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return

                c5=list(self.hub.candles[sym]["5m"])
                if len(c5)<20:
                    await update.message.reply_text(
                        "⏳ *کندل ناکافی*\nربات در حال جمع‌آوری داده‌هاست.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return

                rsi=MathUtils.rsi([c.close for c in c5])
                atr=MathUtils.atr([c.high for c in c5],[c.low for c in c5],[c.close for c in c5])
                mom=MomentumBreakoutEngine.analyze(c5)
                ms=MarketStructureEngine.analyze(c5)

                trades=list(self.hub.trades.get(sym,[]))
                whale_active=any(
                    t["time"]>=datetime.now(timezone.utc)-timedelta(seconds=120)
                    and t["price"]*t["quantity"]>=15000
                    for t in trades
                ) if trades else False

                score=0
                if mom["direction"]!="NEUTRAL": score+=35
                if ms["bias"]!="NEUTRAL": score+=25
                if 40<rsi<60: score+=15
                if whale_active: score+=25

                if score>=70: status="🟢 *عالی*"
                elif score>=50: status="🟡 *متوسط*"
                else: status="🔴 *ضعیف*"

                msg=(
                    f"📊 *تحلیل اسکالپ {sym}*\n"
                    f"{'─'*30}\n"
                    f"💰 *قیمت:* {p:.4f}\n"
                    f"📈 *RSI:* {rsi:.1f}\n"
                    f"📊 *ATR:* {atr:.4f}\n"
                    f"🚀 *مومنتوم:* {mom.get('strength','نامشخص')}\n"
                    f"🏗 *ساختار:* {ms.get('position','نامشخص')}\n"
                    f"🐋 *نهنگ:* {'فعال 🐋' if whale_active else 'ساکت 💤'}\n"
                    f"{'─'*30}\n"
                    f"⭐ *امتیاز:* {score}/100\n"
                    f"{status}"
                )
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

            except Exception as e:
                logger.error(f"Scalp analysis error: {traceback.format_exc()}")
                await update.message.reply_text("❌ خطای غیرمنتظره. لطفاً دوباره تلاش کن.")

        async def _health(self, update, context):
            cb="⛔ فعال" if self.breaker.is_triggered else "✅ غیرفعال"
            active=len(self.tm.active)
            closed=len(self.tm.closed)
            today=datetime.now(timezone.utc).date()
            day_trades=[t for t in self.tm.closed if t.entry_time.date()==today]
            total_pnl=sum(t.pnl_pct for t in day_trades)
            wins=[t for t in day_trades if t.pnl_pct>0]
            wr=len(wins)/len(day_trades)*100 if day_trades else 0

            msg=(
                f"❤️ *سلامت ربات*\n"
                f"{'─'*30}\n"
                f"🔌 مدارشکن: {cb}\n"
                f"📊 درخواست‌ها: {self.hub.req}\n"
                f"⚠️ خطاها: {self.hub.err}\n"
                f"📈 معاملات باز: {active}\n"
                f"📋 معاملات امروز: {len(day_trades)}\n"
                f"✅ نرخ برد: {wr:.0f}%\n"
                f"💰 سود/ضرر امروز: {total_pnl:.2f}%"
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

        async def _trades(self, update, context):
            if not self.tm.active:
                await update.message.reply_text("📋 هیچ معامله‌ی بازی وجود ندارد.")
                return
            lines=[]
            for t in self.tm.active.values():
                p=await self.hub.price(t.symbol)
                if p:
                    pnl=(p-t.entry_price)/t.entry_price*100 if t.direction=="LONG" else (t.entry_price-p)/t.entry_price*100
                    lines.append(f"• {t.symbol} {t.direction} | ورود:{t.entry_price:.4f} | PnL:{pnl:+.2f}%")
            await update.message.reply_text("📋 *معاملات باز:*\n"+"\n".join(lines), parse_mode=ParseMode.MARKDOWN)

        async def _daily_report(self, update, context):
            today=datetime.now(timezone.utc).date()
            day_trades=[t for t in self.tm.closed if t.entry_time.date()==today]
            if not day_trades:
                await update.message.reply_text("📆 امروز هنوز معامله‌ای بسته نشده.")
                return
            wins=[t for t in day_trades if t.pnl_pct>0]
            losses=[t for t in day_trades if t.pnl_pct<=0]
            total_pnl=sum(t.pnl_pct for t in day_trades)
            msg=(
                f"📆 *گزارش امروز*\n"
                f"{'─'*30}\n"
                f"📊 تعداد: {len(day_trades)}\n"
                f"✅ برد: {len(wins)}\n"
                f"❌ باخت: {len(losses)}\n"
                f"🎯 نرخ برد: {len(wins)/len(day_trades)*100:.0f}%\n"
                f"💰 سود/ضرر: {total_pnl:+.2f}%"
            )
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

        async def send_signal(self, sig):
            em="🟢" if sig.direction=="LONG" else "🔴"
            msg=(
                f"{em} *سیگنال اسکالپ {sig.symbol}*\n"
                f"{'─'*30}\n"
                f"⭐ *اطمینان:* {sig.star_rating} ({sig.confidence:.0f}%)\n"
                f"🎯 *ورود:* {sig.entry_price:.4f}\n"
                f"🛑 *SL:* {sig.stop_loss:.4f}\n"
                f"✅ *TP1:* {sig.take_profit[0]:.4f}\n"
                f"✅ *TP2:* {sig.take_profit[1]:.4f}\n"
                f"✅ *TP3:* {sig.take_profit[2]:.4f}\n"
                f"🚀 *قدرت حرکت:* {sig.momentum_strength}\n"
                f"🐋 *نهنگ:* {sig.whale_status}\n"
                f"🏗 *ساختار:* {sig.market_structure}\n"
                f"📊 *R:R = 1:{sig.risk_reward}*\n"
                f"📝 {sig.reason_summary}"
            )
            try:
                await self.bot.send_message(self.channel, text=msg, parse_mode=ParseMode.MARKDOWN)
                logger.info(f"Signal sent: {sig.symbol} {sig.direction}")
            except Exception as e:
                logger.error(f"Send signal error: {e}")

        async def trade_update(self, trade, event):
            emojis={"TP1":"✅","TP2":"✅✅","TP3_FULL":"🏁","STOP_LOSS":"🛑","TIME_EXIT":"⏰","TIME_EXIT_EXTENDED":"⏰🔄"}
            em=emojis.get(event,"📊")
            txt=f"{em} {event} {trade.symbol} | PnL: {trade.pnl_pct:+.2f}%"
            try: await self.bot.send_message(self.channel, text=txt)
            except: pass

        async def start_polling(self):
            await self.app.initialize()
            await self.app.start()
            await self.app.updater.start_polling()
            logger.info("✅ Telegram polling started")

        async def stop_polling(self):
            try:
                await self.app.updater.stop()
                await self.app.stop()
                await self.app.shutdown()
            except: pass
else:
    class TelegramBot:
        def __init__(self,*a,**kw): pass
        async def startup(self): logger.info("Telegram not available")
        async def send_signal(self,*a): pass
        async def trade_update(self,*a): pass
        async def start_polling(self): pass
        async def stop_polling(self): pass

# ============================================
# MAIN BOT
# ============================================
class HassanBot:
    def __init__(self):
        self.hub=MarketDataHub()
        self.breaker=CircuitBreaker()
        self.gen=UltimateSignalGenerator(self.hub, self.breaker)
        self.tm=TradeManager(self.hub, self.breaker)
        self.tele=TelegramBot(
            CONFIG["TELEGRAM_BOT_TOKEN"],
            CONFIG["SIGNAL_CHANNEL"],
            self.hub, self.tm, self.gen, self.breaker
        )

    async def initialize_data(self):
        logger.info("📊 Loading historical data...")
        for sym in CONFIG["SYMBOLS"]:
            logger.info(f"  Loading {sym}...")
            for tf in CONFIG["TIMEFRAMES"]:
                try:
                    candles=await self.hub.klines(sym, tf, CONFIG["CANDLE_LIMITS"][tf])
                    if candles:
                        self.hub.candles[sym][tf].extend(candles)
                        logger.info(f"    {tf}: {len(candles)} candles")
                except Exception as e:
                    logger.error(f"    Failed to load {sym} {tf}: {e}")
        logger.info("✅ Historical data loaded!")

    async def run_engine(self):
        locks={s:asyncio.Lock() for s in CONFIG["SYMBOLS"]}

        async def analyze(sym):
            async with locks[sym]:
                try:
                    if self.breaker.is_triggered: return
                    sig=await self.gen.generate(sym)
                    if sig:
                        trade=self.tm.open(sig)
                        if trade:
                            await self.tele.send_signal(sig)
                except Exception as e:
                    logger.error(f"Analyze error {sym}: {traceback.format_exc()}")

        async def ws_loop(sym, tf):
            try:
                async for _ in self.hub.ws_klines(sym, tf):
                    if tf=="1m" and not locks[sym].locked():
                        asyncio.create_task(analyze(sym))
            except Exception as e:
                logger.error(f"WS loop error {sym} {tf}: {e}")

        async def rest_loop():
            while True:
                for sym in CONFIG["SYMBOLS"]:
                    try:
                        await asyncio.gather(
                            self.hub.orderbook(sym),
                            self.hub.agg_trades(sym),
                            return_exceptions=True
                        )
                    except: pass
                await asyncio.sleep(1)

        async def periodic_loop():
            while True:
                for sym in CONFIG["SYMBOLS"]:
                    if not locks[sym].locked():
                        asyncio.create_task(analyze(sym))
                await asyncio.sleep(10)

        tasks=[
            *[asyncio.create_task(ws_loop(s,tf)) for s in CONFIG["SYMBOLS"] for tf in CONFIG["TIMEFRAMES"]],
            asyncio.create_task(rest_loop()),
            asyncio.create_task(periodic_loop()),
            asyncio.create_task(self.tm.monitor(self.tele.trade_update))
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Engine shutting down...")
        except Exception as e:
            logger.error(f"Engine fatal error: {traceback.format_exc()}")

    async def start(self):
        logger.info("="*50)
        logger.info("🥇 HASSAN ULTIMATE SCALP BOT V2")
        logger.info("="*50)
        logger.info(f"Symbols: {', '.join(CONFIG['SYMBOLS'])}")
        logger.info(f"Min Confidence: {CONFIG['MIN_CONFIDENCE']}%")
        logger.info(f"Max Open Trades: {CONFIG['MAX_OPEN_TRADES']}")
        logger.info(f"Risk/Trade: {CONFIG['RISK_PER_TRADE']*100:.1f}%")
        logger.info(f"Smart Exit: {CONFIG['MAX_SCALP_MINUTES']}min → {CONFIG['EXTENDED_DURATION']}min if progressing")
        logger.info(f"Active Hours: {CONFIG['ACTIVE_HOURS_START']}-{CONFIG['ACTIVE_HOURS_END']} UTC")
        logger.info("="*50)

        await self.initialize_data()
        await self.tele.start_polling()
        await self.tele.startup()
        await self.run_engine()

async def main():
    bot=HassanBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {traceback.format_exc()}")

if __name__=="__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped.")
    except Exception as e:
        print(f"Error: {e}")
