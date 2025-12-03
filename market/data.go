import os
import re
import time
import threading
from typing import Dict, List, Optional
import asyncio
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from telethon import TelegramClient, events
from dotenv import load_dotenv
import uvicorn
from datetime import datetime, timedelta, timezone
import requests  # ⬅️ 新增：调用 Binance API 用

# ===========================
# 1. 配置（从 .env 读取）
# ===========================
load_dotenv()

API_ID = int(os.getenv("TG_API_ID", "123456"))        # my.telegram.org 获取
API_HASH = os.getenv("TG_API_HASH", "your_api_hash")
SESSION_NAME = os.getenv("TG_SESSION_NAME", "oi_relay")

# 原来的 OI 频道
TARGET_CHAT = os.getenv("TG_TARGET_CHAT", "方程式-OI&价格异动（抓庄神器）")

# CoinGlass Bot 对话名或 @username，例如 "CoinGlass Bot"
COINGLASS_BOT_CHAT = os.getenv("TG_COINGLASS_BOT", "CoinGlass Bot")

HTTP_PORT = int(os.getenv("OI_RELAY_PORT", "8000"))

BINANCE_FAPI_BASE = "https://fapi.binance.com"

# ===========================
# 2. OI 数据内存存储
# ===========================
class OIStore:
    def __init__(self):
        self._positions: Dict[str, dict] = {}
        self._lock = threading.Lock()

    def update_from_signal(
        self,
        symbol: str,
        oi_delta_percent: float,
        price_delta_percent: float,
        current_oi_million: float,
        marketcap_million: float,
    ):
        symbol = symbol.upper()
        now = int(time.time())

        with self._lock:
            self._positions[symbol] = {
                "symbol": symbol,
                "oi_delta_percent": oi_delta_percent,
                "price_delta_percent": price_delta_percent,
                "current_oi": current_oi_million,
                "oi_delta": 0.0,
                "oi_delta_value": current_oi_million,
                "marketcap": marketcap_million,
                "ts": now,
            }

        print(
            f"[STORE] {symbol}: "
            f"OI {oi_delta_percent}%, Price {price_delta_percent}%, "
            f"OI ${current_oi_million}M, MC ${marketcap_million}M"
        )

    def prune_old(self, max_age_seconds: int = 7200):
        now = int(time.time())
        removed = 0
        with self._lock:
            to_delete = [
                sym for sym, p in self._positions.items()
                if now - p.get("ts", 0) > max_age_seconds
            ]
            for sym in to_delete:
                del self._positions[sym]
                removed += 1
        if removed > 0:
            print(f"[PRUNE] Removed {removed} stale positions (> {max_age_seconds} s old)")

    def export_oitop_response(self, limit: int = 20) -> dict:
        """
        导出 /oi_top：

        1）按 |oi_delta_percent| 排序
        2）对每个 symbol：
            - 用 Binance Futures fapi/v1/klines 检查是否存在 USDT 永续
            - 若存在：计算过去 15min 价格涨跌百分比，写入 price_delta_percent
            - 若不存在：跳过，不返回给 AI Trader
        """
        # 先在锁内拷贝一份快照，避免长时间持锁进行网络请求
        with self._lock:
            positions_snapshot: List[dict] = list(self._positions.values())

        # 排序并截前 limit 个
        sorted_positions: List[dict] = sorted(
            positions_snapshot,
            key=lambda x: abs(x["oi_delta_percent"]),
            reverse=True,
        )[:limit * 2]  # 多抓一点，后面还要过滤非 binance futures

        result = []
        for p in sorted_positions:
            symbol = p["symbol"]
            price_delta_15m = fetch_binance_15m_price_change(symbol)

            # None 说明：要么不是 Binance Futures，要么 15m 数据异常，直接丢弃
            if price_delta_15m is None:
                print(f"[BINANCE] Skip {symbol}: not futures or no 15m data")
                continue

            rank = len(result) + 1
            result.append({
                "symbol": symbol,
                "rank": rank,
                "current_oi": p["current_oi"],
                "oi_delta": p["oi_delta"],
                "oi_delta_percent": p["oi_delta_percent"],
                "oi_delta_value": p["oi_delta_value"],
                "price_delta_percent": price_delta_15m,
                "net_long": 0.0,
                "net_short": 0.0,
            })

            if len(result) >= limit:
                break

        return {
            "success": True,
            "data": {
                "positions": result,
                "count": len(result),
                "exchange": "binance_futures",
                "time_range": "900s",  # 15分钟
            },
        }

oi_store = OIStore()

# 市值缓存：原频道的 "$XXX MarketCap: $123M"
mcap_buffer: Dict[str, float] = {}

# ===========================
# Binance 辅助函数
# ===========================

def normalize_symbol(symbol: str) -> str:
    """
    模仿你 Go 里的 Normalize:
    - 已经是 XXXUSDT 的就不动
    - 否则补上 USDT
    """
    symbol = symbol.upper()
    if symbol.endswith("USDT"):
        return symbol
    return symbol + "USDT"

def fetch_binance_15m_price_change(symbol: str) -> Optional[float]:
    """
    用 Binance Futures 1m K 线计算过去 15m 价格涨跌百分比：
    - 不存在该合约 / 接口报错 → 返回 None
    - 有数据 → 返回百分比（float）
    """
    norm_symbol = normalize_symbol(symbol)
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/klines"
    params = {
        "symbol": norm_symbol,
        "interval": "1m",
        "limit": 15,  # 最近 15 根 1mK
    }

    try:
        resp = requests.get(url, params=params, timeout=3)
    except Exception as e:
        print(f"[BINANCE] Request error for {norm_symbol}: {e}")
        return None

    if resp.status_code != 200:
        # 例如 {"code":-1121,"msg":"Invalid symbol."}
        print(f"[BINANCE] HTTP {resp.status_code} for {norm_symbol}: {resp.text}")
        return None

    try:
        data = resp.json()
    except Exception as e:
        print(f"[BINANCE] JSON decode error for {norm_symbol}: {e}")
        return None

    # 不存在合约时，Binance 会返回一个对象，而不是数组
    if not isinstance(data, list) or len(data) < 2:
        print(f"[BINANCE] No kline data or not list for {norm_symbol}: {data}")
        return None

    try:
        first_close = float(data[0][4])   # [4] = close
        last_close = float(data[-1][4])
    except Exception as e:
        print(f"[BINANCE] Parse kline close error for {norm_symbol}: {e}")
        return None

    if first_close <= 0:
        return None

    pct = (last_close - first_close) / first_close * 100.0
    print(f"[BINANCE] {norm_symbol} 15m price change = {pct:.2f}%")
    return pct

# ===========================
# 3. 原 OI 频道解析（🇺🇸 行 + 市值行）
# ===========================
EN_LINE_RE = re.compile(
    r"""
    ^🇺🇸\s*
    (?P<symbol>[A-Z0-9]+)
    .*?openinterest\s*(?P<oi_sign>[+\-])(?P<oi_pct>[\d\.]+)%,\s*
    Price\s*(?P<price_sign>[+\-])(?P<price_pct>[\d\.]+)%\s*in\s*the\s*past\s*3600\s*seconds,\s*
    OI:\s*\$(?P<oi_value>[\d\.]+)M,
    .*?24H\s*Price\s*Change:\s*(?P<day_sign>[+\-])(?P<day_pct>[\d\.]+)%
    """,
    re.VERBOSE
)

MCAP_LINE_RE = re.compile(
    r"""
    ^\$?(?P<symbol>[A-Z0-9]+)\s+MarketCap:\s*\$(?P<mcap>[\d\.]+)M
    """,
    re.VERBOSE
)

def _signed(sign: str, value: str) -> float:
    v = float(value)
    return v if sign == '+' else -v

def parse_en_line(text: str) -> Optional[dict]:
    m = EN_LINE_RE.search(text)
    if not m:
        return None
    return {
        "symbol": m.group("symbol"),
        "oi_delta_percent": _signed(m.group("oi_sign"), m.group("oi_pct")),
        "price_delta_percent": _signed(m.group("price_sign"), m.group("price_pct")),
        "oi_million": float(m.group("oi_value")),
        "day_change_percent": _signed(m.group("day_sign"), m.group("day_pct")),
    }

def parse_mcap_line(text: str) -> Optional[tuple]:
    m = MCAP_LINE_RE.search(text)
    if not m:
        return None
    return m.group("symbol"), float(m.group("mcap"))

def process_text(text: str):
    print(f"[TG] New message:\n{text}\n---")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        en = parse_en_line(line)
        if en:
            symbol = en["symbol"]
            mcap = mcap_buffer.get(symbol, 0.0)
            oi_store.update_from_signal(
                symbol=symbol,
                oi_delta_percent=en["oi_delta_percent"],
                price_delta_percent=en["price_delta_percent"],
                current_oi_million=en["oi_million"],
                marketcap_million=mcap,
            )
            print(
                f"[FILTER] Accept {symbol} "
                f"OI {en['oi_delta_percent']}%, Price {en['price_delta_percent']}%"
            )
            continue

        mcap_parsed = parse_mcap_line(line)
        if mcap_parsed:
            symbol, mcap = mcap_parsed
            mcap_buffer[symbol] = mcap
            print(f"[MCAP] {symbol} MarketCap = {mcap}M")
            continue

# ===========================
# 3.b CoinGlass Bot /oi（只要 15m）
# ===========================
OI_LINE_RE = re.compile(
    r"""
    ^\d+\.
    (?P<symbol>\S+)
    \s+
    (?P<oi_value>[\d\.]+)
    \s*
    (?P<unit>[KMB])
    \s*
    (?P<pct_sign>[+\-]?)
    (?P<pct>[\d\.]+)%
    """,
    re.VERBOSE
)

def _unit_to_multiplier(u: str) -> float:
    u = u.upper()
    if u == "K":
        return 1e3
    if u == "M":
        return 1e6
    if u == "B":
        return 1e9
    return 1.0

def parse_coinglass_message(text: str):
    """
    解析 CoinGlass Bot 的 /oi 消息。
    只处理标题里包含 "(15m)" 的（例如 "OI Gainers (15m)"）。
    """
    if "(15m" not in text:
        print("[CG] Skip non-15m message")
        return

    print(f"[CG-RAW]\n{text}\n---")

    section = None  # "oi_gainers" / "oi_losers"

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("OI Gainers"):
            section = "oi_gainers"
            print("[CG] Section = OI Gainers (15m)")
            continue
        if line.startswith("OI Losers"):
            section = "oi_losers"
            print("[CG] Section = OI Losers (15m)")
            continue
        if line.startswith("For more details"):
            break

        if section in ("oi_gainers", "oi_losers"):
            m = OI_LINE_RE.match(line)
            if not m:
                continue
            symbol = m.group("symbol")
            value = float(m.group("oi_value"))
            unit = m.group("unit")
            pct = float(m.group("pct"))
            if m.group("pct_sign") == "-":
                pct = -pct

            oi_usd = value * _unit_to_multiplier(unit)
            oi_million = oi_usd / 1_000_000.0

            oi_store.update_from_signal(
                symbol=symbol,
                oi_delta_percent=pct,
                price_delta_percent=0.0,  # 价格等会儿通过 Binance 算
                current_oi_million=oi_million,
                marketcap_million=0.0,
            )
            print(f"[CG-OI] {section} {symbol} OI={oi_usd} pct={pct}")

# ===========================
# 4. Telegram 监听
# ===========================
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# 原 OI 频道
@client.on(events.NewMessage(chats=TARGET_CHAT))
async def handler(event):
    process_text(event.raw_text)

# CoinGlass Bot：新消息
@client.on(events.NewMessage(chats=COINGLASS_BOT_CHAT))
async def coinglass_new(event):
    msg = event.message
    text = msg.raw_text

    # 如果是 24h 的 OI 榜，并且有键盘，就自动点 15m 按钮
    if "OI Gainers (24h)" in text or "OI Losers (24h)" in text:
        if msg.reply_markup:
            try:
                await msg.click(text="15m")
                print("[CG] Clicked 15m button on 24h message")
            except Exception as e:
                print(f"[CG] click 15m failed: {e}")
                print("[CG] reply_markup =", msg.reply_markup)
        else:
            print("[CG] 24h message has no inline keyboard")

    parse_coinglass_message(text)

# CoinGlass Bot：消息被编辑（点 15m 按钮后）
@client.on(events.MessageEdited(chats=COINGLASS_BOT_CHAT))
async def coinglass_edited(event):
    parse_coinglass_message(event.raw_text)

async def backfill_recent_messages(minutes: int = 30):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    print(f"[BACKFILL] 回溯最近 {minutes} 分钟内的消息，截止时间: {cutoff}")

    entity = await client.get_entity(TARGET_CHAT)

    msgs: List[str] = []
    async for msg in client.iter_messages(entity, limit=1000):
        if msg.date < cutoff:
            break
        if not msg.raw_text:
            continue
        msgs.append(msg.raw_text)

    print(f"[BACKFILL] 共拉取到 {len(msgs)} 条消息，开始按时间顺序处理（旧 -> 新）")
    for raw_text in reversed(msgs):
        process_text(raw_text)
    print("[BACKFILL] 历史消息处理完成")

async def coinglass_poll_task():
    """
    定时给 CoinGlass Bot 发送 /oi。
    收到 24h 消息后，由 coinglass_new 自动点击 15m。
    """
    while True:
        try:
            await client.send_message(COINGLASS_BOT_CHAT, "/oi")
            print("[CG-POLL] Sent /oi to CoinGlass Bot")
        except Exception as e:
            print(f"[CG-POLL] error sending /oi: {e}")

        await asyncio.sleep(600)  # 10 分钟

def run_tg_listener():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def main():
        await client.start()
        print(f"✅ Telegram listener started, watching chats: {TARGET_CHAT}, {COINGLASS_BOT_CHAT}")

        try:
            await backfill_recent_messages(minutes=30)
        except Exception as e:
            print(f"[BACKFILL] 回溯消息时出错: {e}")

        asyncio.create_task(coinglass_poll_task())

        await client.run_until_disconnected()

    loop.run_until_complete(main())

def run_pruner():
    while True:
        oi_store.prune_old(max_age_seconds=2 * 3600)
        time.sleep(300)

# ===========================
# 5. HTTP API
# ===========================
app = FastAPI()

@app.get("/oi_top")
def get_oi_top():
    data = oi_store.export_oitop_response(limit=20)
    return JSONResponse(content=data)

# ===========================
# 6. 主入口
# ===========================
if __name__ == "__main__":
    t = threading.Thread(target=run_tg_listener, daemon=True)
    t.start()

    cleaner = threading.Thread(target=run_pruner, daemon=True)
    cleaner.start()

    print(f"Starting HTTP server on port {HTTP_PORT} ...")
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT)
