import json
import asyncio
import time
import os
import logging
import random
import hashlib
from logging.handlers import RotatingFileHandler
from pathlib import Path
from contextlib import asynccontextmanager
from collections import deque

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import httpx
import secrets
import webbrowser
import re
import copy

# ============================================================
# 日志
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("gateway")
# 压制 httpx 的 INFO 日志（公告拉取等请求噪音）
logging.getLogger("httpx").setLevel(logging.WARNING)

# ============================================================
# 路径与常量
# ============================================================
import sys

if getattr(sys, 'frozen', False):
    APP_DIR = Path(sys._MEIPASS)
    DATA_DIR = Path(sys.executable).parent
else:
    APP_DIR = Path(__file__).parent
    DATA_DIR = Path(__file__).parent

DATA_FILE = DATA_DIR / "providers.json"
CONFIG_FILE = DATA_DIR / "config.json"
HISTORY_FILE = DATA_DIR / "history.jsonl"
USAGE_FILE = DATA_DIR / "usage.jsonl"
META_FILE = DATA_DIR / "models_meta.json"
ROUTERS_FILE = DATA_DIR / "routers.json"
ANNOUNCEMENT_FILE = DATA_DIR / "announcement.json"

APP_VERSION = "1.6.1"

MAX_HISTORY_DAYS = 30
MAX_USAGE_DAYS = 30
HISTORY_CLEANUP_INTERVAL = 6 * 3600
ONE_MILLION = 1048576
POLL_INTERVAL = 300
CIRCUIT_FAIL_THRESHOLD = 3
CIRCUIT_RECOVERY_SECONDS = 60
QUALITY_WINDOW = 20


# ============================================================
# 原子写入
# ============================================================
def atomic_write(path: Path, content: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ============================================================
# 配置说明（写入 config.json 作为备注）
# ============================================================
CONFIG_NOTE = (
    "========== 使用说明 ==========\n"
    "【三种模式切换】\n"
    "  🔒 安全模式（默认）：删除 local_api_key 整个字段，重启后自动生成随机 Key\n"
    "  🔓 开放模式：将 local_api_key 的值设为空字符串 \"\"，任意 API Key 均可通信\n"
    "  🔒 自定义 Key：在 local_api_key 中填入你的密钥，必须匹配才能通信\n"
    "【端口配置】\n"
    "  修改 port 字段即可，默认 8000\n"
    "【轮询配置】\n"
    "  poll_interval：轮询间隔（秒），默认 3600（1 小时）\n"
    "  poll_work_start：工作时间开始（小时），默认 7\n"
    "  poll_work_end：工作时间结束（小时），默认 21\n"
    "  poll_daily_limit：每日轮询次数上限，默认 20，超过后改为每天 12:00 一次\n"
    "=============================="
)


# ============================================================
# 配置加载
# ============================================================
def load_config():
    if CONFIG_FILE.exists():
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        cfg.setdefault("port", 8000)
        cfg.setdefault("_note", CONFIG_NOTE)
        # 轮询配置默认值
        cfg.setdefault("poll_interval", 3600)
        cfg.setdefault("poll_work_start", 7)
        cfg.setdefault("poll_work_end", 21)
        cfg.setdefault("poll_daily_limit", 20)
        # 三种模式：
        # ① local_api_key 字段不存在 → 自动生成随机 Key（安全模式）
        # ② local_api_key 为空字符串 "" → 开放模式（任意 Key 放行）
        # ③ local_api_key 有具体值 → 安全模式（必须匹配）
        if "local_api_key" not in cfg:
            cfg["local_api_key"] = "sk-local-" + secrets.token_hex(16)
            atomic_write(CONFIG_FILE, json.dumps(cfg, ensure_ascii=False, indent=2))
        elif not cfg["local_api_key"]:
            cfg["local_api_key"] = None
        return cfg
    data = {
        "_note": CONFIG_NOTE,
        "local_api_key": "sk-local-" + secrets.token_hex(16),
        "port": 8000,
        "poll_interval": 3600,
        "poll_work_start": 7,
        "poll_work_end": 21,
        "poll_daily_limit": 20,
    }
    atomic_write(CONFIG_FILE, json.dumps(data, ensure_ascii=False, indent=2))
    return data


def save_config():
    """将内存中的 app_config 原子写回 config.json。"""
    atomic_write(CONFIG_FILE, json.dumps(app_config, ensure_ascii=False, indent=2))


def load_providers():
    if DATA_FILE.exists():
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    return []


def save_providers(data):
    atomic_write(DATA_FILE, json.dumps(data, ensure_ascii=False, indent=2))


def load_meta():
    """三层合并：内置兜底(APP_DIR/models_meta.json) → 外部覆盖(DATA_DIR/models_meta.json)。
    dict 字段深合并，其余字段直接覆盖。"""
    default = {
        "aliases": {},
        "context_limits": {},
        "non_chat_keywords": [],
        "model_descriptions": {},
        "supports_vision": {},
    }
    # 内置版（打包内嵌进 exe，断网保底；非打包时与外部版同路径）
    builtin = APP_DIR / "models_meta.json"
    if builtin.exists():
        try:
            default.update(json.loads(builtin.read_text(encoding="utf-8")))
        except Exception:
            pass
    # 外部版（exe 同目录，用户可覆盖/补充）
    if META_FILE.exists():
        try:
            ext = json.loads(META_FILE.read_text(encoding="utf-8"))
            for k, v in ext.items():
                if isinstance(v, dict) and isinstance(default.get(k), dict):
                    default[k].update(v)
                else:
                    default[k] = v
        except Exception:
            pass
    return default


def load_routers():
    if ROUTERS_FILE.exists():
        try:
            return json.loads(ROUTERS_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {}

def save_routers():
    atomic_write(ROUTERS_FILE, json.dumps(ROUTERS, indent=2, ensure_ascii=False))


app_config = load_config()
LOCAL_API_KEY = app_config.get("local_api_key")
ROUTERS = load_routers()

meta = load_meta()
MODEL_ALIASES = meta.get("aliases", {})
CONTEXT_LIMITS = meta.get("context_limits", {})
NON_CHAT_KEYWORDS = meta.get("non_chat_keywords", [])
MODEL_DESCRIPTIONS = meta.get("model_descriptions", {})
SUPPORTS_VISION = meta.get("supports_vision", {})


# ============================================================
# 鉴权
# ============================================================
security = HTTPBearer(auto_error=False)


def verify_client(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """客户端调用 /v1/* 的鉴权"""
    # 未配置 API Key 时，接受任意 Key（开放模式）
    if not LOCAL_API_KEY:
        return credentials
    if not credentials or credentials.credentials != LOCAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return credentials


def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """管理面板调用 /api/* 的鉴权，直接使用 local_api_key"""
    # 未配置 API Key 时，接受任意 Key（开放模式）
    if not LOCAL_API_KEY:
        return credentials
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing credentials")
    if credentials.credentials != LOCAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return credentials


# ============================================================
# 全局状态
# ============================================================
providers = load_providers()
health_status: dict = {}
model_details: dict = {}
model_quality: dict = {}          # key -> {ok, fail, error, latencies: deque}
circuit_breaker: dict = {}        # key -> {fails, open_until}
model_session_count: dict = {}    # key -> 本轮对话中的调用次数
model_last_called: dict = {}      # key -> 最后一次成功调用的时间
_last_random_boosts: dict = {}    # key -> 当前轮次的随机偏移（-5%~+5%）
_last_boost_time: float = 0       # 上次生成随机偏移的时间
providers_lock = asyncio.Lock()
history_lock = asyncio.Lock()
usage_lock = asyncio.Lock()
http_client: httpx.AsyncClient | None = None
poll_task = None
last_poll_time: float = 0
last_check_time: float = time.time()
last_history_cleanup: float = 0


def mark_full_check():
    """记录一次完整检测的时间，用于重置自动轮询计时"""
    global last_check_time
    last_check_time = time.time()


# ============================================================
# 历史记录（异步文件 IO）
# ============================================================
def _append_history_sync(snapshot: dict):
    line = json.dumps({"time": time.time(), "data": snapshot}, ensure_ascii=False) + "\n"
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(line)


async def append_history(snapshot: dict):
    await asyncio.to_thread(_append_history_sync, snapshot)


def _read_history_sync(hours: int):
    if not HISTORY_FILE.exists():
        return []
    cutoff = time.time() - hours * 3600
    records = []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec["time"] >= cutoff:
                    records.append(rec)
            except Exception:
                pass
    return records


async def read_history(hours: int = 24):
    async with history_lock:
        return await asyncio.to_thread(_read_history_sync, hours)


def _cleanup_history_sync():
    if not HISTORY_FILE.exists():
        return 0
    cutoff = time.time() - MAX_HISTORY_DAYS * 86400
    kept = []
    removed = 0
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec["time"] >= cutoff:
                    kept.append(line if line.endswith("\n") else line + "\n")
                else:
                    removed += 1
            except Exception:
                pass
    if removed > 0:
        atomic_write(HISTORY_FILE, "".join(kept))
    return removed


async def maybe_cleanup_history():
    global last_history_cleanup
    now = time.time()
    if now - last_history_cleanup < HISTORY_CLEANUP_INTERVAL:
        return
    last_history_cleanup = now
    n = await asyncio.to_thread(_cleanup_history_sync)
    if n:
        logger.info("history cleanup: removed %d expired records", n)
    un = await asyncio.to_thread(_cleanup_usage_sync)
    if un:
        logger.info("usage cleanup: removed %d expired records", un)


# ============================================================
# 消耗统计（异步文件 IO）
# ============================================================
def _append_usage_sync(record: dict):
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with open(USAGE_FILE, "a", encoding="utf-8") as f:
        f.write(line)


async def append_usage(record: dict):
    await asyncio.to_thread(_append_usage_sync, record)


def _read_usage_sync(days: int):
    if not USAGE_FILE.exists():
        return []
    cutoff = time.time() - days * 86400
    records = []
    with open(USAGE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec.get("ts", 0) >= cutoff:
                    records.append(rec)
            except Exception:
                pass
    return records


async def read_usage(days: int = 1):
    async with usage_lock:
        return await asyncio.to_thread(_read_usage_sync, days)


def _cleanup_usage_sync():
    if not USAGE_FILE.exists():
        return 0
    cutoff = time.time() - MAX_USAGE_DAYS * 86400
    kept = []
    removed = 0
    with open(USAGE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec.get("ts", 0) >= cutoff:
                    kept.append(line if line.endswith("\n") else line + "\n")
                else:
                    removed += 1
            except Exception:
                pass
    if removed > 0:
        atomic_write(USAGE_FILE, "".join(kept))
    return removed


# ============================================================
# 模型工具函数
# ============================================================
def is_chat_model(model_id: str) -> bool:
    lower = model_id.lower()
    return not any(kw in lower for kw in NON_CHAT_KEYWORDS)


def is_free_model(model_info: dict) -> bool:
    pricing = model_info.get("pricing", {})
    prompt_price = pricing.get("prompt", "")
    completion_price = pricing.get("completion", "")
    try:
        if float(prompt_price) == 0 and float(completion_price) == 0:
            return True
    except (ValueError, TypeError):
        pass
    return False


def is_free_by_name(model_id: str) -> bool:
    lower = model_id.lower()
    return ":free" in lower or "-free" in lower


def get_enabled_models(provider: dict) -> list[str]:
    """返回该 provider 未被禁用的模型列表"""
    disabled = set(provider.get("disabled_models", []))
    return [m for m in provider.get("models", []) if m not in disabled]


def normalize_model(model: str) -> str:
    """归一化模型名到标准名（去 xxx/ 前缀 + 转小写）。
    先查别名表做名字修正（如 mistral-small-2603 → mistral-small-4-119b-2603），
    再统一去前缀转小写。这样不同平台/大小写命名都归一到同一标准名。"""
    if model in MODEL_ALIASES:
        model = MODEL_ALIASES[model]
    return model.split('/')[-1].lower()


def get_context_length(model: str) -> int:
    # ① 归一化名查表
    norm = normalize_model(model)
    ctx = CONTEXT_LIMITS.get(norm) or CONTEXT_LIMITS.get(model)
    if ctx:
        return ctx
    # ② 大小写回退（防标准名表里仍存了带前缀/带大小写的旧键）
    lower = model.lower()
    for k, v in CONTEXT_LIMITS.items():
        if k.lower() == lower:
            return v
    # ③ 轮询拉取的 model_details
    return (model_details.get(norm, {}).get("context_length")
            or model_details.get(model, {}).get("context_length")
            or 32768)


def is_vision_model(model: str) -> bool:
    """是否支持识图（基于 supports_vision 标记 + 归一化匹配）"""
    norm = normalize_model(model)
    return bool(SUPPORTS_VISION.get(norm) or SUPPORTS_VISION.get(model))


def is_1m_model(model: str) -> bool:
    ctx = get_context_length(model)
    return bool(ctx) and ctx >= ONE_MILLION


def mask_key(key: str) -> str:
    if not key:
        return ""
    if len(key) <= 12:
        return "****"
    return key[:6] + "****" + key[-4:]


# ============================================================
# 质量分（内存滑动窗口）
# ============================================================
def update_model_quality(key: str, info: dict):
    q = model_quality.get(key)
    if q is None:
        q = {"status_window": deque(maxlen=QUALITY_WINDOW), "latencies": deque(maxlen=QUALITY_WINDOW)}
        model_quality[key] = q
    st = info.get("status", "unknown")
    if st in ("ok", "fail", "error"):
        q["status_window"].append(st)
    if st == "ok":
        lat = info.get("latency_ms")
        if lat:
            q["latencies"].append(lat)


def get_quality_score(key: str) -> float:
    """0~1 可用率，无数据返回 0.5（中性，避免新模型排在已稳定的模型前面）"""
    q = model_quality.get(key)
    if not q or not q["status_window"]:
        return 0.5
    ok_count = sum(1 for s in q["status_window"] if s == "ok")
    return ok_count / len(q["status_window"])


def get_avg_latency(key: str):
    q = model_quality.get(key)
    if not q or not q["latencies"]:
        return None
    return sum(q["latencies"]) / len(q["latencies"])


# ============================================================
# 熔断
# ============================================================
def is_circuit_open(key: str) -> bool:
    cb = circuit_breaker.get(key)
    if not cb:
        return False
    if cb.get("open_until") and time.time() >= cb["open_until"]:
        cb["fails"] = 0
        cb["open_until"] = 0
        return False
    return bool(cb.get("open_until"))


def record_fail(key: str):
    cb = circuit_breaker.setdefault(key, {"fails": 0, "open_until": 0})
    cb["fails"] += 1
    if cb["fails"] >= CIRCUIT_FAIL_THRESHOLD:
        cb["open_until"] = time.time() + CIRCUIT_RECOVERY_SECONDS
        logger.warning("circuit opened: %s", key)
    update_model_quality(key, {"status": "fail"})
    _append_history_sync({key: {"status": "fail", "checked_at": time.time()}})
    # 写入调用日志（失败记录）
    parts = key.split("||", 1)
    _append_usage_sync({
        "ts": time.time(), "model": parts[1] if len(parts) > 1 else key,
        "provider": parts[0] if len(parts) > 1 else "unknown",
        "pt": 0, "ct": 0, "tt": 0, "duration": 0, "tps": 0,
        "status": "fail",
    })


def record_success(key: str):
    cb = circuit_breaker.get(key)
    if cb:
        cb["fails"] = 0
        cb["open_until"] = 0
    update_model_quality(key, {"status": "ok"})
    _append_history_sync({key: {"status": "ok", "checked_at": time.time()}})
    # 记录模型调用计数和最后调用时间（用于随机轮换）
    model_session_count[key] = model_session_count.get(key, 0) + 1
    model_last_called[key] = time.time()


# ============================================================
# 探测
# ============================================================
async def check_model(base_url: str, api_key: str, model: str) -> dict:
    actual_model = MODEL_ALIASES.get(model, model)
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": actual_model,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5,
        "stream": False,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    start = time.time()
    try:
        resp = await http_client.post(url, json=payload, headers=headers, timeout=30)
        latency = round((time.time() - start) * 1000)
        if resp.status_code == 200:
            usage = resp.json().get("usage", {})
            return {
                "status": "ok",
                "code": resp.status_code,
                "latency_ms": latency,
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
            }
        return {
            "status": "fail",
            "code": resp.status_code,
            "latency_ms": latency,
            "detail": resp.text[:200],
        }
    except Exception as e:
        latency = round((time.time() - start) * 1000)
        return {"status": "error", "latency_ms": latency, "detail": str(e)[:200]}


async def fetch_model_details(base_url: str, api_key: str) -> dict:
    url = base_url.rstrip("/") + "/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = await http_client.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            model_list = data.get("data", data) if isinstance(data, dict) else data
            details = {}
            for m in model_list:
                if isinstance(m, dict) and "id" in m:
                    pricing = m.get("pricing", {})
                    details[m["id"]] = {
                        "context_length": m.get("context_length"),
                        "prompt_price": pricing.get("prompt", ""),
                        "completion_price": pricing.get("completion", ""),
                    }
            return details
    except Exception:
        logger.exception("fetch_model_details failed for %s", base_url)
    return {}


async def fetch_models(base_url: str, api_key: str, free_only: bool = True) -> list[str]:
    url = base_url.rstrip("/") + "/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = await http_client.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            model_list = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(model_list, list):
                return []
            if free_only:
                has_pricing = any(isinstance(m, dict) and m.get("pricing") for m in model_list)
                if has_pricing:
                    free_by_api = [
                        m for m in model_list
                        if isinstance(m, dict) and "id" in m and is_free_model(m)
                    ]
                    if free_by_api:
                        return [m["id"] for m in free_by_api if is_chat_model(m["id"])]
                free_by_name = [
                    m["id"] for m in model_list
                    if isinstance(m, dict) and "id" in m
                    and is_free_by_name(m["id"]) and is_chat_model(m["id"])
                ]
                if free_by_name:
                    return free_by_name
            return [
                m["id"] for m in model_list
                if "id" in m and isinstance(m, dict) and is_chat_model(m["id"])
            ]
    except Exception:
        logger.exception("fetch_models failed for %s", base_url)
    return []


async def verify_provider_key_impl(base_url: str, api_key: str) -> dict:
    """校验上游 key 是否有效：调上游 /models 接口，401/403 判定 key 无效"""
    url = base_url.rstrip("/") + "/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = await http_client.get(url, headers=headers, timeout=15)
        if resp.status_code in (401, 403):
            return {"ok": False, "detail": f"Key 无效（HTTP {resp.status_code}）"}
        if resp.status_code == 200:
            return {"ok": True, "detail": "连接成功"}
        return {"ok": False, "detail": f"上游返回 HTTP {resp.status_code}"}
    except httpx.RequestError as e:
        return {"ok": False, "detail": f"连接失败：{str(e)[:150]}"}
    except Exception as e:
        return {"ok": False, "detail": f"校验异常：{str(e)[:150]}"}


# ============================================================
# 轮询
# ============================================================
async def run_health_checks(tasks: list[tuple[str, str, str, str]]) -> dict:
    """并发检测所有 (name, model, base_url, api_key) 任务，返回 {key: result}。"""
    sem = asyncio.Semaphore(10)

    async def limited_check(url, key, m):
        async with sem:
            return await check_model(url, key, m)

    results = await asyncio.gather(
        *[limited_check(url, key, m) for _, m, url, key in tasks],
        return_exceptions=True,
    )
    new_status = {}
    for (name, m, _, _), result in zip(tasks, results):
        k = f"{name}||{m}"
        if isinstance(result, Exception):
            new_status[k] = {
                "status": "error",
                "detail": str(result)[:200],
                "checked_at": time.time(),
            }
        else:
            result["checked_at"] = time.time()
            new_status[k] = result
        update_model_quality(k, new_status[k])
    return new_status


async def poll_all():
    global health_status, last_poll_time, last_check_time
    # 首次拉取 model details
    for p in list(providers):
        try:
            details = await fetch_model_details(p["base_url"], p["api_key"])
            if details:
                model_details.update(details)
        except Exception:
            logger.exception("initial model_details fetch failed: %s", p.get("name"))

    # 按模型独立计数——从历史记录中读取每个模型被轮询的次数
    # 注意：仅统计轮询产生的记录（一行中有多个模型的状态），不统计请求失败记录
    model_poll_count: dict[str, int] = {}
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as _f:
                for line in _f:
                    try:
                        rec = json.loads(line.strip())
                        keys = list(rec.get("data", {}).keys())
                        # 轮询记录：一行中有多个模型的状态
                        # 请求失败记录：一行中只有1个模型
                        if len(keys) > 1:
                            for key in keys:
                                model_poll_count[key] = model_poll_count.get(key, 0) + 1
                    except Exception:
                        pass
        except Exception:
            pass

    while True:
        now = time.localtime()

        # 读取轮询配置
        interval = app_config.get("poll_interval", 3600)
        work_start = app_config.get("poll_work_start", 7)
        work_end = app_config.get("poll_work_end", 21)
        daily_limit = app_config.get("poll_daily_limit", 20)

        current_hour = now.tm_hour

        # 判断是否在工作时间
        in_work_hours = work_start <= current_hour < work_end

        if not in_work_hours:
            # 非工作时间：标记上次轮询时间（避免 UI 一直显示"初始化中"）
            if last_poll_time == 0:
                last_poll_time = time.time()
            # 计算下次工作开始时间，等待到那个时候
            next_work = time.mktime((
                now.tm_year, now.tm_mon, now.tm_mday,
                work_start, 0, 0, now.tm_wday, now.tm_yday, now.tm_isdst
            ))
            if next_work <= time.time():
                next_work += 86400  # 明天
            wait_seconds = next_work - time.time()
            logger.info("非工作时间，等待 %d 秒后恢复轮询", int(wait_seconds))
            while time.time() < max(next_work, last_check_time + interval):
                await asyncio.sleep(30)
            continue

        # 检测新增模型：不在 model_poll_count 中的模型，立即执行首次轮询
        new_model_tasks = []
        for p in list(providers):
            for m in get_enabled_models(p):
                key = f"{p['name']}||{m}"
                if key not in model_poll_count:
                    new_model_tasks.append((p["name"], m, p["base_url"], p["api_key"]))
        if new_model_tasks:
            try:
                logger.info("检测到 %d 个新增模型，立即执行首次探测", len(new_model_tasks))
                new_status = await run_health_checks(new_model_tasks)
                health_status.update(new_status)
                for key in new_status:
                    model_poll_count[key] = model_poll_count.get(key, 0) + 1
                await append_history(new_status)
                logger.info("new model first poll done: %d models", len(new_status))
            except Exception:
                logger.exception("new model first poll failed")

        # 构建本次要轮询的模型列表（只选未达到上限的）
        tasks = []
        for p in list(providers):
            for m in get_enabled_models(p):
                key = f"{p['name']}||{m}"
                if model_poll_count.get(key, 0) < daily_limit:
                    tasks.append((p["name"], m, p["base_url"], p["api_key"]))

        # 如果所有模型都已达到上限，改为每天 12:00 检测一次
        if not tasks:
            # 标记轮询已初始化，避免 UI 卡在"初始化中…"
            if last_poll_time == 0:
                last_poll_time = time.time()
            noon_today = time.mktime((
                now.tm_year, now.tm_mon, now.tm_mday,
                12, 0, 0, now.tm_wday, now.tm_yday, now.tm_isdst
            ))
            if time.time() < noon_today:
                wait_seconds = noon_today - time.time()
            else:
                wait_seconds = noon_today + 86400 - time.time()
            logger.info("所有模型轮询已达 %d 次上限，改为每天 12:00 检测一次", daily_limit)
            while time.time() < max(time.time() + wait_seconds, last_check_time + interval):
                await asyncio.sleep(30)
            continue

        try:
            new_status = await run_health_checks(tasks)
            health_status = new_status
            last_poll_time = time.time()
            last_check_time = last_poll_time
            for key in new_status:
                model_poll_count[key] = model_poll_count.get(key, 0) + 1
            await append_history(new_status)
            await maybe_cleanup_history()
            ok_count = sum(1 for v in new_status.values() if v.get("status") == "ok")
            under_limit = sum(1 for c in model_poll_count.values() if c < daily_limit)
            logger.info("poll done: %d/%d ok, 剩余 %d 个模型未达上限", ok_count, len(new_status), under_limit)
        except Exception:
            logger.exception("poll_all loop error")
        # 等待到 last_check_time + interval；
        # 若手动检测更新了 last_check_time，则顺延，避免短时间内重复轮询
        while time.time() < last_check_time + interval:
            await asyncio.sleep(5)


# ============================================================
# lifespan
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, poll_task
    # 运行时日志落盘（带轮转，避免无限膨胀）；放在此处确保 uvicorn 配置 logging 后再挂，不被清空
    try:
        _fh = RotatingFileHandler(DATA_DIR / "gateway.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        logging.getLogger().addHandler(_fh)
    except Exception:
        pass
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=10.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    app.state.http = http_client
    # 启动时从历史记录回放所有记录到 model_quality（deque 自动控制最多 20 条）
    # 确保路由质量分与 UI 显示的稳定度数据一致
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as _f:
                lines = _f.readlines()
            for line in lines:
                rec = json.loads(line.strip())
                for key, info in rec.get("data", {}).items():
                    update_model_quality(key, info)
            logger.info("replayed %d history lines into model_quality", len(lines))
        except Exception:
            logger.exception("replay history failed")
    poll_task = asyncio.create_task(poll_all())
    yield
    if poll_task:
        poll_task.cancel()
    await http_client.aclose()


app = FastAPI(title="模型API网关", lifespan=lifespan)
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))
templates.env.auto_reload = True


@app.middleware("http")
async def no_cache_middleware(request: Request, call_next):
    """给页面和 API 响应加 no-cache，避免 pywebview/浏览器缓存旧前端。"""
    resp = await call_next(request)
    if request.url.path in ("/",) or request.url.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp


# ============================================================
# Pydantic 模型
# ============================================================
class ProviderIn(BaseModel):
    name: str
    base_url: str
    api_key: str
    models: list[str] = []
    free_only: bool = True


class ProviderUpdate(BaseModel):
    name: str | None = None
    base_url: str | None = None
    api_key: str | None = None
    models: list[str] | None = None
    free_only: bool | None = None


class ToggleModelIn(BaseModel):
    model: str
    enabled: bool


class VerifyKeyIn(BaseModel):
    base_url: str
    api_key: str


class OpenUrlIn(BaseModel):
    url: str


class PresetApplyIn(BaseModel):
    keys: dict = {}


# ============================================================
# 模型选择
# ============================================================
def _effective_score(key: str) -> float:
    """路由综合分 = 质量分 - 延迟惩罚 + 随机偏移"""
    base = get_quality_score(key)
    # 延迟惩罚：超过1秒的部分每1秒扣2%，最多扣0.3分（lat 单位：毫秒→秒）
    lat = (get_avg_latency(key) or 0) / 1000
    penalty = min(round(max(0, lat - 1) * 0.02, 4), 0.3)
    boost = _last_random_boosts.get(key, 0)
    result = base - penalty + boost
    logger.info("ROUTE %s: base=%.2f penalty=%.2f boost=%.2f result=%.2f", key, base, penalty, boost, result)
    return result


def pick_available_models(model: str | None = None, force: bool = False) -> list[tuple[dict, str]]:
    """返回按质量排序的候选 (provider, model) 列表"""
    
    raw = []
    unhealthy_raw = []
    
    # 判断是否需要生成随机偏移（连续使用10次以上 + 闲置5分钟以上）
    global _last_random_boosts, _last_boost_time
    now = time.time()
    needs_reshuffle = False
    for key, count in list(model_session_count.items()):
        if count >= 10 and now - model_last_called.get(key, now) > 300:
            needs_reshuffle = True
            break
    if needs_reshuffle:
        # 清空旧偏移，为新轮次生成随机偏移（-5%~+5%）
        _last_random_boosts = {}
        _last_boost_time = now
        # 重置会话计数，避免每次请求都触发重新生成
        model_session_count.clear()
    
    # 如果请求的是自定义路由组
    if model in ROUTERS:
        target_models = set(ROUTERS[model])
        for p in providers:
            for m in get_enabled_models(p):
                if m in target_models:
                    k = f"{p['name']}||{m}"
                    if is_circuit_open(k):
                        unhealthy_raw.append((p, m, k))
                    else:
                        raw.append((p, m, k))
                        if needs_reshuffle:
                            _last_random_boosts[k] = random.uniform(-0.05, 0.05)
        if not raw:
            raw = unhealthy_raw
        scored = [
            (_effective_score(k), get_avg_latency(k) or 1e9, p, m)
            for p, m, k in raw
        ]
        scored.sort(key=lambda x: (-x[0], x[1]))
        return [(p, m) for _, _, p, m in scored]

    # 否则按具体模型匹配
    for p in providers:
        for m in get_enabled_models(p):
            prefixed = f"{p['name']}-{m}"
            if model and model != m and model != prefixed:
                continue
            k = f"{p['name']}||{m}"
            if force or model:
                raw.append((p, m, k))
                if needs_reshuffle:
                    _last_random_boosts[k] = random.uniform(-0.05, 0.05)
                continue
            st = health_status.get(k, {}).get("status")
            if not is_circuit_open(k) and st in ("ok", None, "unknown"):
                raw.append((p, m, k))
                if needs_reshuffle:
                    _last_random_boosts[k] = random.uniform(-0.05, 0.05)
            else:
                unhealthy_raw.append((p, m, k))
    if not raw:
        raw = unhealthy_raw
    scored = [
        (_effective_score(k), get_avg_latency(k) or 1e9, p, m)
        for p, m, k in raw
    ]
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [(p, m) for _, _, p, m in scored]


def pick_available_model(model: str | None = None, force: bool = False):
    cands = pick_available_models(model, force)
    return cands[0] if cands else (None, None)


# ============================================================
# Hermes 工具名压缩 / 还原
# ============================================================
HERMES_MAP = [
    ("mcp_hermes_studio_use_hermes_studio_use_", "mcp_hsu_"),
    ("mcp_hermes_studio_devices_hermes_studio_lan_", "mcp_hsd_"),
    ("mcp_hermes_studio_api_hermes_studio_api_", "mcp_hsa_"),
]


def compress_hermes(obj: dict) -> dict:
    s = json.dumps(obj, ensure_ascii=False)
    for long, short in HERMES_MAP:
        s = s.replace(long, short)
    return json.loads(s)


def restore_hermes_text(text: str) -> str:
    for long, short in HERMES_MAP:
        text = text.replace(short, long)
    return text


def merge_reasoning(obj: dict) -> dict:
    """保留 reasoning_content 字段原样透传，不合并到 content"""
    return obj

# ============================================================
# 回复语言跟随：根据用户消息语言决定回复语言
# ============================================================
LANG_HINT = (
    "\n\n【重要】请始终使用简体中文回答用户。"
    "思考过程(reasoning)也请用中文。"
    "代码、命令、文件名、专有名词、标识符等保持原样即可，不要翻译。"
)


def ensure_lang_reply(body: dict) -> dict:
    """注入简体中文回复提示。
    - 已有 system 且为纯文本：在末尾追加指令（带判重，幂等）。
    - 无 system：在最前面插入一条 system。"""
    msgs = body.get("messages")
    if not isinstance(msgs, list) or not msgs:
        return body
    first = msgs[0]
    if isinstance(first, dict) and first.get("role") == "system":
        c = first.get("content")
        if isinstance(c, str) and "请始终使用简体中文" not in c:
            first["content"] = c.rstrip() + LANG_HINT
        return body
    msgs.insert(0, {"role": "system", "content": "请使用简体中文回答。" + LANG_HINT})
    return body


# ============================================================
# 页面
# ============================================================
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "local_api_key": LOCAL_API_KEY,
            "app_version": APP_VERSION,
        },
    )


# ============================================================
# 管理接口（admin 鉴权）
# ============================================================
@app.get("/api/poll-status")
async def poll_status(_=Depends(verify_admin)):
    now = time.localtime()
    ws = app_config.get("poll_work_start", 7)
    we = app_config.get("poll_work_end", 21)
    in_work = ws <= now.tm_hour < we
    if last_poll_time > 0:
        status_str = "working" if in_work else f"sleeping (work hours: {ws}:00-{we}:00)"
    else:
        status_str = "init"
    return {
        "last_poll_time": last_poll_time,
        "total_models": sum(len(get_enabled_models(p)) for p in providers),
        "status": status_str,
        "ready": last_poll_time > 0,
    }


@app.get("/api/history")
async def get_history(hours: int = 24, _=Depends(verify_admin)):
    return await read_history(hours)


_stability_cache: dict = {}
STABILITY_CACHE_TTL = 30


@app.get("/api/stability")
async def get_stability(hours: int = 24, _=Depends(verify_admin)):
    now = time.time()
    cached = _stability_cache.get(hours)
    if cached and now - cached[0] < STABILITY_CACHE_TTL:
        return cached[1]
    records = await read_history(hours)
    model_stats: dict = {}
    for rec in records:
        for key, info in rec.get("data", {}).items():
            if key not in model_stats:
                model_stats[key] = {"ok": 0, "fail": 0, "error": 0, "total": 0, "latencies": []}
            model_stats[key]["total"] += 1
            st = info.get("status", "unknown")
            if st == "ok":
                model_stats[key]["ok"] += 1
                if info.get("latency_ms"):
                    model_stats[key]["latencies"].append(info["latency_ms"])
            elif st == "fail":
                model_stats[key]["fail"] += 1
            elif st == "error":
                model_stats[key]["error"] += 1
    allowed = set()

    for p in providers:
        for m in p.get("models", []):
            k = f"{p['name']}||{m}"
            allowed.add(k)
            if k not in model_stats:
                model_stats[k] = {"ok": 0, "fail": 0, "error": 0, "total": 0, "latencies": []}
    model_stats = {k: v for k, v in model_stats.items() if k in allowed}
    result = []
    for key, s in model_stats.items():
        name, model = key.split("||", 1)
        avg_lat = sum(s["latencies"]) / len(s["latencies"]) if s["latencies"] else None
        result.append({
            "provider": name,
            "model": model,
            "checks": s["total"],
            "ok": s["ok"],
            "fail": s["fail"],
            "error": s["error"],
            "availability": round(s["ok"] / s["total"] * 100, 1) if s["total"] else 0,
            "avg_latency_ms": round(avg_lat) if avg_lat else None,
            "min_latency_ms": min(s["latencies"]) if s["latencies"] else None,
            "max_latency_ms": max(s["latencies"]) if s["latencies"] else None,
            "last_status": health_status.get(key, {}).get("status", "unknown"),
            "vision": is_vision_model(model),
        })
    result.sort(key=lambda x: (-x["availability"], x["avg_latency_ms"] or 99999))
    _stability_cache[hours] = (now, result)
    return result


@app.get("/api/usage")
async def get_usage(days: int = 1, _=Depends(verify_admin)):
    days = max(1, min(days, MAX_USAGE_DAYS))
    records = await read_usage(days)
    total = {"pt": 0, "ct": 0, "tt": 0, "requests": 0}
    by_day = {}
    by_model = {}
    for r in records:
        ts = r.get("ts", 0)
        day = time.strftime("%Y-%m-%d", time.localtime(ts))
        pt = r.get("pt", 0) or 0
        ct = r.get("ct", 0) or 0
        tt = r.get("tt", 0) or (pt + ct)
        m = r.get("model", "unknown")
        p = r.get("provider", "unknown")
        total["pt"] += pt
        total["ct"] += ct
        total["tt"] += tt
        total["requests"] += 1
        d = by_day.setdefault(day, {"pt": 0, "ct": 0, "tt": 0, "requests": 0})
        d["pt"] += pt
        d["ct"] += ct
        d["tt"] += tt
        d["requests"] += 1
        mk = f"{p} · {m}"
        mm = by_model.setdefault(mk, {"pt": 0, "ct": 0, "tt": 0, "requests": 0, "provider": p, "model": m})
        mm["pt"] += pt
        mm["ct"] += ct
        mm["tt"] += tt
        mm["requests"] += 1
    by_day_list = [{"date": d, **v} for d, v in sorted(by_day.items())]
    by_model_list = [
        {"provider": v["provider"], "model": v["model"], "pt": v["pt"], "ct": v["ct"], "tt": v["tt"], "requests": v["requests"]}
        for _, v in sorted(by_model.items(), key=lambda x: -x[1]["tt"])
    ]
    return {"days": days, "total": total, "by_day": by_day_list, "by_model": by_model_list}


@app.get("/api/usage-logs")
async def get_usage_logs(days: int = 1, _=Depends(verify_admin)):
    """返回模型调用日志，含耗时和 TPS"""
    days = max(1, min(days, MAX_USAGE_DAYS))
    records = await read_usage(days)
    # 按时间倒序排列
    records.sort(key=lambda r: r.get("ts", 0), reverse=True)
    logs = []
    for r in records:
        ts = r.get("ts", 0)
        dur = r.get("duration", 0)
        tps = r.get("tps", 0)
        logs.append({
            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
            "provider": r.get("provider", "unknown"),
            "model": r.get("model", "unknown"),
            "pt": r.get("pt", 0),
            "ct": r.get("ct", 0),
            "tt": r.get("tt", 0),
            "duration": dur,
            "tps": tps,
            "status": r.get("status", "ok"),
            "routing_score": round(get_quality_score(f"{r.get('provider', '')}||{r.get('model', '')}") * 100, 1),
        })
    return {"days": days, "total": len(logs), "logs": logs}


@app.get("/api/quality-scores")
async def get_quality_scores(_=Depends(verify_admin)):
    """返回所有模型的路由质量分（滑动窗口），前端可与 UI 统计分对比"""
    keys = list(model_quality.keys())
    return {key: round(get_quality_score(key) * 100, 1) for key in sorted(keys)}


@app.get("/api/model-details")
async def get_model_details(_=Depends(verify_admin)):
    merged = {}
    # 1. 上游探测结果（键为上游模型 id / 原始名）
    for k, v in model_details.items():
        merged[k] = dict(v)
    # 2. 对 providers 里每个模型(原始名)，用别名归一化查 meta 兜底
    #    解决魔搭等 provider 用别名形式(如 ZhipuAI/GLM-5.2)而 meta 里
    #    只有规范化名(如 glm-5.2) 导致前端查不到上下文/描述的问题

    for p in providers:
        for m in p.get("models", []):
            entry = merged.setdefault(m, {})
            norm = MODEL_ALIASES.get(m, m)
            meta_desc = MODEL_DESCRIPTIONS.get(norm, {})
            if not entry.get("context_length"):
                ctx = meta_desc.get("ctx") or CONTEXT_LIMITS.get(norm)
                if ctx:
                    entry["context_length"] = ctx
            if not entry.get("desc"):
                desc = meta_desc.get("desc", "")
                if desc:
                    entry["desc"] = desc
    # 3. 对 meta 里规范化名也建条目（兼容以规范化名查询）
    for k, v in MODEL_DESCRIPTIONS.items():
        if k not in merged:
            merged[k] = {}
        # 上游 context_length 为 None/0/缺失时，用元数据覆盖
        if not merged[k].get("context_length"):
            merged[k]["context_length"] = v.get("ctx")
        merged[k]["desc"] = v.get("desc", "")
    return merged


@app.get("/api/context-limits")
async def get_context_limits(_=Depends(verify_admin)):
    return {"ok": True, "data": CONTEXT_LIMITS}

class ContextLimitUpdate(BaseModel):
    model: str
    context_length: int

@app.put("/api/context-limits")
async def update_context_limit(req: ContextLimitUpdate, _=Depends(verify_admin)):
    global CONTEXT_LIMITS, meta
    meta = load_meta()
    if "context_limits" not in meta:
        meta["context_limits"] = {}
    meta["context_limits"][req.model] = req.context_length
    CONTEXT_LIMITS[req.model] = req.context_length
    atomic_write(META_FILE, json.dumps(meta, indent=2, ensure_ascii=False))
    return {"ok": True}


@app.delete("/api/context-limits/{model}")
async def delete_context_limit(model: str, _=Depends(verify_admin)):
    """删除某条自定义上下文长度配置"""
    global CONTEXT_LIMITS, meta
    meta = load_meta()
    if "context_limits" in meta and model in meta["context_limits"]:
        del meta["context_limits"][model]
    CONTEXT_LIMITS.pop(model, None)
    atomic_write(META_FILE, json.dumps(meta, indent=2, ensure_ascii=False))
    return {"ok": True}


@app.get("/api/routers")
async def get_routers_api(_=Depends(verify_admin)):
    return {"ok": True, "data": ROUTERS}

@app.post("/api/routers")
async def save_routers_api(request: Request, _=Depends(verify_admin)):
    global ROUTERS
    body = await request.json()
    ROUTERS = body
    save_routers()
    return {"ok": True}


@app.get("/api/vision-models")
async def vision_models_api(_=Depends(verify_admin)):
    """返回 supports_vision 标记的模型名列表，供前端识图配置标记"""
    return {"ok": True, "data": sorted(SUPPORTS_VISION.keys())}


# ---------- 系统公告（Gitee 远程，本地兜底） ----------
DEFAULT_ANNOUNCEMENT_URL = "https://gitee.com/ywtc000/dongye/raw/master/announcement.md"
ANNOUNCEMENT_CACHE_FILE = DATA_DIR / "announcement_cache.json"
_announcement_cache = {"content": None, "ts": 0}
ANNOUNCEMENT_TTL = 300


def _content_hash(content: str) -> str:
    """计算内容 MD5 指纹，用于前端检测公告变动"""
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def _announce_response(ok: bool, content: str) -> dict:
    return {"ok": ok, "content": content, "hash": _content_hash(content)}


@app.get("/api/announcement")
async def get_announcement(_=Depends(verify_admin)):
    """优先读 config.json 的 announcement_url（如 Gitee raw 链接）远程抓取；
    未配置或抓取失败时回退到本地 announcement.json。远程结果缓存 5 分钟。"""
    cfg = load_config()
    url = cfg.get("announcement_url") or DEFAULT_ANNOUNCEMENT_URL
    now = time.time()
    if _announcement_cache["content"] is not None and now - _announcement_cache["ts"] < ANNOUNCEMENT_TTL:
        return _announce_response(True, _announcement_cache["content"])
    # 远程抓取
    try:
        resp = await http_client.get(url, timeout=10, follow_redirects=True)
        if resp.status_code == 200 and resp.text.strip():
            content = resp.text
            _announcement_cache["content"] = content
            _announcement_cache["ts"] = now
            # 持久化到本地缓存文件，断网时回退显示上次成功的内容
            try:
                atomic_write(ANNOUNCEMENT_CACHE_FILE, json.dumps({"content": content, "ts": now}, ensure_ascii=False))
            except Exception:
                logger.warning("write announcement cache file failed")
            return _announce_response(True, content)
    except Exception:
        logger.warning("fetch remote announcement failed: %s", url)
    # 远程失败：读本地缓存文件（上次成功抓取的内容）
    if ANNOUNCEMENT_CACHE_FILE.exists():
        try:
            data = json.loads(ANNOUNCEMENT_CACHE_FILE.read_text(encoding="utf-8"))
            if data.get("content"):
                return _announce_response(True, data["content"])
        except Exception:
            pass
    # 最终兜底：默认 announcement.json
    if ANNOUNCEMENT_FILE.exists():
        try:
            data = json.loads(ANNOUNCEMENT_FILE.read_text(encoding="utf-8"))
            return _announce_response(True, data.get("content", ""))
        except Exception:
            logger.exception("parse announcement.json failed")
    return _announce_response(False, "暂无公告内容。")


# ---------- 在线更新 ----------
VERSION_CHECK_URL = ""
_update_download_state = {
    "downloading": False,
    "progress": 0,
    "total": 0,
    "done": False,
    "error": None,
    "file": None,
}


def _version_gt(a: str, b: str) -> bool:
    """比较版本号 a > b"""
    try:
        pa = [int(x) for x in a.split(".")]
        pb = [int(x) for x in b.split(".")]
        while len(pa) < len(pb):
            pa.append(0)
        while len(pb) < len(pa):
            pb.append(0)
        return pa > pb
    except Exception:
        return a.strip() != b.strip()


def _cleanup_old_exe():
    """启动时清理上次更新遗留的 .old 文件"""
    old_path = sys.executable + ".old"
    if os.path.exists(old_path):
        try:
            os.remove(old_path)
        except Exception:
            pass


@app.get("/api/check-update")
async def check_update(_=Depends(verify_admin)):
    """检查 gitee 是否有新版本"""
    cfg = load_config()
    url = cfg.get("version_check_url") or VERSION_CHECK_URL
    try:
        resp = await http_client.get(url, timeout=10, follow_redirects=True)
        if resp.status_code == 200:
            data = resp.json()
            if not isinstance(data, dict):
                return {"ok": False, "error": "版本信息格式错误"}
            latest_ver = data.get("version", "")
            has_update = _version_gt(latest_ver, APP_VERSION)
            min_ver = data.get("min_version", "")
            force_update = bool(min_ver and _version_gt(min_ver, APP_VERSION))
            return {
                "ok": True,
                "current": APP_VERSION,
                "latest": latest_ver,
                "has_update": has_update,
                "force_update": force_update,
                "download_url": data.get("download_url", ""),
                "release_notes": data.get("release_notes", ""),
                "min_version": min_ver,
            }
    except Exception as e:
        logger.warning("check update failed: %s", e)
    return {"ok": False, "error": "无法连接更新服务器"}


@app.post("/api/start-download")
async def start_download(data: dict, _=Depends(verify_admin)):
    """启动后台下载新版 exe，返回后通过 /api/download-progress 轮询进度"""
    url = data.get("url", "")
    if not url:
        return {"ok": False, "error": "缺少下载地址"}
    if _update_download_state["downloading"]:
        return {"ok": False, "error": "正在下载中，请稍候"}
    _update_download_state.update({
        "downloading": True, "progress": 0, "total": 0,
        "done": False, "error": None, "file": None,
    })
    asyncio.create_task(_do_download(url))
    return {"ok": True}


async def _do_download(url: str):
    """后台下载任务，流式写入临时文件，实时更新进度"""
    import tempfile
    # 只取临时文件名，不保持句柄（Windows 下未关闭的句柄会导致后续 open 失败）
    fd, tmp_path = tempfile.mkstemp(suffix=".exe", dir=DATA_DIR)
    os.close(fd)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(600, connect=15)) as client:
            async with client.stream("GET", url, follow_redirects=True) as resp:
                if resp.status_code != 200:
                    _update_download_state["error"] = f"下载失败: HTTP {resp.status_code}"
                    _update_download_state["downloading"] = False
                    return
                content_length = resp.headers.get("content-length")
                total = int(content_length) if content_length else 0
                _update_download_state["total"] = total
                downloaded = 0
                with open(tmp_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=256 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            _update_download_state["progress"] = round(downloaded / total * 100, 1)
        _update_download_state["done"] = True
        _update_download_state["file"] = tmp_path
        _update_download_state["downloading"] = False
    except Exception as e:
        _update_download_state["error"] = str(e)
        _update_download_state["downloading"] = False
        try:
            os.remove(tmp_path)
        except Exception:
            pass


@app.get("/api/download-progress")
async def download_progress(_=Depends(verify_admin)):
    """返回当前下载进度"""
    return {"ok": True, **{k: v for k, v in _update_download_state.items()}}


@app.post("/api/apply-update")
async def apply_update(_=Depends(verify_admin)):
    """应用更新：替换 exe 并重启"""
    if not getattr(sys, 'frozen', False):
        return {"ok": False, "error": "开发模式下不支持热更新，请打包后使用"}
    if not _update_download_state["done"] or not _update_download_state["file"]:
        return {"ok": False, "error": "没有可应用的更新"}
    new_file = _update_download_state["file"]
    if not os.path.exists(new_file):
        return {"ok": False, "error": "更新文件不存在"}
    try:
        _do_swap_and_restart(new_file)
    except Exception as e:
        return {"ok": False, "error": f"更新失败: {e}"}
    return {"ok": True}


def _do_swap_and_restart(new_exe: str):
    """重命名当前 exe → 替换新 exe → 启动新进程 → 退出当前进程"""
    import subprocess
    current_exe = sys.executable
    old_exe = current_exe + ".old"
    # 1. 删除旧残留
    if os.path.exists(old_exe):
        os.remove(old_exe)
    # 2. 当前 exe 改名为 .old（运行中的 exe 可以改名不能删）
    os.rename(current_exe, old_exe)
    # 3. 新 exe 移到当前 exe 位置
    os.rename(new_exe, current_exe)
    # 4. 启动新 exe
    creationflags = 0x00000008 if sys.platform == "win32" else 0  # DETACHED_PROCESS
    subprocess.Popen([current_exe], close_fds=True, creationflags=creationflags)
    # 5. 退出
    os._exit(0)


@app.get("/api/providers")
async def list_providers(_=Depends(verify_admin)):
    result = []

    for p in providers:
        item = {
            "name": p["name"],
            "base_url": p["base_url"],
            "api_key_masked": mask_key(p.get("api_key", "")),
            "models": p.get("models", []),
            "disabled_models": p.get("disabled_models", []),
            "free_only": p.get("free_only", True),
            "health": {},
        }
        for m in p.get("models", []):
            k = f"{p['name']}||{m}"
            item["health"][m] = health_status.get(k, {"status": "unknown"})
        result.append(item)
    return result


@app.post("/api/providers")
async def add_provider(data: ProviderIn, _=Depends(verify_admin)):
    if not re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9_.\-]+$', data.name):
        raise HTTPException(400, "名称只能包含中文、字母、数字、横杠(-)、下划线(_)、点(.)，不能含斜杠/空格等特殊字符")
    # 校验 key 有效性（保存前强制校验）
    vr = await verify_provider_key_impl(data.base_url, data.api_key)
    if not vr["ok"]:
        raise HTTPException(400, f"API Key 校验失败：{vr['detail']}")
    async with providers_lock:
        for p in providers:
            if p["name"] == data.name:
                raise HTTPException(400, "名称已存在")
        if not data.models:
            data.models = await fetch_models(data.base_url, data.api_key, data.free_only)
        providers.append(data.model_dump())
        save_providers(providers)
    return {"ok": True}


@app.post("/api/providers/verify-key")
async def verify_provider_key(data: VerifyKeyIn, _=Depends(verify_admin)):
    """校验上游 base_url + api_key 是否可用"""
    return await verify_provider_key_impl(data.base_url, data.api_key)


@app.post("/api/open-url")
async def open_url(data: OpenUrlIn, _=Depends(verify_admin)):
    """用系统默认浏览器打开外链（pywebview 内 target=_blank 会被拦截，统一走此接口）"""
    url = (data.url or "").strip()
    if not re.match(r'^https?://', url, re.I):
        raise HTTPException(400, "仅允许 http/https 链接")
    try:
        webbrowser.open(url)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"打开失败: {e}")


@app.get("/api/preset-info")
async def preset_info(_=Depends(verify_admin)):
    """返回预设清单（远端热更新优先，内置兜底）"""
    return await load_preset()


@app.get("/api/vision-assist")
async def get_vision_assist(_=Depends(verify_admin)):
    """返回识图辅助开关状态（默认关闭）"""
    cfg = app_config.get("vision_assist", {})
    enabled = cfg.get("enabled", False) if isinstance(cfg, dict) else False
    return {"enabled": enabled}


@app.put("/api/vision-assist")
async def set_vision_assist(data: dict, _=Depends(verify_admin)):
    """开启/关闭识图辅助，并持久化到 config.json"""
    enabled = bool(data.get("enabled", False))
    cfg = app_config.get("vision_assist", {})
    if not isinstance(cfg, dict):
        cfg = {}
    cfg["enabled"] = enabled
    app_config["vision_assist"] = cfg
    save_config()
    return {"enabled": enabled}


@app.post("/api/providers/preset")
async def apply_preset(data: PresetApplyIn, _=Depends(verify_admin)):
    """一键应用预设：三平台逐个校验 key → 创建/覆盖 provider → 合并路由组。
    - 用户填了 Key 的平台：校验 → 覆盖旧配置 → 重新拉模型
    - 用户没填 Key 但已有同名 provider：跳过，保留原配置不变
    - 用户没填 Key 且无同名 provider：标记未配置"""
    preset = await load_preset()
    platforms = preset.get("platforms", {})
    keys = data.keys or {}
    results = {}
    created_names = []
    async with providers_lock:
        existing_names = {p["name"] for p in providers}
        for plat_name, plat_cfg in platforms.items():
            key = (keys.get(plat_name) or "").strip()
            if not key:
                # 没填 Key：如果已有同名 provider，用已有 key 重新拉模型（确保预设新增的模型生效）
                if plat_name in existing_names:
                    existing = next((p for p in providers if p["name"] == plat_name), None)
                    if existing and existing.get("api_key"):
                        key = existing["api_key"]
                        # 继续往下执行（复用已有 key 重新拉模型）
                    else:
                        results[plat_name] = {"ok": True, "detail": "保留已有配置（无可用 Key）"}
                        continue
                else:
                    results[plat_name] = {"ok": False, "detail": "未填写 Key"}
                    continue
            # 校验 key
            vr = await verify_provider_key_impl(plat_cfg["base_url"], key)
            if not vr["ok"]:
                results[plat_name] = {"ok": False, "detail": vr["detail"]}
                continue
            # 同名则先移除旧配置（覆盖历史数据，用新 key 重建）
            if plat_name in existing_names:
                providers[:] = [p for p in providers if p["name"] != plat_name]
                existing_names.discard(plat_name)
            # 拉模型
            fetched = await fetch_models(plat_cfg["base_url"], key, plat_cfg.get("free_only", True))
            visible = plat_cfg.get("models_visible", [])
            if visible:
                models = [m for m in fetched if m in set(visible)]
                for m in visible:
                    if m not in models:
                        models.append(m)
                disabled = []
            else:
                models = fetched
                disabled = []
            providers.append({
                "name": plat_name,
                "base_url": plat_cfg["base_url"],
                "api_key": key,
                "models": models,
                "disabled_models": disabled,
                "free_only": plat_cfg.get("free_only", True),
            })
            existing_names.add(plat_name)
            created_names.append(plat_name)
            results[plat_name] = {"ok": True, "detail": f"已配置 {len(models)} 个模型"}
        save_providers(providers)
        # 预设路由组覆盖（预设有的组完全替换为模板，预设没的组保留不动）
        preset_routers = preset.get("routers", {})
        for gname, members in preset_routers.items():
            ROUTERS[gname] = list(members)
        save_routers()
    return {"ok": True, "results": results, "created": created_names}


@app.post("/api/providers/{name}/fetch-models")
async def refresh_models(name: str, _=Depends(verify_admin)):
    async with providers_lock:
        for p in providers:
            if p["name"] == name:
                models = await fetch_models(p["base_url"], p["api_key"], p.get("free_only", True))
                if models:
                    p["models"] = models
                    save_providers(providers)
                return {"ok": True, "models": models}
    raise HTTPException(404, "未找到")


@app.get("/api/providers/{name}/available-models")
async def get_available_models(name: str, _=Depends(verify_admin)):

    for p in providers:
        if p["name"] == name:
            models = await fetch_models(p["base_url"], p["api_key"], free_only=False)
            return {"ok": True, "models": models}
    raise HTTPException(404, "未找到")


@app.put("/api/providers/{name}")
async def update_provider(name: str, data: ProviderUpdate, _=Depends(verify_admin)):
    async with providers_lock:
        for i, p in enumerate(providers):
            if p["name"] == name:
                providers[i].update(data.model_dump(exclude_unset=True))
                save_providers(providers)
                return {"ok": True}
    raise HTTPException(404, "未找到")


@app.delete("/api/providers/{name}")
async def delete_provider(name: str, _=Depends(verify_admin)):
    global providers
    async with providers_lock:
        providers = [p for p in providers if p["name"] != name]
        save_providers(providers)
    return {"ok": True}


@app.post("/api/providers/{name}/toggle-model")
async def toggle_model(name: str, data: ToggleModelIn, _=Depends(verify_admin)):
    async with providers_lock:
        for p in providers:
            if p["name"] == name:
                disabled = p.get("disabled_models", [])
                if data.enabled:
                    if data.model in disabled:
                        disabled.remove(data.model)
                else:
                    if data.model not in disabled:
                        disabled.append(data.model)
                p["disabled_models"] = disabled
                save_providers(providers)
                return {"ok": True, "disabled_models": disabled}
    raise HTTPException(404, "未找到")


@app.post("/api/check/{name}/{model}")
async def manual_check(name: str, model: str, _=Depends(verify_admin)):

    for p in providers:
        if p["name"] == name:
            result = await check_model(p["base_url"], p["api_key"], model)
            k = f"{name}||{model}"
            result["checked_at"] = time.time()
            health_status[k] = result
            update_model_quality(k, result)
            await append_history({k: result})
            return result
    raise HTTPException(404, "未找到")


@app.post("/api/check/all")
async def check_all(_=Depends(verify_admin)):
    tasks = []
    for p in list(providers):
        for m in get_enabled_models(p):
            tasks.append((p["name"], m, p["base_url"], p["api_key"]))
    results = await run_health_checks(tasks)
    health_status.update(results)
    await append_history(results)
    mark_full_check()
    return results


# ============================================================
# 预设模板（三层加载：远端热更新 → 内置兜底）
# ============================================================
PRESET_REMOTE_URL = "https://gitee.com/ywtc000/dongye/raw/master/presets.json"
PRESET_DOC_URL = "https://pv284bk9no6.feishu.cn/wiki/HCOuwXuZGibDUGkWLlpcQuiLnDf"
PRESET_CACHE_TTL = 300

# 内置兜底预设（断网保底；平台变更时改远端 presets.json 热更新即可，无需重新打包）
BUILTIN_PRESET = {
    "version": "2026-07-20",
    "updated_at": "2026-07-20",
    "doc_url": PRESET_DOC_URL,
    "platforms": {
        "NVIDIA": {
            "base_url": "https://integrate.api.nvidia.com/v1",
            "free_only": True,
            "key_page_url": "https://build.nvidia.com/",
            "auth_hint": "需绑定手机号",
            "models_visible": [
                "deepseek-ai/deepseek-v4-flash",
                "deepseek-ai/deepseek-v4-pro",
                "minimaxai/minimax-m3",
                "mistralai/mistral-large-3-675b-instruct-2512",
                "mistralai/mistral-small-4-119b-2603",
                "nvidia/nemotron-3-super-120b-a12b",
                "nvidia/nemotron-3-ultra-550b-a55b",
                "qwen/qwen3.5-122b-a10b",
                "z-ai/glm-5.2",
                "meta/llama-3.2-11b-vision-instruct",
                "nvidia/nemotron-nano-12b-v2-vl",
                "nvidia/llama-3.1-nemotron-nano-vl-8b-v1",
            ],
        },
        "SenseNova": {
            "base_url": "https://token.sensenova.cn/v1",
            "free_only": False,
            "key_page_url": "https://platform.sensenova.cn/console/keys",
            "auth_hint": "手机号注册登录即可",
            "models_visible": [
                "deepseek-v4-flash",
                "glm-5.2",
                "sensenova-6.7-flash-lite",
            ],
        },
        "魔搭": {
            "base_url": "https://api-inference.modelscope.cn/v1",
            "free_only": False,
            "key_page_url": "https://modelscope.cn/my/myaccesstoken",
            "auth_hint": "需绑定阿里云账号（支付宝实名）",
            "models_visible": [
                "Qwen/Qwen3.5-122B-A10B",
                "Qwen/Qwen3.5-397B-A17B",
                "deepseek-ai/DeepSeek-V4-Flash",
                "deepseek-ai/DeepSeek-V4-Pro",
                "OpenGVLab/InternVL3_5-241B-A28B",
                "Qwen/Qwen3-VL-8B-Thinking",
                "Qwen/Qwen3-VL-8B-Instruct",
                "PaddlePaddle/ERNIE-4.5-VL-28B-A3B-PT",
            ],
        },
    },
    "routers": {
        "256k": [
            "mistralai/mistral-large-3-675b-instruct-2512",
            "mistralai/mistral-small-4-119b-2603",
            "nvidia/nemotron-3-super-120b-a12b",
            "nvidia/nemotron-3-ultra-550b-a55b",
            "qwen/qwen3.5-122b-a10b",
            "sensenova-6.7-flash-lite",
            "Qwen/Qwen3.5-122B-A10B",
            "Qwen/Qwen3.5-397B-A17B",
        ],
        "1m": [
            "deepseek-ai/deepseek-v4-pro",
            "minimaxai/minimax-m3",
            "z-ai/glm-5.2",
            "glm-5.2",
            "deepseek-ai/DeepSeek-V4-Pro",
            "deepseek-ai/deepseek-v4-flash",
            "deepseek-v4-flash",
            "deepseek-ai/DeepSeek-V4-Flash",
        ],
        "识图": [
            "sensenova-6.7-flash-lite",
            "mistralai/mistral-large-3-675b-instruct-2512",
            "mistralai/mistral-small-4-119b-2603",
            "meta/llama-3.2-11b-vision-instruct",
            "nvidia/nemotron-nano-12b-v2-vl",
            "nvidia/llama-3.1-nemotron-nano-vl-8b-v1",
            "OpenGVLab/InternVL3_5-241B-A28B",
            "Qwen/Qwen3-VL-8B-Thinking",
            "Qwen/Qwen3-VL-8B-Instruct",
            "PaddlePaddle/ERNIE-4.5-VL-28B-A3B-PT",
        ],
    },
}

_preset_cache = {"data": None, "ts": 0.0}


async def load_preset(force_remote: bool = False) -> dict:
    """三层加载：远端热更新(优先) → 内置兜底。缓存 PRESET_CACHE_TTL 秒。"""
    now = time.time()
    if (not force_remote and _preset_cache["data"]
            and now - _preset_cache["ts"] < PRESET_CACHE_TTL):
        return _preset_cache["data"]
    if http_client:
        try:
            resp = await http_client.get(PRESET_REMOTE_URL, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and data.get("platforms"):
                    _preset_cache["data"] = data
                    _preset_cache["ts"] = now
                    return data
        except Exception:
            logger.warning("load_preset remote fetch failed, fallback to builtin")
    if not _preset_cache["data"]:
        _preset_cache["data"] = BUILTIN_PRESET
        _preset_cache["ts"] = now
    return _preset_cache["data"]


def has_image(body: dict) -> bool:
    """检测 messages 是否含 image_url（仅看最后一轮 user 消息，历史图片不算）"""
    msgs = body.get("messages", [])
    for i in range(len(msgs) - 1, -1, -1):
        msg = msgs[i]
        if isinstance(msg, dict) and msg.get("role") == "user":
            content = msg.get("content")
            if isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "image_url":
                        return True
            return False  # 最后一轮 user 消息没有图，不再往前看
    return False


CN_HINT = "（请用简体中文回答）"

def _inject_cn_hint(body: dict):
    """将中文回复指令注入最后一条 user 消息，确保识图模型用中文回答。
    部分小模型会忽略 system prompt，直接塞进用户消息最可靠。"""
    msgs = body.get("messages")
    if not isinstance(msgs, list):
        return
    for i in range(len(msgs) - 1, -1, -1):
        msg = msgs[i]
        if isinstance(msg, dict) and msg.get("role") == "user":
            content = msg.get("content")
            if isinstance(content, str) and CN_HINT not in content:
                msg["content"] = content + "\n" + CN_HINT
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        t = part.get("text", "")
                        if CN_HINT not in t:
                            part["text"] = t + "\n" + CN_HINT
                        break
            break


# ============================================================
# 代理（客户端鉴权）
# ============================================================
async def _stream_with_failover(candidates, body, is_router, prelude: str = "", start_time: float = 0):
    """流式转发，中断时自动切换下一个候选模型继续输出。prelude 为先输出给用户的提示文本。"""

    async def gen():
        accumulated = ""
        prefix_done = False
        max_attempts = 2 if is_router else 1

        if prelude:
            yield "data: " + json.dumps({"choices": [{"delta": {"content": prelude}, "index": 0}]}, ensure_ascii=False) + "\n\n"

        for attempt in range(max_attempts):
            for provider, model in candidates:
                k = f"{provider['name']}||{model}"
                req_body = copy.deepcopy(body)
                req_body["model"] = MODEL_ALIASES.get(model, model)

                if accumulated:
                    msgs = list(req_body.get("messages", []))
                    msgs.append({"role": "assistant", "content": accumulated})
                    msgs.append({"role": "user", "content": "请继续上面的回复，从中断处接着写。"})
                    req_body["messages"] = msgs

                url = provider["base_url"].rstrip("/") + "/chat/completions"
                headers = {
                    "Authorization": f"Bearer {provider['api_key']}",
                    "Content-Type": "application/json",
                }

                req = http_client.build_request("POST", url, json=req_body, headers=headers)
                try:
                    resp = await http_client.send(req, stream=True)
                except httpx.RequestError as e:
                    logger.warning("stream connect error to %s: %s", provider["name"], e)
                    record_fail(k)
                    continue

                if resp.status_code != 200:
                    try:
                        await resp.aread()
                    except Exception:
                        pass
                    await resp.aclose()
                    logger.warning("upstream stream error %d from %s", resp.status_code, provider["name"])
                    record_fail(k)
                    continue

                usage_obj = None
                stream_ok = True

                try:
                    async for line in resp.aiter_lines():
                        if not line:
                            continue
                        if not line.startswith("data: "):
                            yield line + "\n"
                            continue
                        data_str = line[6:]
                        if data_str.strip() == "[DONE]":
                            yield "data: [DONE]\n\n"
                            break
                        try:
                            obj = json.loads(data_str)
                            if obj.get("usage"):
                                usage_obj = obj["usage"]
                            if "model" in obj and isinstance(obj["model"], str):
                                obj["model"] = f"{provider['name']} · {model}"
                            choices = obj.get("choices") or []
                            if choices:
                                delta = choices[0].get("delta") or {}
                                c = delta.get("content")
                                if isinstance(c, str):
                                    accumulated += c
                                rc = delta.get("reasoning_content")
                                if isinstance(rc, str):
                                    accumulated += rc
                            if not prefix_done:
                                choices2 = obj.get("choices") or []
                                if choices2:
                                    delta2 = choices2[0].get("delta") or {}
                                    c2 = delta2.get("content")
                                    if isinstance(c2, str) and c2:
                                        delta2["content"] = f"🤖 {provider['name']} · {model}\n\n{c2}"
                                        prefix_done = True
                            out = json.dumps(obj, ensure_ascii=False)
                            out = restore_hermes_text(out)
                            yield "data: " + out + "\n\n"
                        except json.JSONDecodeError:
                            yield line + "\n"
                    record_success(k)
                    try:
                        pt = (usage_obj or {}).get("prompt_tokens", 0) or 0
                        ct = (usage_obj or {}).get("completion_tokens", 0) or 0
                        tt = pt + ct
                        dur = time.time() - start_time if start_time else 0
                        await append_usage({
                            "ts": time.time(), "model": model,
                            "provider": provider["name"],
                            "pt": pt, "ct": ct, "tt": tt,
                            "duration": round(dur, 2),
                            "tps": round(ct / dur, 2) if dur > 0 and ct > 0 else 0,
                        })
                    except Exception:
                        logger.exception("append_usage(stream) failed")
                    return
                except Exception:
                    stream_ok = False
                    logger.exception("stream interrupted from %s, switching", provider["name"])
                    record_fail(k)
                    try:
                        await resp.aclose()
                    except Exception:
                        pass
                    continue
                finally:
                    if stream_ok:
                        try:
                            await resp.aclose()
                        except Exception:
                            pass

        yield "data: " + json.dumps({"choices": [{"delta": {"content": "\n\n⚠️ 所有模型均失败，回复中断。"}, "index": 0}]}, ensure_ascii=False) + "\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.api_route("/v1/chat/completions", methods=["POST"], dependencies=[Depends(verify_client)])
async def proxy_chat(request: Request, force: bool = False):
    body = await request.json()
    body = compress_hermes(body)
    body = ensure_lang_reply(body)
    requested_model = body.get("model")

    # 识图辅助：含图片且目标非识图组/识图模型 → 直接转交识图路由组
    vision_cfg = app_config.get("vision_assist", {})
    vision_enabled = vision_cfg.get("enabled", False) if isinstance(vision_cfg, dict) else False
    is_vision_request = (requested_model == "识图") or bool(requested_model and is_vision_model(requested_model))
    vision_prelude = ""
    if vision_enabled and has_image(body) and not is_vision_request:
        if "识图" in ROUTERS:
            requested_model = "识图"
            vision_prelude = "🖼️ 已切换到视觉模型回复…\n\n"
            # 识图模型 max_tokens 上限较低（部分仅 32768），避免客户端传的百万级值导致 upstream 400
            for key in ("max_tokens", "max_completion_tokens"):
                if body.get(key, 0) > 16384:
                    body[key] = 16384
            # 部分识图模型忽略 system prompt，把中文指令直接注入用户消息末尾
            _inject_cn_hint(body)
        else:
            raise HTTPException(503, "识图辅助已开启，但未配置识图路由组，无法处理图片。")

    candidates = pick_available_models(requested_model, force=force)
    if not candidates:
        raise HTTPException(503, f"无可用的模型: {requested_model or '任意'}")

    _start_time = time.time()
    is_router = requested_model in ROUTERS
    stream = body.get("stream", False)
    last_err = None

    if stream:
        return await _stream_with_failover(candidates, body, is_router, prelude=vision_prelude, start_time=_start_time)

    for attempt in (2,) if is_router else (1,):
        for provider, model in candidates:
            k = f"{provider['name']}||{model}"
            req_body = copy.deepcopy(body)
            req_body["model"] = MODEL_ALIASES.get(model, model)
            url = provider["base_url"].rstrip("/") + "/chat/completions"
            headers = {
                "Authorization": f"Bearer {provider['api_key']}",
                "Content-Type": "application/json",
            }

            try:
                resp = await http_client.post(url, json=req_body, headers=headers, timeout=120)
                if resp.status_code >= 400:
                    logger.warning("upstream %d from %s: %s", resp.status_code, provider["name"], resp.text[:200])
                    record_fail(k)
                    last_err = f"upstream {resp.status_code}"
                    continue
                try:
                    parsed = json.loads(resp.text)
                    parsed = merge_reasoning(parsed)
                    parsed_str = json.dumps(parsed, ensure_ascii=False)
                    parsed_str = restore_hermes_text(parsed_str)
                    parsed = json.loads(parsed_str)
                    record_success(k)
                    parsed["model"] = f"{provider['name']} · {model}"
                    try:
                        u = parsed.get("usage") or {}
                        pt = u.get("prompt_tokens", 0) or 0
                        ct = u.get("completion_tokens", 0) or 0
                        tt = pt + ct
                        dur = time.time() - _start_time if '_start_time' in dir() else 0
                        await append_usage({
                            "ts": time.time(), "model": model,
                            "provider": provider["name"],
                            "pt": pt, "ct": ct, "tt": tt,
                            "duration": round(dur, 2),
                            "tps": round(ct / dur, 2) if dur > 0 and ct > 0 else 0,
                        })
                    except Exception:
                        logger.exception("append_usage(non-stream) failed")
                    try:
                        msg = parsed["choices"][0]["message"]
                        c = msg.get("content")
                        prefix_parts = []
                        if vision_prelude:
                            prefix_parts.append(vision_prelude.rstrip())
                        prefix_parts.append(f"🤖 {provider['name']} · {model}")
                        prefix = "\n\n".join(prefix_parts)
                        if isinstance(c, str) and c:
                            msg["content"] = f"{prefix}\n\n{c}"
                        elif isinstance(c, str):
                            msg["content"] = prefix
                    except (KeyError, IndexError, TypeError):
                        pass
                    return JSONResponse(content=parsed, status_code=resp.status_code)
                except json.JSONDecodeError:
                    logger.warning("upstream non-json from %s: %s", provider["name"], resp.text[:200])
                    record_fail(k)
                    last_err = f"upstream non-json ({resp.status_code})"
                    continue
            except httpx.RequestError as e:
                logger.warning("forward error to %s: %s", provider["name"], e)
                record_fail(k)
                last_err = str(e)
                continue
            except Exception as e:
                logger.exception("unexpected forward error to %s", provider["name"])
                record_fail(k)
                last_err = str(e)
                continue

    raise HTTPException(502, f"所有候选模型均失败: {last_err}")


_models_cache = {"ts": 0, "data": None}
MODELS_CACHE_TTL = 30


@app.api_route("/v1/models", methods=["GET"], dependencies=[Depends(verify_client)])
async def proxy_models():
    now = time.time()
    if _models_cache["data"] and now - _models_cache["ts"] < MODELS_CACHE_TTL:
        return _models_cache["data"]
    models_list = []

    # 自定义路由组作为可输出的模型
    for router_name in ROUTERS:
        models_list.append({
            "id": router_name,
            "object": "model",
            "owned_by": "Router",
            "available": True,
        })

    for p in providers:
        disabled = set(p.get("disabled_models", []))
        for m in p.get("models", []):
            if m in disabled:
                continue
            k = f"{p['name']}||{m}"
            st = health_status.get(k, {}).get("status")
            # 三态：unknown/None -> True（乐观），ok -> True，fail/error -> False
            available = st in (None, "unknown", "ok")
            ctx_len = get_context_length(m)
            models_list.append({
                "id": f"{p['name']}-{m}",
                "object": "model",
                "owned_by": p["name"],
                "available": available,
                "context_length": ctx_len,
                "max_position_embeddings": ctx_len,
                "max_model_len": ctx_len,
            })
    result = {"object": "list", "data": models_list}
    _models_cache["data"] = result
    _models_cache["ts"] = now
    return result


if __name__ == "__main__":
    import uvicorn
    import threading
    import webview
    import time
    from PIL import Image, ImageDraw
    import pystray
    import msvcrt

    # ---- 清理上次更新的残留文件 ----
    _cleanup_old_exe()

    # ---- 单实例限制 ----
    # 真实客户端（打包 exe）：已运行则弹窗提示并退出，不允许重复打开。
    # 开发/测试（python app.py）：设环境变量 GATEWAY_AUTO_KILL=1 时，
    # 自动关闭占用端口的旧实例后再启动，方便反复重启调试。
    import socket
    import subprocess

    def port_in_use(port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            return s.connect_ex(("127.0.0.1", port)) == 0

    def kill_old_instance(port: int):
        try:
            out = subprocess.run(
                ["netstat", "-ano", "-p", "TCP"],
                capture_output=True, text=True, timeout=10,
            ).stdout
            for line in out.splitlines():
                if f":{port}" in line and "LISTENING" in line:
                    pid = line.split()[-1]
                    if pid.isdigit():
                        subprocess.run(
                            ["taskkill", "/PID", pid, "/F"],
                            capture_output=True, text=True, timeout=10,
                        )
                        return True
        except Exception:
            pass
        return False

    SERVER_PORT = app_config.get("port", 8000)
    AUTO_KILL = os.environ.get("GATEWAY_AUTO_KILL") == "1"

    if AUTO_KILL and port_in_use(SERVER_PORT):
        kill_old_instance(SERVER_PORT)
        time.sleep(1.5)

    # 文件锁：真实客户端靠它拦截重复启动；测试模式端口已清，锁也能正常获取
    LOCK_FILE = str(DATA_DIR / ".gateway.lock")
    try:
        _lock_fd = open(LOCK_FILE, "w")
        msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
    except (OSError, IOError):
        import ctypes
        ctypes.windll.user32.MessageBoxW(
            0, "网关客户端已在运行中，请勿重复启动。", "提示", 0x30
        )
        sys.exit(0)

    # ---- 生成托盘图标（纯几何图形，不依赖外部图片文件） ----
    def create_tray_icon():
        img = Image.new('RGBA', (64, 64), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        # 蓝色圆角底
        draw.rounded_rectangle([4, 4, 60, 60], radius=14, fill=(30, 144, 255))
        # 白色右箭头，代表"网关/转发"
        draw.polygon([(22, 20), (44, 32), (22, 44)], fill="white")
        return img

    # ---- 全局状态：quitting 用于区分"点X隐藏"与"托盘退出" ----
    state = {"window": None, "quitting": False}

    # ---- 托盘菜单回调 ----
    def on_show(icon, item):
        w = state["window"]
        if w:
            w.show()

    def on_quit(icon, item):
        state["quitting"] = True
        icon.stop()
        w = state["window"]
        if w:
            w.destroy()

    tray_icon = pystray.Icon(
        "model-gateway",
        create_tray_icon(),
        "无限额度监控网关",
        menu=pystray.Menu(
            pystray.MenuItem("显示窗口", on_show, default=True),
            pystray.MenuItem("退出", on_quit),
        ),
    )

    # ---- FastAPI 服务器（daemon 线程，主进程退出时自动结束） ----
    def start_server():
        uvicorn.run(app, host="127.0.0.1", port=SERVER_PORT, log_level="warning")

    t = threading.Thread(target=start_server, daemon=True)
    t.start()

    # 给一点点时间让 FastAPI 绑定端口
    time.sleep(1)

    # ---- 创建原生的桌面窗口 ----
    window = webview.create_window(
        '无限额度监控网关', f'http://127.0.0.1:{SERVER_PORT}/', width=1200, height=800
    )
    state["window"] = window

    # ---- 拦截关闭：点 X 时隐藏到托盘，而非退出程序 ----
    def on_closing():
        if state["quitting"]:
            return  # 退出流程：放行，允许真正关闭
        window.hide()
        return False  # 阻止关闭，仅隐藏窗口

    window.events.closing += on_closing

    # ---- 启动系统托盘（独立 daemon 线程） ----
    threading.Thread(target=tray_icon.run, daemon=True).start()

    # ---- 启动 webview（主线程阻塞，窗口全部关闭后返回） ----
    webview.start()

