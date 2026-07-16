import json
import asyncio
import time
import logging
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

# ============================================================
# 日志
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("gateway")

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

APP_VERSION = "1.4.1"

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
# 配置加载
# ============================================================
def load_config():
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    data = {
        "local_api_key": "sk-local-" + secrets.token_hex(16),
    }
    atomic_write(CONFIG_FILE, json.dumps(data, indent=2))
    return data


def load_providers():
    if DATA_FILE.exists():
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    return []


def save_providers(data):
    atomic_write(DATA_FILE, json.dumps(data, ensure_ascii=False, indent=2))


def load_meta():
    default = {
        "aliases": {},
        "context_limits": {},
        "non_chat_keywords": [],
        "model_descriptions": {},
    }
    if META_FILE.exists():
        default.update(json.loads(META_FILE.read_text(encoding="utf-8")))
    return default


def load_routers():
    if ROUTERS_FILE.exists():
        try:
            return json.loads(ROUTERS_FILE.read_text(encoding="utf-8"))
        except:
            pass
    return {}

def save_routers():
    ROUTERS_FILE.write_text(json.dumps(ROUTERS, indent=2, ensure_ascii=False), encoding="utf-8")


app_config = load_config()
LOCAL_API_KEY = app_config.get("local_api_key")
ROUTERS = load_routers()

meta = load_meta()
MODEL_ALIASES = meta.get("aliases", {})
CONTEXT_LIMITS = meta.get("context_limits", {})
NON_CHAT_KEYWORDS = meta.get("non_chat_keywords", [])
MODEL_DESCRIPTIONS = meta.get("model_descriptions", {})


# ============================================================
# 鉴权
# ============================================================
security = HTTPBearer(auto_error=False)


def verify_client(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """客户端调用 /v1/* 的鉴权"""
    if not credentials or credentials.credentials != LOCAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return credentials


def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """管理面板调用 /api/* 的鉴权，直接使用 local_api_key"""
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


def get_context_length(model: str) -> int:
    actual = MODEL_ALIASES.get(model, model)
    ctx = CONTEXT_LIMITS.get(model) or CONTEXT_LIMITS.get(actual)
    if ctx:
        return ctx
    return model_details.get(actual, {}).get("context_length") or 32768


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
    """0~1 可用率，无数据返回 1.0（乐观）"""
    q = model_quality.get(key)
    if not q or not q["status_window"]:
        return 1.0
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


def record_success(key: str):
    cb = circuit_breaker.get(key)
    if cb:
        cb["fails"] = 0
        cb["open_until"] = 0


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


# ============================================================
# 轮询
# ============================================================
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

    while True:
        try:
            tasks = []
            for p in list(providers):
                for m in get_enabled_models(p):
                    tasks.append((p["name"], m, p["base_url"], p["api_key"]))
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
            health_status = new_status
            last_poll_time = time.time()
            last_check_time = last_poll_time
            await append_history(new_status)
            await maybe_cleanup_history()
            ok_count = sum(1 for v in new_status.values() if v.get("status") == "ok")
            logger.info("poll done: %d/%d ok", ok_count, len(new_status))
        except Exception:
            logger.exception("poll_all loop error")
        # 等待到 last_check_time + POLL_INTERVAL；
        # 若手动检测更新了 last_check_time，则顺延，避免短时间内重复轮询
        while time.time() < last_check_time + POLL_INTERVAL:
            await asyncio.sleep(5)


# ============================================================
# lifespan
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, poll_task
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=10.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    app.state.http = http_client
    poll_task = asyncio.create_task(poll_all())
    yield
    if poll_task:
        poll_task.cancel()
    await http_client.aclose()


app = FastAPI(title="模型API网关", lifespan=lifespan)
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))
templates.env.auto_reload = True


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


# ============================================================
# 模型选择
# ============================================================
def pick_available_models(model: str | None = None, force: bool = False) -> list[tuple[dict, str]]:
    """返回按质量排序的候选 (provider, model) 列表"""
    
    raw = []
    unhealthy_raw = []
    
    # 1. 如果请求的是自定义路由组
    if model in ROUTERS:
        target_models = set(ROUTERS[model])
        for p in providers:
            for m in get_enabled_models(p):
                if m in target_models:
                    k = f"{p['name']}||{m}"
                    raw.append((p, m, k))
        scored = [
            (get_quality_score(k), 1.0 / (get_avg_latency(k) or 1e9), p, m, k)
            for p, m, k in raw
        ]
        scored.sort(key=lambda x: (-x[0], -x[1]))
        return [(p, m) for _, _, p, m, _ in scored]
        
    # 3. 如果请求的是具体模型
    for p in providers:
        for m in get_enabled_models(p):
            prefixed = f"{p['name']}-{m}"
            if model and model != m and model != prefixed:
                continue
            k = f"{p['name']}||{m}"
            if force or model:
                raw.append((p, m, k))
                continue
            st = health_status.get(k, {}).get("status")
            if not is_circuit_open(k) and st in ("ok", None, "unknown"):
                raw.append((p, m, k))
            else:
                unhealthy_raw.append((p, m, k))
    if not raw:
        raw = unhealthy_raw
    scored = [
        (get_quality_score(k), get_avg_latency(k) or 1e9, p, m)
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
    for choice in choices:
        target = choice.get("delta") or choice.get("message")
        if not target or not isinstance(target, dict):
            continue
        rc = target.pop("reasoning_content", None)
        if rc is None:
            continue
        wrapped = f"<think>{rc}</think>"
        c = target.get("content")
        if isinstance(c, str) and c:
            target["content"] = c + wrapped
        else:
            target["content"] = wrapped
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
    return {
        "last_poll_time": last_poll_time,
        "total_models": sum(len(get_enabled_models(p)) for p in providers),
    }


@app.get("/api/history")
async def get_history(hours: int = 24, _=Depends(verify_admin)):
    return await read_history(hours)


@app.get("/api/stability")
async def get_stability(hours: int = 24, _=Depends(verify_admin)):
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
        })
    result.sort(key=lambda x: (-x["availability"], x["avg_latency_ms"] or 99999))
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
    META_FILE.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"ok": True}


@app.delete("/api/context-limits/{model}")
async def delete_context_limit(model: str, _=Depends(verify_admin)):
    """删除某条自定义上下文长度配置"""
    global CONTEXT_LIMITS, meta
    meta = load_meta()
    if "context_limits" in meta and model in meta["context_limits"]:
        del meta["context_limits"][model]
    CONTEXT_LIMITS.pop(model, None)
    META_FILE.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
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


# ---------- 系统公告（Gitee 远程，本地兜底） ----------
DEFAULT_ANNOUNCEMENT_URL = "https://gitee.com/ywtc000/dongye/raw/master/announcement.md"
ANNOUNCEMENT_CACHE_FILE = DATA_DIR / "announcement_cache.json"
_announcement_cache = {"content": None, "ts": 0}
ANNOUNCEMENT_TTL = 300


@app.get("/api/announcement")
async def get_announcement(_=Depends(verify_admin)):
    """优先读 config.json 的 announcement_url（如 Gitee raw 链接）远程抓取；
    未配置或抓取失败时回退到本地 announcement.json。远程结果缓存 5 分钟。"""
    cfg = load_config()
    url = cfg.get("announcement_url") or DEFAULT_ANNOUNCEMENT_URL
    now = time.time()
    if _announcement_cache["content"] is not None and now - _announcement_cache["ts"] < ANNOUNCEMENT_TTL:
        return {"ok": True, "content": _announcement_cache["content"]}
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
            return {"ok": True, "content": content}
    except Exception:
        logger.warning("fetch remote announcement failed: %s", url)
    # 远程失败：读本地缓存文件（上次成功抓取的内容）
    if ANNOUNCEMENT_CACHE_FILE.exists():
        try:
            data = json.loads(ANNOUNCEMENT_CACHE_FILE.read_text(encoding="utf-8"))
            if data.get("content"):
                return {"ok": True, "content": data["content"]}
        except Exception:
            pass
    # 最终兜底：默认 announcement.json
    if ANNOUNCEMENT_FILE.exists():
        try:
            data = json.loads(ANNOUNCEMENT_FILE.read_text(encoding="utf-8"))
            return {"ok": True, "content": data.get("content", "")}
        except Exception:
            logger.exception("parse announcement.json failed")
    return {"ok": False, "content": "暂无公告内容。"}


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
    async with providers_lock:
        for p in providers:
            if p["name"] == data.name:
                raise HTTPException(400, "名称已存在")
        if not data.models:
            data.models = await fetch_models(data.base_url, data.api_key, data.free_only)
        providers.append(data.model_dump())
        save_providers(providers)
    return {"ok": True}


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
    results = {}
    tasks = []

    for p in providers:
        for m in get_enabled_models(p):
            tasks.append((p["name"], m, p["base_url"], p["api_key"]))
    sem = asyncio.Semaphore(10)

    async def limited_check(url, key, m):
        async with sem:
            return await check_model(url, key, m)

    check_results = await asyncio.gather(
        *[limited_check(url, key, m) for _, m, url, key in tasks],
        return_exceptions=True,
    )
    for (name, m, _, _), result in zip(tasks, check_results):
        k = f"{name}||{m}"
        if isinstance(result, Exception):
            results[k] = {"status": "error", "detail": str(result)[:200], "checked_at": time.time()}
        else:
            result["checked_at"] = time.time()
            results[k] = result
        update_model_quality(k, results[k])
    health_status.update(results)
    await append_history(results)
    mark_full_check()
    return results


# ============================================================
# 代理（客户端鉴权）
# ============================================================
async def _stream_with_failover(candidates, body, is_router):
    """流式转发，中断时自动切换下一个候选模型继续输出"""

    async def gen():
        accumulated = ""
        prefix_done = False
        max_attempts = 2 if is_router else 1

        for attempt in range(max_attempts):
            for provider, model in candidates:
                k = f"{provider['name']}||{model}"
                req_body = json.loads(json.dumps(body))
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
                        await append_usage({
                            "ts": time.time(), "model": model,
                            "provider": provider["name"],
                            "pt": pt, "ct": ct, "tt": pt + ct,
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

    candidates = pick_available_models(requested_model, force=force)
    if not candidates:
        raise HTTPException(503, f"无可用的模型: {requested_model or '任意'}")

    is_router = requested_model in ROUTERS
    stream = body.get("stream", False)
    last_err = None

    if stream:
        return await _stream_with_failover(candidates, body, is_router)

    for attempt in (2,) if is_router else (1,):
        for provider, model in candidates:
            k = f"{provider['name']}||{model}"
            req_body = json.loads(json.dumps(body))
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
                        await append_usage({
                            "ts": time.time(), "model": model,
                            "provider": provider["name"],
                            "pt": pt, "ct": ct, "tt": pt + ct,
                        })
                    except Exception:
                        logger.exception("append_usage(non-stream) failed")
                    try:
                        msg = parsed["choices"][0]["message"]
                        c = msg.get("content")
                        prefix = f"🤖 {provider['name']} · {model}"
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


@app.api_route("/v1/models", methods=["GET"], dependencies=[Depends(verify_client)])
async def proxy_models():
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
    return {"object": "list", "data": models_list}


if __name__ == "__main__":
    import uvicorn
    import threading
    import webview
    import time
    from PIL import Image, ImageDraw
    import pystray
    import msvcrt

    # ---- 单实例限制：防止重复启动 ----
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
        uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")

    t = threading.Thread(target=start_server, daemon=True)
    t.start()

    # 给一点点时间让 FastAPI 绑定端口
    time.sleep(1)

    # ---- 创建原生的桌面窗口 ----
    window = webview.create_window(
        '无限额度监控网关', 'http://127.0.0.1:8000/', width=1200, height=800
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

