import pytest
import sys
from pathlib import Path

# 让测试能 import app
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app as app_module


# ============================================================
# 辅助：构造 provider
# ============================================================
def provider(name="P", models=None, disabled=None):
    return {
        "name": name,
        "base_url": "http://x/v1",
        "api_key": "k",
        "models": models or [],
        "disabled_models": disabled or [],
    }


# ============================================================
# get_enabled_models
# ============================================================
def test_enabled_models_filters_disabled():
    p = provider(models=["a", "b", "c"], disabled=["b"])
    assert app_module.get_enabled_models(p) == ["a", "c"]


def test_enabled_models_no_disabled():
    p = provider(models=["a", "b"])
    assert app_module.get_enabled_models(p) == ["a", "b"]


def test_enabled_models_empty():
    p = provider(models=[])
    assert app_module.get_enabled_models(p) == []


# ============================================================
# is_chat_model / is_free_model
# ============================================================
def test_chat_model_true():
    assert app_module.is_chat_model("deepseek-ai/deepseek-v4-flash") is True


def test_chat_model_false_for_embedding():
    assert app_module.is_chat_model("text-embedding-3") is False


def test_free_model_by_pricing_zero():
    info = {"pricing": {"prompt": "0", "completion": "0"}}
    assert app_module.is_free_model(info) is True


def test_free_model_by_pricing_zero_decimal():
    info = {"pricing": {"prompt": "0.00", "completion": "0E-10"}}
    assert app_module.is_free_model(info) is True


def test_not_free_model():
    info = {"pricing": {"prompt": "0.001", "completion": "0.002"}}
    assert app_module.is_free_model(info) is False


def test_free_model_no_pricing():
    assert app_module.is_free_model({}) is False


# ============================================================
# mask_key
# ============================================================
def test_mask_key_long():
    # 前6字符 "nvapi-" + "****" + 后4字符 "lmno"
    assert app_module.mask_key("nvapi-abcdefghijklmno") == "nvapi-****lmno"


def test_mask_key_short():
    assert app_module.mask_key("short") == "****"


def test_mask_key_empty():
    assert app_module.mask_key("") == ""


# ============================================================
# merge_reasoning
# ============================================================
def test_merge_reasoning_combines_content():
    obj = {"choices": [{"delta": {"reasoning_content": "think", "content": "hi"}}]}
    out = app_module.merge_reasoning(obj)
    assert out["choices"][0]["delta"]["content"] == "hi<think>think</think>"
    assert "reasoning_content" not in out["choices"][0]["delta"]


def test_merge_reasoning_no_content_field():
    obj = {"choices": [{"delta": {"reasoning_content": "think"}}]}
    out = app_module.merge_reasoning(obj)
    assert out["choices"][0]["delta"]["content"] == "<think>think</think>"


def test_merge_reasoning_no_reasoning():
    obj = {"choices": [{"delta": {"content": "hi"}}]}
    out = app_module.merge_reasoning(obj)
    assert out["choices"][0]["delta"]["content"] == "hi"


def test_merge_reasoning_message_key():
    obj = {"choices": [{"message": {"reasoning_content": "think", "content": "hi"}}]}
    out = app_module.merge_reasoning(obj)
    assert out["choices"][0]["message"]["content"] == "hi<think>think</think>"
    assert "reasoning_content" not in out["choices"][0]["message"]


def test_merge_reasoning_no_choices():
    obj = {"id": "x"}
    assert app_module.merge_reasoning(obj) == {"id": "x"}



# ============================================================
# compress / restore hermes
# ============================================================
def test_compress_then_restore_roundtrip():
    body = {"messages": [{"role": "user", "content": "mcp_hermes_studio_use_hermes_studio_use_tool"}]}
    compressed = app_module.compress_hermes(body)
    assert "mcp_hsu_" in compressed["messages"][0]["content"]
    s = app_module.restore_hermes_text("__mcp_hsu_tool__")
    assert "mcp_hermes_studio_use_hermes_studio_use_" in s


# ============================================================
# pick_available_models：disabled 过滤
# ============================================================
def test_pick_skips_disabled(monkeypatch):
    monkeypatch.setattr(app_module, "providers", [
        provider(name="NVIDIA", models=["a", "b"], disabled=["b"])
    ])
    monkeypatch.setattr(app_module, "health_status", {
        "NVIDIA||a": {"status": "ok"},
        "NVIDIA||b": {"status": "ok"},
    })
    cands = app_module.pick_available_models()
    models = [m for _, m in cands]
    assert "a" in models
    assert "b" not in models


def test_pick_respects_health(monkeypatch):
    monkeypatch.setattr(app_module, "providers", [
        provider(name="NVIDIA", models=["a", "b"])
    ])
    monkeypatch.setattr(app_module, "health_status", {
        "NVIDIA||a": {"status": "ok"},
        "NVIDIA||b": {"status": "fail"},
    })
    cands = app_module.pick_available_models()
    models = [m for _, m in cands]
    assert "a" in models
    assert "b" not in models


def test_pick_explicit_model_without_prefix_bypasses_health(monkeypatch):
    # 显式指定裸模型名，跳过健康检查，直接透传给上游
    monkeypatch.setattr(app_module, "providers", [
        provider(name="NVIDIA", models=["a", "b"])
    ])
    monkeypatch.setattr(app_module, "health_status", {
        "NVIDIA||a": {"status": "ok"},
        "NVIDIA||b": {"status": "fail"},
    })
    cands = app_module.pick_available_models("b")
    models = [m for _, m in cands]
    assert "b" in models  # 显式指定时不过滤，直接透传


def test_pick_force_bypasses_health(monkeypatch):
    monkeypatch.setattr(app_module, "providers", [
        provider(name="NVIDIA", models=["a", "b"])
    ])
    monkeypatch.setattr(app_module, "health_status", {
        "NVIDIA||a": {"status": "ok"},
        "NVIDIA||b": {"status": "fail"},
    })
    cands = app_module.pick_available_models("b", force=True)
    models = [m for _, m in cands]
    assert "b" in models


def test_pick_returns_empty_when_no_providers(monkeypatch):
    monkeypatch.setattr(app_module, "providers", [])
    monkeypatch.setattr(app_module, "health_status", {})
    assert app_module.pick_available_models() == []


# ============================================================
# 熔断
# ============================================================
def test_circuit_opens_after_threshold(monkeypatch):
    monkeypatch.setattr(app_module, "circuit_breaker", {})
    k = "X||y"
    for _ in range(app_module.CIRCUIT_FAIL_THRESHOLD):
        app_module.record_fail(k)
    assert app_module.is_circuit_open(k) is True


def test_circuit_resets_on_success(monkeypatch):
    monkeypatch.setattr(app_module, "circuit_breaker", {})
    k = "X||y"
    app_module.record_fail(k)
    app_module.record_success(k)
    assert app_module.is_circuit_open(k) is False


# ============================================================
# 质量分
# ============================================================
def test_quality_optimistic_when_no_data(monkeypatch):
    monkeypatch.setattr(app_module, "model_quality", {})
    assert app_module.get_quality_score("missing") == 1.0


def test_quality_tracks_ok_fail(monkeypatch):
    monkeypatch.setattr(app_module, "model_quality", {})
    k = "X||y"
    app_module.update_model_quality(k, {"status": "ok", "latency_ms": 100})
    app_module.update_model_quality(k, {"status": "ok", "latency_ms": 200})
    app_module.update_model_quality(k, {"status": "fail"})
    assert app_module.get_quality_score(k) == pytest.approx(2 / 3)


# ============================================================
# is_1m_model
# ============================================================
def test_is_1m_model_true(monkeypatch):
    monkeypatch.setattr(app_module, "model_details", {})
    # context_limits 里有 1M 的
    assert app_module.is_1m_model("deepseek-ai/deepseek-v4-flash") is True


def test_is_1m_model_false(monkeypatch):
    monkeypatch.setattr(app_module, "model_details", {})
    assert app_module.is_1m_model("sensenova-u1-fast") is False
