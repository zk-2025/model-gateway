# 模型 API 网关

多 LLM 提供商反向代理 + 健康巡检面板，提供 OpenAI 兼容的 `/v1/*` 接口。

## 快速开始

```bash
pip install -r requirements.txt
python app.py
# 访问 http://127.0.0.1:8000
```

首次启动会自动生成 `config.json`，包含：
- `local_api_key`：客户端调用 `/v1/*` 的密钥
- `admin_token`：管理面板 `/api/*` 的令牌

在 `providers.json` 中配置上游提供商。

## 鉴权说明

| 接口 | 凭据 |
|------|------|
| `/v1/chat/completions`、`/v1/models` | `Authorization: Bearer <local_api_key>` |
| `/api/*`（管理面板） | `Authorization: Bearer <admin_token>`（也接受 local_api_key） |

管理面板首次打开需输入 `admin_token`（从 `config.json` 获取）。

## 智能路由

- `auto-router-1m`：在所有支持 1M 上下文的健康模型中按可用率/延迟择优。
- `auto-router-1m` 自动判定：模型 `context_length >= 1048576` 即纳入候选（见 `models_meta.json`）。

## 配置文件

- `config.json`：本地密钥
- `providers.json`：上游提供商列表
- `models_meta.json`：模型别名 / 上下文长度 / 描述 / 非对话关键词
- `history.jsonl`：巡检历史（自动保留 30 天）

## 测试

```bash
pytest tests/ -v
```
