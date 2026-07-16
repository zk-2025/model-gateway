# 📢 系统公告

## 🔄 v1.4.1 更新说明

本次更新主要优化了**思考过程（reasoning）的透传处理**：

- 上游返回的 `reasoning_content` 现在**原样透传**给客户端，不再合并进 `content`；
- 移除了此前在流式输出中自动插入的 ```` ``` ```` 代码围栏；
- 兼容原生思考过程展示（如支持 reasoning 的模型），由客户端自行决定如何渲染。

> ⚠️ 如果你之前依赖网关把思考内容包进正文，升级后需由客户端自行处理 `reasoning_content` 字段。

---

## 🆓 免费接口申请地址

本网关聚合多家免费额度，需自行注册并获取 API Key 后填入"上游提供商"：

| 提供商 | 申请 / 获取 Key 地址 |
|--------|----------------------|
| NVIDIA（NIM） | https://build.nvidia.com/ |
| 商汤 SenseNova | https://www.sensenova.cn/ |
| 魔搭 ModelScope | https://modelscope.cn/my/myaccesstoken |
| Google Gemini | https://aistudio.google.com/apikey |

> 各平台免费额度政策以官网最新说明为准。

---

## 📘 使用详细手册

完整图文教程（添加提供商、路由组、监控面板、系统托盘常驻等）：
https://pv284bk9no6.feishu.cn/wiki/HCOuwXuZGibDUGkWLlpcQuiLnDf?from=from_copylink

---

⭐ 如果这个项目对你有帮助，欢迎点 Star 支持！你的 Star 是持续更新的动力。
