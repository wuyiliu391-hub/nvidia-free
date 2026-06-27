# nvidia-free

**免费使用 NVIDIA NIM API 接入 OpenAI Codex CLI 的代理工具。**

> ⚠️ **已修复的关键问题（v1.1）**：
> - ✅ **工具调用参数丢失**：`function_call` 的 `arguments` 字段被错误读取为 `output`，导致所有工具参数为空 → 模型无法执行写入操作
> - ✅ **沙盒模式限制**：Codex 默认设置 `sandbox_mode=read-only`，模型拒绝写入文件 → 自动修改为 `elevated`
> - ✅ **工具格式不兼容**：Codex 发送 Responses API 扁平格式工具定义，代理只支持 Chat Completions 嵌套格式 → 同时支持两种格式
> - ✅ **模型名映射**：Codex 发送 `gpt-5.4` 等 OpenAI 模型名，NVIDIA 上不存在 → 自动映射到配置的默认模型
> - ✅ **503 自动重试**：NVIDIA 过载时自动等待重试（最多 3 次）
> - ✅ **流式工具调用**：`output_item.added` 事件立即包含 `name` 和 `call_id`

将 OpenAI Responses API 格式转换为 NVIDIA Chat Completions API，通过多 key 轮询 + 全局速率控制规避限流。

## 功能

- **Responses API ↔ Chat Completions 双向转换**：Codex CLI 发送的 `/v1/responses` 请求自动转换
- **Tool Calling 完整支持**：扁平/嵌套两种工具格式，流式/非流式工具调用
- **SSE 流式转换**：NVIDIA Chat Completions SSE → Responses API 标准事件流
- **多 key 轮询**：多个 NVIDIA API key 自动轮换，429 时自动切换
- **全局速率控制**：令牌桶限流器，防止滑动窗口限流
- **沙盒模式修复**：自动解除 Codex 的只读限制
- **503 自动重试**：NVIDIA 过载时自动等待重试

## 快速开始

```bash
# 1. 克隆
git clone https://github.com/wuyiliu391-hub/nvidia-free.git
cd nvidia-free

# 2. 配置
cp config.json.example config.json
# 编辑 config.json，填入你的 NVIDIA API key（从 https://build.nvidia.com/ 获取）

# 3. 编译运行
go build -o nvidia-proxy.exe .
./nvidia-proxy.exe
```

## Codex CLI 配置

```bash
export OPENAI_BASE_URL=http://localhost:9099/v1
export OPENAI_API_KEY=any-value
codex
```

## API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/v1/responses` | POST | Responses API（Codex CLI 主路由） |
| `/v1/chat/completions` | POST | Chat Completions（备用） |
| `/v1/models` | GET | 模型列表 |
| `/health` | GET | 健康检查 |
| `/stats` | GET | 代理状态（key 数量、请求数等） |

## 配置说明

`config.json`：
```json
{
  "keys": [
    "nvapi-key1",
    "nvapi-key2",
    "nvapi-key3"
  ]
}
```

- 支持任意数量的 key，来自不同 NVIDIA 账号
- 每个 key 限制 38 次/分钟（留 2 次余量）
- 全局速率 1 次/秒（60 次/分钟）

## 技术细节

### 限流策略

NVIDIA 使用滑动窗口限流，同一秒内大量请求会触发 429。代理通过令牌桶算法控制全局发送速率，确保请求均匀分布。

### 格式转换

```
Codex CLI                    代理                    NVIDIA
  │                           │                       │
  │ POST /v1/responses        │                       │
  │ (Responses API)           │                       │
  │──────────────────────────>│                       │
  │                           │ POST /v1/chat/        │
  │                           │ completions           │
  │                           │ (Chat Completions)    │
  │                           │──────────────────────>│
  │                           │                       │
  │                           │<──────────────────────│
  │                           │ SSE stream            │
  │<──────────────────────────│                       │
  │ SSE stream                │                       │
  │ (Responses API events)    │                       │
```

### Tool Calling 转换

Codex CLI 使用 Responses API 扁平格式的工具定义：
```json
{"type":"function","name":"read_file","description":"...","parameters":{...}}
```

代理将其转换为 Chat Completions 嵌套格式：
```json
{"type":"function","function":{"name":"read_file","description":"...","parameters":{...}}}
```

同时清理 NVIDIA 不支持的字段（`additionalProperties`、`strict`）。

## 修复历史

### v1.1 (2026-06-27)

**核心修复：Codex 工具调用不工作**

问题现象：接入 Codex 后，模型只会"说"要做什么，但不真正执行工具调用（不写入文件、不执行命令）。

根因分析：
1. **`arguments` 字段读取错误**：代理在处理 `function_call` 项目时，错误地读取 `output` 字段（不存在）而不是 `arguments` 字段，导致工具参数全部丢失为 `{}`。模型收到空参数的工具调用 → 困惑 → 放弃执行。
2. **沙盒模式限制**：Codex 默认设置 `sandbox_mode=read-only`，明确告诉模型"你只能读文件"。模型遵循指令，拒绝调用写入类工具。
3. **工具格式不兼容**：Codex 发送 Responses API 扁平格式 `{"type":"function","name":"..."}` ，代理只支持 Chat Completions 嵌套格式 `{"type":"function","function":{"name":"..."}}`。导致工具定义丢失，模型没有可用工具。
4. **模型名映射缺失**：Codex 发送 `gpt-5.4` 等 OpenAI 模型名，NVIDIA 上不存在，请求 404 失败。
5. **503 无重试**：NVIDIA 过载时直接报错，不自动重试。

修复方案：
- 读取 `arguments` 字段而非 `output`
- 自动将 `sandbox_mode=read-only` 修改为 `elevated`
- 同时支持扁平和嵌套两种工具格式
- 非 NVIDIA 模型名自动映射到 `default_model`
- 503 自动重试（最多 3 次，递增等待）

### v1.0 (2026-06-26)

初始版本：基本的 Responses API ↔ Chat Completions 转换。

## 许可证

MIT
