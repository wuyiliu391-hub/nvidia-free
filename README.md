# nvidia-free

**免费使用 NVIDIA NIM API，实现 Coding Plan 级别的高并发 AI 编程体验。**

完美适配 **Codex CLI**、**Hermes** 等 AI 编程工具，支持 Responses API 和原生 Chat Completions API 双接口。

> 💡 **10 个免费 key = $200/月 Coding Plan 体验**

## 功能

- **双 API 支持**：Responses API + 原生 Chat Completions API
- **Codex CLI 完美适配**：自动转换 Responses API 格式
- **Hermes 原生接入**：直接配置 `openai_chat` 模式
- **Tool Calling 完整支持**：扁平/嵌套两种工具格式，流式/非流式工具调用
- **智能 key 轮询**：优先选最空闲 key，503 后自动冷却
- **沙盒模式修复**：自动解除 Codex 的只读限制
- **429/503 自动重试**：过载时自动等待重试并换 key

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

## 客户端配置

### Codex CLI

编辑 `~/.codex/config.toml`：

```toml
model = "z-ai/glm-5.1"
api_base = "http://localhost:9099/v1"
wire_api = "responses"  # 或 "chat"
```

### Hermes

编辑 `~/.hermes/config.yaml`：

```yaml
custom_providers:
- name: nvidia
  base_url: http://localhost:9099/v1
  api_key: nv-proxy
  api_mode: openai_chat
  models:
    z-ai/glm-5.1:
      context_length: 131072
      name: GLM-5.1
    moonshotai/kimi-k2.5:
      context_length: 131072
      name: Kimi K2.5
    deepseek/deepseek-r1:
      context_length: 131072
      name: DeepSeek R1
  model: z-ai/glm-5.1
model:
  default: z-ai/glm-5.1
  provider: nvidia
```

### 环境变量（通用）

```bash
export OPENAI_BASE_URL=http://localhost:9099/v1
export OPENAI_API_KEY=any-value
```

## API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/v1/responses` | POST | Responses API（Codex 默认） |
| `/v1/chat/completions` | POST | Chat Completions API（原生） |
| `/v1/models` | GET | 模型列表 |
| `/health` | GET | 健康检查 |
| `/stats` | GET | 代理状态（key 数量、请求数等） |

## 支持的模型

| 模型 | 说明 |
|------|------|
| `z-ai/glm-5.1` | 智谱 GLM-5.1（默认） |
| `moonshotai/kimi-k2.5` | 月之暗面 Kimi |
| `deepseek/deepseek-r1` | DeepSeek R1 |
| `qwen/qwen3-235b-a22b` | 通义千问 3 |

## 配置说明

`config.json`：
```json
{
  "keys": [
    "nvapi-key1",
    "nvapi-key2",
    "nvapi-key3"
  ],
  "default_model": "z-ai/glm-5.1"
}
```

## 高并发推荐：10 个 key

| Key 数量 | 总容量 | 体感 | 成本 |
|---------|--------|------|------|
| 1 个 | 30/分钟 | 偶尔卡顿 | 免费 |
| 5 个 | 150/分钟 | 日常可用 | 免费 |
| **10 个** | **300/分钟** | **≈ Coding Plan** | **免费** |
| 20 个 | 600/分钟 | 超高并发 | 免费 |

> 💰 OpenAI Coding Plan $200/月 ≈ 10 个免费 NVIDIA key

## 智能轮询策略

代理会自动：
1. **优先选最空闲的 key**：剩余配额最多的 key 优先
2. **失败冷却**：503 后的 key 冷却 5 秒，避免连续失败
3. **动态评分**：`分数 = 剩余配额 - 失败次数×2`
4. **自动重试**：503/429 自动换 key 重试（最多 3 次）

## 工作原理

### Responses API 模式（Codex 默认）
```
Codex CLI → Responses API → 代理转换 → Chat Completions API → NVIDIA NIM
    ↑                                                              ↓
    ←────── Responses API ←── 代理转换 ←── Chat Completions ←─────┘
```

### Chat Completions API 模式（Hermes / 通用）
```
客户端 → Chat Completions API → 代理转发 → NVIDIA NIM
    ↑                                          ↓
    ←────── Chat Completions ←─────────────────┘
```

## 更新历史

### v1.3 (2026-06-27)

- ✨ 新增 Hermes 原生接入支持
- ✨ 新增智能 key 选择（优先最空闲）
- ✨ 新增 503 失败冷却机制
- 🔧 优化高并发体验，推荐 10 个 key

### v1.2 (2026-06-27)

- ✨ 新增原生 Chat Completions API 端点
- 🔧 429/503 自动重试并换 key
- 🔧 thinking/reasoning_effort 参数自动剥离

### v1.1 (2026-06-27)

- ✅ 修复 `arguments` 字段读取错误
- ✅ 修复沙盒模式限制
- ✅ 修复工具格式兼容性
- ✅ 新增 503 自动重试

### v1.0 (2026-06-26)

初始版本：基本的 Responses API ↔ Chat Completions 转换。

## 许可证

MIT
