# nvidia-free

**免费使用 NVIDIA NIM API，实现高并发 AI 编程体验。**

## 快速开始

### 第一步：安装 9router

```bash
npm install -g 9router
```

### 第二步：确保环境没问题

```bash
# 检查 Node.js 版本（需要 16+）
node -v

# 检查 npm 版本
npm -v

# 检查 9router 是否安装成功
9router --version
```

### 第三步：启动 9router 并接入英伟达账号轮询

```bash
# 启动 9router
9router
```

启动后会自动打开 Dashboard：`http://localhost:20128`

在 Dashboard 中配置：

1. 进入 **Providers** 页面
2. 选择 **ZCode** 提供商
3. 填入你的英伟达 API Key（从 https://build.nvidia.com/ 获取）
4. 支持多账号轮询，添加多个 Key 实现高并发

### 第四步：客户端配置

**Claude Code / Codex / Cursor 等工具：**

```
Endpoint: http://localhost:20128/v1
API Key:  [从 Dashboard 获取]
Model:    [选择支持的模型]
```

## 环境要求

- Node.js 16+
- npm 8+
- 稳定的网络连接

## 轮询配置

在 9router Dashboard 中可以配置：

- **多账号轮询**：添加多个英伟达 API Key
- **自动故障转移**：单个 Key 限流后自动切换
- **负载均衡**：智能分配请求到不同 Key

## 获取英伟达 API Key

访问 https://build.nvidia.com/ 注册并获取免费 API Key。

使用 `注册脚本.txt` 中的油猴脚本可以一键获取 Key。

## 许可证

MIT
