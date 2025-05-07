
# MCP Task Manager: 重新定义AI代理的执行边界

[![smithery badge](https://smithery.ai/badge/@chriscarrollsmith/taskqueue-mcp)](https://smithery.ai/server/@chriscarrollsmith/taskqueue-mcp)

## 为什么传统AI助手无法胜任复杂任务？

当今的AI助手面临一个根本性问题：**它们被限制在单一对话循环中**。这种架构导致：

- 🕒 **时间约束**：复杂任务被迫拆分为多个独立会话
- 🔄 **上下文丢失**：每次对话都需要重建任务背景
- 🧩 **执行割裂**：无法保持长时间、多步骤任务的连贯性

## 颠覆传统：双循环架构的突破

MCP Task Manager ([npm包: taskqueue-mcp](https://www.npmjs.com/package/taskqueue-mcp)) 彻底改变了这一现状，通过创新的**双循环分离架构**解锁了AI代理的全新能力：

```
用户对话循环 ⟷ [任务队列] ⟷ 代理执行循环
```

这不仅仅是一个任务管理工具，而是重新定义了AI代理执行模式的框架：

- 🔀 **循环解耦**：用户对话和代理执行被优雅分离，同时保持同步
- ⏱️ **突破时间限制**：代理可以持续执行复杂任务，不受单次对话时间限制
- 🔍 **全程可观察**：任务执行状态实时可见，进度透明
- 🛑 **精确控制点**：关键节点可由用户审批，确保执行质量

## 企业级应用的革命性影响

这种架构在企业环境中尤为强大：

- **长时间自动化工作流**：代理可以执行持续数小时甚至数天的复杂流程
- **多系统集成**：无缝连接企业内多个独立系统
- **可审计执行链**：提供完整的执行记录，满足合规要求
- **资源优化**：减少用户等待时间，提高AI资源利用效率

## 快速开始

### 基本设置

在Claude Desktop、Cursor或其他MCP客户端中配置工具：

```json
{
  "tools": {
    "taskqueue": {
      "command": "npx",
      "args": ["-y", "taskqueue-mcp"]
    }
  }
}
```

### 使用公共MCP服务器设置

如果你想使用公共MCP服务器而不是本地运行服务：

```json
{
  "tools": {
    "taskqueue": {
      "url": "https://taskqueue-public.vcorp.ai/rest/your-unique-chat-id",
      "stream": true
    }
  }
}
```

**注意**：请将`your-unique-chat-id`替换为你自己的唯一标识符，以确保你的数据与其他用户隔离。

CLI工具使用：

```bash
npx taskqueue --help
```

## 部署和服务访问

### 后台队列系统

该项目使用BullMQ作为后台任务队列系统，无法直接执行。推荐通过Docker Compose进行部署，然后通过streaming HTTP协议访问。

### Docker Compose部署

```bash
docker-compose up -d
```

这将启动所有必要的服务，包括Redis实例（BullMQ依赖）和MCP服务器。

### 公共MCP服务地址

```
https://taskqueue-public.vcorp.ai/rest/[your-chat-id]
```

## 实际应用场景

### 场景一：复杂数据分析项目

AI代理能够：
- 连接多个数据源
- 执行长时间数据清洗和转换
- 生成分析报告
- 在关键节点请求用户确认

所有这些**无需用户持续等待**或**多次重新开始对话**。

### 场景二：跨系统业务流程自动化

代理可以：
- 从CRM提取客户数据
- 在ERP系统中创建订单
- 触发物流系统流程
- 更新财务记录

这种多系统集成在传统单一对话循环中几乎不可能实现。

### 场景三：大型代码库重构

代理能够：
- 分析整个代码库架构
- 制定多阶段重构计划
- 执行代码修改并运行测试
- 在每个阶段完成后请求代码审查

## 核心工具概览

### 项目管理工具

- `list_projects`: 列出所有项目
- `read_project`: 获取特定项目详情
- `create_project`: 创建新项目及初始任务
- `delete_project`: 删除项目
- `add_tasks_to_project`: 向现有项目添加新任务
- `finalize_project`: 在所有任务完成后结束项目

### 任务管理工具

- `list_tasks`: 列出特定项目的所有任务
- `read_task`: 获取特定任务的详情
- `create_task`: 在项目中创建新任务
- `update_task`: 修改任务属性（标题、描述、状态）
- `delete_task`: 从项目中删除任务
- `approve_task`: 审批已完成的任务
- `get_next_task`: 获取项目中的下一个待处理任务
- `mark_task_done`: 标记任务已完成并提供详情

## 加入革新

MCP Task Manager代表了AI代理执行模式的新范式，突破了传统对话模型的局限。

无论你是构建企业级解决方案，还是探索AI长期自主性的新边界，这一框架都能为你提供前所未有的灵活性和能力。

## Bull Board 任务队列监控

TaskQueue MCP 集成了Bull Board提供直观的任务队列监控界面：

- **实时监控** - 查看队列状态、任务详情、失败原因
- **任务管理** - 重试失败任务、清空队列等操作
- **动态队列发现** - 自动发现和监控新创建的队列

通过以下URL访问：
```
http://localhost:3000/bull-board
```

## 许可证

MIT
