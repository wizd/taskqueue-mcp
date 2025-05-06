# 多租户环境中Redis键命名规范

## 问题背景

在多租户环境中，发现Redis键命名存在混乱，主要表现在以下几个方面：

1. 有些键使用了正确的租户前缀格式：`tenant:tenantid:project:proj-1:metadata`
2. 某些键使用了BullMQ默认的前缀：`bull:proj_proj-1:meta`
3. 还有一些键出现了错误的冒号格式：`:tenant_tenantid_proj_proj-1:meta`

这种不一致性导致了数据访问错误、监控困难和维护挑战。

## 根本原因分析

1. **BullMQ队列命名问题**：BullMQ默认使用`bull:`前缀，但在多租户环境中需要自定义前缀
2. **冒号处理不一致**：BullMQ队列名不能包含冒号，需要特殊处理
3. **Bull Board监控集成问题**：Bull Board集成时使用的队列名前缀不一致

## 解决方案

### 1. 规范化Redis键命名

- **项目元数据哈希表**：`tenant:tenantid:project:proj-N:metadata`
- **项目计数器**：`tenant:tenantid:taskqueue:counters:projects`
- **任务计数器**：`tenant:tenantid:taskqueue:counters:tasks`
- **项目任务集合**：`tenant:tenantid:project:proj-N:tasks`
- **项目队列名**：`tenant_tenantid_proj_proj-N` (注意无冒号)

### 2. 队列名处理逻辑

在BullMQ中创建队列时：
- 规范化租户前缀：`tenant:tenantid:` → `tenant_tenantid_`
- 设置空前缀：`prefix: ''`，避免BullMQ自动添加`bull:`前缀

### 3. Bull Board监控集成

- 队列适配器使用项目ID作为注册键
- 队列选项始终使用空前缀：`prefix: ''`
- 移除冗余参数：`addQueueToBoard(projectId)` 和 `removeQueueFromBoard(projectId)`

### 4. 清理错误格式的Redis键

- 使用`scripts/clean-redis-keys.js`工具清理格式错误的键
- 识别并清理前导冒号键和重复的项目键

## 最佳实践

1. **前缀一致性**：使用`BullMQTaskManager.setTenantId()`设置租户，自动处理所有前缀
2. **队列创建**：通过`BullMQService.getProjectQueue()`创建队列，确保前缀正确
3. **钩子函数**：每个公共方法使用`ensureTenantPrefixApplied()`确保前缀已正确应用
4. **清理监控**：定期检查Bull Board监控面板，确保队列名称一致性

## 关于队列命名中避免冒号的说明

BullMQ内部使用冒号作为键名分隔符，因此队列名称中不能包含冒号。我们的命名规则中：

- Redis键：使用`tenant:tenantid:project:proj-1:metadata`格式（含冒号）
- 队列名：使用`tenant_tenantid_proj_proj-1`格式（用下划线替代冒号）

这样既能保持Redis键的可读性，又能满足BullMQ对队列名的要求。 