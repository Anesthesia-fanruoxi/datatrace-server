# 减少 SSE 广播次数优化

## 问题描述

在启动或停止任务时，后端短时间内多次推送 `task_detail` 事件，导致前端收到重复推送。

---

## 问题表现

- 启动全量同步：收到多次 detail 推送
- 启动增量同步：收到多次 detail 推送
- 停止任务：收到 1 次 detail 推送

---

## 原因

任务启动的不同阶段都会触发状态变更：
1. 任务状态更新为 running → 推送
2. 切换到 initialize 步骤 → 推送
3. 切换到 sync_data 步骤 → 推送

---

## 优化方案

### 1. 合并推送

将短时间内多次状态变更合并为一次推送。

### 2. 按需推送

只在关键节点推送 detail：
- 任务启动完成
- 任务停止
- 任务失败

### 3. 前端去重

前端收到 `task_detail` 事件时，对比当前状态，相同则忽略。

---

## 当前实现

通过 EventBus 事件驱动推送，SSEHub 按 topic 过滤。
前端 `useTaskStore` 接收事件后直接更新 state，Vue 响应式系统自动处理重复更新。
# 减少SSE广播次数优化

## 问题描述

在启动或停止任务时，后端会在短时间内多次调用 `BroadcastTaskDetailUpdate()`，导致前端收到重复的任务详情推送。

### 问题表现

- 启动全量同步任务：收到2次 detail 推送
- 启动增量同步任务：收到3次 detail 推送
- 停止任务：收到1次 detail 推送（正常）

### 原因分析

在任务启动的不同阶段都会广播任务详情更新：

**增量同步启动时：**
1. 任务状态更新为运行 → 广播（第1次）
2. 切换到 initialize 步骤 → 广播（第2次）
3. 切换到 sync_data 步骤 → 广播（第3次）
4. 切换到 incremental 步骤 → 广播（第4次，但这个是在全量完成后）

**全量同步启动时：**
1. 任务状态更新为运行 → 广播（第1次）
2. 切换到 sync_data 步骤 → 广播（第2次）

## 优化方案

### 优化原则

1. 只在关键状态变化时广播
2. 中间步骤切换不广播（SSE会定时推送，前端会自动更新）
3. 保留任务启动和结束时的广播

### 修改内容

#### 1. services/incremental_sync_helpers.go

**移除 initialize 步骤的广播：**
```go
// 6. 更新任务步骤
database.DB.Model(&models.SyncTask{}).
    Where("id = ?", s.taskID).
    Update("current_step", "initialize")
progressManager.UpdateTaskStep(s.taskID, "initialize")

// 注释：不在此处广播，避免启动时重复推送
```

**移除 sync_data 步骤的广播：**
```go
database.DB.Model(&models.SyncTask{}).
    Where("id = ?", s.taskID).
    Update("current_step", "sync_data")
progressManager.UpdateTaskStep(s.taskID, "sync_data")

// 注释：不在此处广播，避免启动时重复推送
```

#### 2. services/task_full_sync_starter.go

**移除全量同步中 sync_data 步骤的广播：**
```go
// 更新任务步骤
database.DB.Model(&models.SyncTask{}).
    Where("id = ?", taskID).
    Update("current_step", "sync_data")
progressManager.UpdateTaskStep(taskID, "sync_data")

// 注释：不在此处广播，避免启动时重复推送
```

#### 3. services/incremental_sync.go

**保留切换到 incremental 步骤的广播（重要阶段切换）：**
```go
// 8. 更新任务步骤为增量消费
database.DB.Model(&models.SyncTask{}).
    Where("id = ?", s.taskID).
    Update("current_step", "incremental")

// 广播任务详情更新（切换到增量阶段，需要通知前端）
s.sseService.BroadcastTaskDetailUpdate(s.taskID)
```

## 优化效果

### 修改后的广播次数

**全量同步启动：**
- 启动时：1次广播 ✅
- 完成时：1次广播 ✅
- 总计：2次（减少1次）

**增量同步启动：**
- 启动时：1次广播 ✅
- 切换到增量阶段：1次广播 ✅
- 完成时：1次广播 ✅
- 总计：3次（减少2次，且分散在不同时间点）

**停止任务：**
- 停止时：1次广播 ✅
- 总计：1次（无变化）

### 性能提升

1. 减少了启动瞬间的重复推送
2. 降低了网络流量
3. 减少了前端的重复渲染
4. 保持了功能完整性（SSE定时推送会更新中间状态）

## 注意事项

1. 前端依然会通过SSE定时推送获取最新的任务状态
2. 关键的阶段切换（如进入增量阶段）仍然会立即广播
3. 任务启动和结束时的广播保持不变

## 相关文件

- `services/incremental_sync_helpers.go`
- `services/task_full_sync_starter.go`
- `services/incremental_sync.go`
- `services/task_control_service.go`
- `services/task_sse_service.go`

## 修改日期

2024-XX-XX
