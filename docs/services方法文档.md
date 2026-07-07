# Services 方法文档

## 文档说明

本文档列出 `services/` 目录下所有文件的核心类型和方法。

**文档版本**: v2.0
**最后更新**: 2026-07

---

## 文件清单

| 文件 | 行数 | 核心类型 | 职责 |
|---|---|---|---|
| cache_store.go | 371 | CacheStore | 缓存抽象接口（Memory/Redis） |
| event_bus.go | 48 | EventBus | 进程内事件发布订阅 |
| sse_hub.go | 204 | SSEHub | 统一 SSE 推送入口 |
| credential_service.go | 150 | CredentialService | 凭据 CRUD + 加密 |
| datasource_service.go | 371 | DataSourceService | 数据源 CRUD + 连接测试 |
| task_service.go | 366 | TaskService | 任务 CRUD + 配置管理 |
| task_control_service.go | 315 | TaskControlService | 任务启停控制 |
| task_execution_manager.go | 85 | TaskExecutionManager | 运行实例管理 |
| task_progress_manager.go | 395 | TaskProgressManager | 进度管理（内存 map） |
| task_log_service.go | 154 | TaskLogService | 任务日志文件持久化 |
| task_table_status_service.go | 145 | TaskTableStatusService | 表/步骤状态管理 |
| task_full_sync_starter.go | 363 | FullSyncStarter | 全量→增量编排 |
| sync_engine.go | 361 | SyncEngine | 全量同步核心引擎 |
| sync_init.go | 259 | — | 目标库初始化 |
| mysql_reader.go | 179 | MySQLReader | 分批读取器 |
| mysql_writer.go | 119 | MySQLWriter | 批量写入器 |
| foreign_key_analyzer.go | 99 | ForeignKeyAnalyzer | 外键拓扑排序 |
| adaptive_config_calculator.go | 65 | AdaptiveConfigCalculator | 自适应批次计算 |
| incremental_sync.go | 396 | IncrementalSync | 增量同步总控 |
| incremental_sync_helpers.go | 53 | — | 增量同步辅助 |
| incremental_consumer.go | 131 | IncrementalConsumer | 事件消费引擎 |
| incremental_consumer_event_handler.go | 135 | ConsumerEventHandler | 事件处理 |
| incremental_consumer_mapper.go | 51 | ConsumerMapper | 源→目标映射 |
| incremental_stats_service.go | 95 | IncrementalStatsService | 增量统计 |
| binlog_listener.go | 290 | BinlogListener | Binlog 监听（canal） |
| binlog_queue.go | 168 | BinlogQueue | 事件队列（内存/Redis） |
| health_check_service.go | 169 | HealthCheckService | 数据源健康检查 |

---

## 横切服务

### CacheStore

```go
type CacheStore interface {
    Get(key string) (string, error)
    Set(key string, value interface{}, ttl time.Duration) error
    Del(key string) error
    HGet(key, field string) (string, error)
    HSet(key, field string, value interface{}) error
    HDel(key string, fields ...string) error
    HGetAll(key string) (map[string]string, error)
    Exists(key string) (bool, error)
}
```

实现：`MemoryCache`（默认）、`RedisCache`（Redis 启用时）

### EventBus

```go
func NewEventBus() *EventBus
func (b *EventBus) Subscribe(topic string, handler func(interface{}))
func (b *EventBus) Publish(topic string, data interface{})
```

### SSEHub

```go
func NewSSEHub() *SSEHub
func (h *SSEHub) AddClient(clientID string, topics []string, ch chan string)
func (h *SSEHub) RemoveClient(clientID string)
func (h *SSEHub) PublishJSON(topic string, eventType string, data interface{})
func (h *SSEHub) Run()  // goroutine
```

---

## 基础管理服务

### CredentialService

```go
func NewCredentialService(db *gorm.DB, crypto *utils.CryptoService) *CredentialService
func (s *CredentialService) List() ([]models.Credential, error)
func (s *CredentialService) GetByID(id string) (*models.Credential, error)
func (s *CredentialService) Create(cred *models.Credential) error
func (s *CredentialService) Update(cred *models.Credential) error
func (s *CredentialService) Delete(id string) error
func (s *CredentialService) TestConnection(id string) (*TestResult, error)
func (s *CredentialService) MaskPassword(cred *models.Credential) *models.Credential
```

### DataSourceService

```go
func NewDataSourceService(db *gorm.DB, crypto *utils.CryptoService, credSvc *CredentialService) *DataSourceService
func (s *DataSourceService) List() ([]models.DataSource, error)
func (s *DataSourceService) GetByID(id string) (*models.DataSource, error)
func (s *DataSourceService) Create(ds *models.DataSource) error
func (s *DataSourceService) Update(ds *models.DataSource) error
func (s *DataSourceService) Delete(id string) error
func (s *DataSourceService) TestConnection(id string) (*TestResult, error)
func (s *DataSourceService) TestConnectionByDSN(dsn string) (*TestResult, error)
func (s *DataSourceService) GetDatabases(id string) ([]string, error)
func (s *DataSourceService) GetTables(id string) ([]string, error)
func (s *DataSourceService) GetDatabaseTables(id string) (map[string][]string, error)
func (s *DataSourceService) GetTableColumns(id, database, table string) ([]ColumnInfo, error)
func (s *DataSourceService) BuildDSN(ds *models.DataSource) (string, error)
```

### TaskService

```go
func NewTaskService(db *gorm.DB) *TaskService
func (s *TaskService) List() ([]models.SyncTask, error)
func (s *TaskService) GetByID(id string) (*models.SyncTask, error)
func (s *TaskService) Create(task *models.SyncTask) error
func (s *TaskService) Update(task *models.SyncTask) error
func (s *TaskService) Delete(id string) error
func (s *TaskService) GetConfig(id string) (*TaskConfig, error)
func (s *TaskService) UpdateConfig(id string, config *TaskConfig) error
func (s *TaskService) GetConfigView(id string) (*ConfigView, error)
func (s *TaskService) GetStats() (*TaskStats, error)
```

---

## 任务控制服务

### TaskControlService

```go
func NewTaskControlService(db *gorm.DB, taskSvc *TaskService, execMgr *TaskExecutionManager,
    syncEngine *SyncEngine, fullSync *FullSyncStarter, incrSync *IncrementalSync,
    progressMgr *TaskProgressManager, eventBus *EventBus,
    logSvc *TaskLogService, tableStatusSvc *TaskTableStatusService) *TaskControlService

func (s *TaskControlService) StartTask(taskID string) error
func (s *TaskControlService) StopTask(taskID string) error
func (s *TaskControlService) PauseTask(taskID string) error
func (s *TaskControlService) ResumeTask(taskID string) error
func (s *TaskControlService) GetProgress(taskID string) (*TaskProgress, error)
func (s *TaskControlService) GetLogs(taskID string, since time.Time) ([]LogEntry, error)
func (s *TaskControlService) ClearLogs(taskID string) error
func (s *TaskControlService) GetTableStatus(taskID string) (map[string]string, error)
func (s *TaskControlService) GetStepStatus(taskID string) (map[string]string, error)
```

### TaskExecutionManager

```go
func NewTaskExecutionManager() *TaskExecutionManager
func (m *TaskExecutionManager) Register(taskID string, cancel context.CancelFunc)
func (m *TaskExecutionManager) Unregister(taskID string)
func (m *TaskExecutionManager) GetCancel(taskID string) (context.CancelFunc, bool)
func (m *TaskExecutionManager) IsRunning(taskID string) bool
```

### TaskProgressManager

```go
func NewTaskProgressManager(eventBus *EventBus) *TaskProgressManager
func (m *TaskProgressManager) InitProgress(taskID string, tables []string)
func (m *TaskProgressManager) StartTable(taskID, tableKey string, totalRows int64)
func (m *TaskProgressManager) UpdateProgress(taskID, tableKey string, syncedRows int64)
func (m *TaskProgressManager) CompleteTable(taskID, tableKey string)
func (m *TaskProgressManager) FailTable(taskID, tableKey string, errMsg string)
func (m *TaskProgressManager) GetProgress(taskID string) *TaskProgress
func (m *TaskProgressManager) SetStatus(taskID string, status string)
```

---

## 辅助服务

### TaskLogService

```go
func NewTaskLogService(logDir string) *TaskLogService
func (s *TaskLogService) AppendLog(taskID, category, message string)
func (s *TaskLogService) ReadLogs(taskID string, since time.Time) ([]LogEntry, error)
func (s *TaskLogService) ClearLogs(taskID string) error
```

### TaskTableStatusService

```go
func NewTaskTableStatusService(cache CacheStore, eventBus *EventBus) *TaskTableStatusService
func (s *TaskTableStatusService) InitSteps(taskID string, steps []string)
func (s *TaskTableStatusService) UpdateStep(taskID, step, status string)
func (s *TaskTableStatusService) GetStepStatus(taskID string) (map[string]string, error)
func (s *TaskTableStatusService) SetTableStatus(taskID, table, status string)
func (s *TaskTableStatusService) GetTableStatus(taskID string) (map[string]string, error)
func (s *TaskTableStatusService) ClearTaskTables(taskID string) error
```

### HealthCheckService

```go
func NewHealthCheckService(dsSvc *DataSourceService, cache CacheStore, eventBus *EventBus) *HealthCheckService
func (s *HealthCheckService) Start(interval time.Duration)
func (s *HealthCheckService) Stop()
func (s *HealthCheckService) CheckAll() map[string]*HealthStatus
func (s *HealthCheckService) CheckOne(dsID string) (*HealthStatus, error)
```

---

## 全量同步

### SyncEngine

```go
func NewSyncEngine(progressMgr *TaskProgressManager, eventBus *EventBus) *SyncEngine
func (e *SyncEngine) SyncTable(ctx context.Context, reader *MySQLReader, writer *MySQLWriter,
    sourceDB, sourceTable, targetDB, targetTable string,
    fields []string, batchSize int, taskID string, tableTimeoutMin int) *SyncTableResult
```

### MySQLReader

```go
func NewMySQLReader(db *sql.DB) *MySQLReader
func (r *MySQLReader) GetColumns(ctx context.Context, database, table string) ([]ColumnInfo, error)
func (r *MySQLReader) GetRowCount(ctx context.Context, database, table string) (int64, error)
func (r *MySQLReader) ReadBatch(ctx context.Context, database, table string,
    columns []string, offset, limit int) ([][]interface{}, error)
```

### MySQLWriter

```go
func NewMySQLWriter(db *sql.DB) *MySQLWriter
func (w *MySQLWriter) CreateDatabase(ctx context.Context, database string) error
func (w *MySQLWriter) CreateTable(ctx context.Context, targetDB, targetTable, ddl string) error
func (w *MySQLWriter) TruncateTable(ctx context.Context, database, table string) error
func (w *MySQLWriter) BatchInsert(ctx context.Context, database, table string,
    columns []string, rows [][]interface{}) error
```

### ForeignKeyAnalyzer

```go
func NewForeignKeyAnalyzer() *ForeignKeyAnalyzer
func (a *ForeignKeyAnalyzer) Analyze(ctx context.Context, db *sql.DB,
    database string, tables []string) ([]string, error)  // 返回拓扑排序后的表列表
```

### AdaptiveConfigCalculator

```go
func NewAdaptiveConfigCalculator() *AdaptiveConfigCalculator
func (c *AdaptiveConfigCalculator) CalcBatchSize(columns int, totalRows int64) int
```

### FullSyncStarter

```go
func NewFullSyncStarter(db *gorm.DB, taskSvc *TaskService, syncEngine *SyncEngine,
    progressMgr *TaskProgressManager, eventBus *EventBus,
    logSvc *TaskLogService, tableStatusSvc *TaskTableStatusService) *FullSyncStarter
func (s *FullSyncStarter) Start(ctx context.Context, taskID string, config *TaskConfig,
    sourceDSN string, targets []TargetInfo) error
```

---

## 增量同步

### IncrementalSync

```go
func NewIncrementalSync(db *gorm.DB, eventBus *EventBus, statsSvc *IncrementalStatsService,
    fullSync *FullSyncStarter, tableStatusSvc *TaskTableStatusService) *IncrementalSync
func (s *IncrementalSync) Start(ctx context.Context, taskID string, config *TaskConfig,
    sourceDSN string, targets []TargetInfo) error
func (s *IncrementalSync) Stop() error
```

### BinlogListener

```go
func NewBinlogListener(queues []BinlogQueue, eventBus *EventBus, taskID string) *BinlogListener
func (l *BinlogListener) Start(ctx context.Context, dsn string, pos mysql.Position, gtid mysql.GTIDSet) error
func (l *BinlogListener) Stop()
func (l *BinlogListener) SetFilterTables(tables []string)
func (l *BinlogListener) AddFilterTable(table string)
func (l *BinlogListener) RemoveFilterTable(table string)
```

### BinlogQueue

```go
type BinlogQueue interface {
    Push(event *BinlogEvent) error
    Pull() (*BinlogEvent, error)
    Ack(offset int64) error
    Len() int
}

// 实现
func NewMemoryQueue() *MemoryQueue
func NewRedisQueue(client *redis.Client, key string) *RedisQueue
```

### IncrementalConsumer

```go
func NewIncrementalConsumer(queue BinlogQueue, mapper *ConsumerMapper,
    handler *ConsumerEventHandler, statsSvc *IncrementalStatsService,
    eventBus *EventBus, taskID string) *IncrementalConsumer
func (c *IncrementalConsumer) Start(ctx context.Context)
func (c *IncrementalConsumer) Stop()
```

### IncrementalStatsService

```go
func NewIncrementalStatsService(cache CacheStore, eventBus *EventBus) *IncrementalStatsService
func (s *IncrementalStatsService) UpdateStats(taskID string, stats *IncrementalStats)
func (s *IncrementalStatsService) GetStats(taskID string) (*IncrementalStats, error)
```
# Services 目录方法文档

本文档记录了 `services/` 目录下所有文件的方法和作用。

## 目录结构

```
services/
├── 全量同步相关
│   └── task_full_sync_starter.go   # 全量同步启动器
├── 增量同步相关
│   ├── incremental_sync.go         # 增量同步引擎
│   ├── incremental_sync_helpers.go # 增量同步辅助函数
│   ├── incremental_consumer.go     # 增量消费者
│   ├── incremental_consumer_event_handler.go # 增量消费事件处理器
│   ├── incremental_consumer_mapper.go # 增量消费映射器
│   ├── binlog_listener.go          # Binlog监听器
│   ├── binlog_queue.go             # Binlog队列
│   └── incremental_stats_service.go # 增量统计服务
├── 表结构同步相关
│   ├── table_structure_parser.go   # 表结构解析器
│   ├── table_structure_modifier.go # 表结构修改器
│   └── table_structure_alter.go    # 表结构对比和ALTER服务
├── 公共同步工具
│   ├── sync_engine.go              # 同步引擎核心
│   ├── sync_init.go                # 同步初始化（创库创表）
│   ├── sync_helpers.go             # 同步辅助函数（映射转换）
│   ├── mysql_reader.go             # MySQL读取器（数据处理）
│   ├── mysql_writer.go             # MySQL写入器（数据处理）
│   ├── foreign_key_analyzer.go     # 外键分析器（创库创表）
│   ├── task_foreign_key_sorter.go  # 任务外键排序器（创库创表）
│   └── adaptive_config_calculator.go # 自适应配置计算器
├── 任务管理相关
│   ├── task_service.go             # 任务服务
│   ├── task_control_service.go     # 任务控制服务
│   ├── task_execution_manager.go   # 任务执行管理器
│   ├── task_progress_manager.go    # 任务进度管理器
│   ├── task_progress_service.go    # 任务进度服务
│   ├── task_sse_service.go         # 任务SSE服务
│   ├── task_log_service.go         # 任务日志服务
│   └── config_cache_service.go     # 配置缓存服务
├── 数据源管理相关
│   ├── datasource_service.go       # 数据源服务
│   ├── datasource_test_service.go  # 数据源测试服务
│   ├── datasource_sse_service.go   # 数据源SSE服务
│   ├── credential_service.go       # 凭据服务
│   └── mysql_service.go            # MySQL元数据服务
└── 系统工具类
    ├── system_resource_detector.go # 系统资源检测器
    ├── table_name_validator.go     # 表名验证器
    └── log_file_watcher.go         # 日志文件监听器
```

---

## 文件详细说明
## 1. 全量同步相关文件

### 1.1 task_full_sync_starter.go - 全量同步启动器
**作用**: 全量同步任务的启动入口，控制整个全量同步流程

**主要方法**:
- `startFullSyncTask(taskID)` - 启动全量同步任务
- `startIncrementalTask(taskID)` - 启动增量同步任务

---

## 2. 增量同步相关文件

### 2.1 incremental_sync.go - 增量同步引擎
**作用**: 增量同步的总控引擎，协调Binlog监听、队列和消费

**主要方法**:
- `NewIncrementalSync(taskID)` - 创建增量同步引擎
- `Start()` - 启动增量同步
- `StartWithContext(ctx)` - 使用外部context启动增量同步
- `Stop()` - 停止增量同步
- `GetStatus()` - 获取增量同步状态

### 2.2 incremental_sync_helpers.go - 增量同步辅助函数
**作用**: 增量同步的辅助功能实现

**主要方法**:
- `initDatabaseConnections()` - 初始化数据库连接
- `closeDatabaseConnections()` - 关闭数据库连接
- `checkBinlogConfig()` - 检查Binlog配置
- `captureSnapshot()` - 捕获快照点
- `createQueue()` - 创建队列
- `startBinlogListener()` - 启动Binlog监听器
- `executeFullSync()` - 执行全量同步
- `startIncrementalConsumer()` - 启动增量消费
- `markFullSyncCompleted()` - 标记全量同步完成

### 2.3 incremental_consumer.go - 增量消费者
**作用**: 消费Binlog事件队列，将变更应用到目标数据库

**主要方法**:
- `NewIncrementalConsumer(config)` - 创建增量消费引擎
- `Start()` - 启动消费
- `consumeLoop()` - 消费循环
- `Stop()` - 停止消费

### 2.3 incremental_consumer_event_handler.go - 增量消费事件处理器
**作用**: 处理具体的INSERT/UPDATE/DELETE事件

**主要方法**:
- `processEvent(event)` - 处理单个事件
- `applyInsert(event)` - 应用INSERT事件
- `applyUpdate(event)` - 应用UPDATE事件
- `applyDelete(event)` - 应用DELETE事件

### 2.4 incremental_consumer_mapper.go - 增量消费映射器
**作用**: 映射源数据库表名到目标数据库表名，过滤字段

**主要方法**:
- `mapDatabaseAndTable(sourceDB, sourceTable)` - 映射数据库和表名
- `GetStatistics()` - 获取统计信息
- `getSelectedFields(sourceDB, sourceTable)` - 获取选中字段列表
- `filterEventFields(event)` - 根据配置过滤事件字段

### 2.5 binlog_listener.go - Binlog监听器
**作用**: 监听MySQL的Binlog事件，解析并推送到队列

**主要方法**:
- `NewBinlogListener(config)` - 创建Binlog监听器
- `Start()` - 启动监听
- `eventLoop(streamer)` - 事件循环
- `handleEvent(ev)` - 处理事件
- `handleInsert(ev)` - 处理INSERT事件
- `handleUpdate(ev)` - 处理UPDATE事件
- `handleDelete(ev)` - 处理DELETE事件
- `shouldProcess(database, table)` - 判断是否应该处理该表
- `Stop()` - 停止监听
- `GetCurrentPosition()` - 获取当前Binlog位置

### 2.6 binlog_queue.go - Binlog队列
**作用**: 提供内存队列和Redis队列两种实现，缓存Binlog事件

**队列接口方法**:
- `Push(event)` - 推送事件到队列
- `Pop(ctx)` - 从队列获取事件（阻塞）
- `Ack(eventID)` - 确认事件已处理
- `Close()` - 关闭队列
- `Size()` - 获取队列大小

**内存队列实现**:
- `NewMemoryQueue(bufferSize)` - 创建内存队列

**Redis队列实现**:
- `NewRedisQueue(client, taskID, consumerName)` - 创建Redis队列
- `GetPendingCount()` - 获取待处理消息数量
- `ClaimPendingMessages(idleTime)` - 认领超时的待处理消息

---

## 3. 表结构同步相关文件

### 3.1 table_structure_parser.go - 表结构解析器
**作用**: 解析CREATE TABLE语句，提取字段、索引、外键等信息

**主要方法**:
- `NewTableStructureParser()` - 创建表结构解析器
- `Parse(createSQL)` - 解析CREATE TABLE语句
- `splitDefinitions(content)` - 分割定义（处理逗号分隔）
- `parseField(def, structure)` - 解析字段定义
- `parsePrimaryKey(def, structure)` - 解析主键定义
- `parseIndex(def, structure)` - 解析索引定义
- `parseForeignKey(def, structure)` - 解析外键定义

### 3.2 table_structure_modifier.go - 表结构修改器
**作用**: 修改表结构定义，支持字段过滤等功能

**主要方法**:
- `NewTableStructureModifier()` - 创建表结构修改器
- `FilterFieldsAndRebuild(createSQL, selectedFields, newTableName)` - 过滤字段并重建CREATE TABLE语句
- `buildCreateSQL()` - 构建CREATE TABLE语句
- `replaceTableName()` - 替换表名
- `GenerateAlterSQL()` - 生成ALTER TABLE语句

### 3.3 table_structure_alter.go - 表结构对比和ALTER服务
**作用**: 对比源表和目标表结构差异，生成ALTER语句

**主要方法**:
- `NewTableStructureAlterService()` - 创建表结构对比服务
- `CompareAndAlter()` - 对比并执行ALTER
- `getCreateTableSQL()` - 获取表的CREATE TABLE语句
- `tableExists()` - 检查表是否存在
- `generateAlterSQLs()` - 生成ALTER SQL语句
- `extractFieldNames()` - 提取字段名列表
- `normalizeFieldDef()` - 标准化字段定义

---

## 4. 公共同步工具

### 3.1 sync_engine.go - 同步引擎核心
**作用**: 全量和增量同步的核心引擎，负责协调整个同步过程

**主要方法**:
- `NewSyncEngine()` - 创建同步引擎实例
- `loadTargetConns(targetIDs []string)` - 加载目标连接配置
- `ensureTargetTableExists()` - 确保目标表存在
- `createTableLike()` - 根据源表创建目标表
- `Worker(ctx, taskID, taskQueue, workerID)` - 工作器，处理同步任务队列
- `SyncTable(ctx, taskID, unitName)` - 同步单个表的数据

### 3.2 sync_init.go - 同步初始化
**作用**: 负责数据同步前的初始化工作，包括创建数据库和表结构（全量和增量共用）

**主要方法**:
- `InitializeWorker(ctx, taskID, unitNames)` - 初始化工作器
- `InitializeTable(ctx, taskID, unitName)` - 初始化单个表
- `dropTable(ctx, taskID, unitName)` - 删除表
- `createTable(ctx, taskID, unitName)` - 创建表
- `fetchSourceTableRows(ctx, taskID, task, config, sourcePassword)` - 获取源表行数

### 3.3 sync_helpers.go - 同步辅助函数
**作用**: 提供同步过程中的各种辅助功能（全量和增量共用）

**主要方法**:
- `failUnit(taskID, unitName, errMsg)` - 标记单元失败
- `pauseUnit(taskID, unitName, msg)` - 标记单元暂停
- `parseUnitName(unitName, config)` - 解析单元名称
- `safePercent(processed, total)` - 安全计算百分比
- `getSelectedFields(config, sourceDB, sourceTable)` - 获取选中字段列表
- `calculateAdaptiveBatchSize()` - 计算自适应批次大小

### 3.4 mysql_reader.go - MySQL读取器
**作用**: 从源MySQL数据库读取数据，支持批量读取和字段选择（全量和增量共用）

**主要方法**:
- `NewMySQLReader()` - 创建MySQL读取器
- `NewMySQLReaderWithFields()` - 创建支持字段选择的MySQL读取器
- `ReadBatch()` - 读取一批数据
- `GetTotalCount()` - 获取总记录数
- `HasMore()` - 是否还有更多数据
- `Reset()` - 重置读取器到初始位置
- `GetDatabaseCharset()` - 获取数据库字符集
- `Close()` - 关闭连接

### 3.5 mysql_writer.go - MySQL写入器
**作用**: 向目标MySQL数据库写入数据，支持批量写入和表结构创建（全量和增量共用）

**主要方法**:
- `NewMySQLWriter()` - 创建MySQL写入器
- `CreateDatabaseIfNotExists()` - 创建数据库（如果不存在）
- `WriteBatch(records)` - 批量写入数据
- `TruncateTable()` - 清空表
- `DropTable()` - 删除表
- `CreateTableLike()` - 根据源表结构创建表
- `CreateTableLikeWithFields()` - 根据源表结构创建表（支持字段过滤）
- `Close()` - 关闭连接

### 3.6 table_structure_parser.go - 表结构解析器
**作用**: 解析CREATE TABLE语句，提取字段、索引、外键等信息

**主要方法**:
- `NewTableStructureParser()` - 创建表结构解析器
- `Parse(createSQL)` - 解析CREATE TABLE语句
- `splitDefinitions(content)` - 分割定义（处理逗号分隔）
- `parseField(def, structure)` - 解析字段定义
- `parsePrimaryKey(def, structure)` - 解析主键定义
- `parseIndex(def, structure)` - 解析索引定义
- `parseForeignKey(def, structure)` - 解析外键定义

## 4. 公共同步工具

### 4.1 sync_engine.go - 同步引擎核心
**作用**: 全量和增量同步的核心引擎，负责协调整个同步过程

**主要方法**:
- `NewSyncEngine()` - 创建同步引擎实例
- `loadTargetConns(targetIDs []string)` - 加载目标连接配置
- `ensureTargetTableExists()` - 确保目标表存在
- `createTableLike()` - 根据源表创建目标表
- `Worker(ctx, taskID, taskQueue, workerID)` - 工作器，处理同步任务队列
- `SyncTable(ctx, taskID, unitName)` - 同步单个表的数据

### 4.2 sync_init.go - 同步初始化
**作用**: 负责数据同步前的初始化工作，包括创建数据库和表结构（全量和增量共用）

**主要方法**:
- `InitializeWorker(ctx, taskID, unitNames)` - 初始化工作器
- `InitializeTable(ctx, taskID, unitName)` - 初始化单个表
- `dropTable(ctx, taskID, unitName)` - 删除表
- `createTable(ctx, taskID, unitName)` - 创建表
- `fetchSourceTableRows(ctx, taskID, task, config, sourcePassword)` - 获取源表行数

### 4.3 sync_helpers.go - 同步辅助函数
**作用**: 提供同步过程中的各种辅助功能（全量和增量共用）

**主要方法**:
- `failUnit(taskID, unitName, errMsg)` - 标记单元失败
- `pauseUnit(taskID, unitName, msg)` - 标记单元暂停
- `parseUnitName(unitName, config)` - 解析单元名称
- `safePercent(processed, total)` - 安全计算百分比
- `getSelectedFields(config, sourceDB, sourceTable)` - 获取选中字段列表
- `calculateAdaptiveBatchSize()` - 计算自适应批次大小

### 4.4 mysql_reader.go - MySQL读取器
**作用**: 从源MySQL数据库读取数据，支持批量读取和字段选择（全量和增量共用）

**主要方法**:
- `NewMySQLReader()` - 创建MySQL读取器
- `NewMySQLReaderWithFields()` - 创建支持字段选择的MySQL读取器
- `ReadBatch()` - 读取一批数据
- `GetTotalCount()` - 获取总记录数
- `HasMore()` - 是否还有更多数据
- `Reset()` - 重置读取器到初始位置
- `GetDatabaseCharset()` - 获取数据库字符集
- `Close()` - 关闭连接

### 4.5 mysql_writer.go - MySQL写入器
**作用**: 向目标MySQL数据库写入数据，支持批量写入和表结构创建（全量和增量共用）

**主要方法**:
- `NewMySQLWriter()` - 创建MySQL写入器
- `CreateDatabaseIfNotExists()` - 创建数据库（如果不存在）
- `WriteBatch(records)` - 批量写入数据
- `TruncateTable()` - 清空表
- `DropTable()` - 删除表
- `CreateTableLike()` - 根据源表结构创建表
- `CreateTableLikeWithFields()` - 根据源表结构创建表（支持字段过滤）
- `Close()` - 关闭连接

### 4.6 foreign_key_analyzer.go - 外键分析器
**作用**: 分析表之间的外键依赖关系，提供拓扑排序功能

**主要方法**:
- `AnalyzeForeignKeys(database, tables)` - 分析数据库中的外键关系
- `TopologicalSort(tables, relations)` - 拓扑排序，返回删除顺序
- `SortTablesForDrop(db, database, tables)` - 对表进行排序，返回删除顺序
- `SortTablesForDropWithCheck()` - 对表进行排序，返回删除顺序和是否有外键依赖
- `SortTablesForDropWithFKList()` - 对表进行排序，返回删除顺序、是否有外键依赖、外键表列表

### 4.7 task_foreign_key_sorter.go - 任务外键排序器
**作用**: 对同步任务中的表进行外键依赖排序

### 4.8 adaptive_config_calculator.go - 自适应配置计算器
**作用**: 根据系统资源和表大小计算最优的线程数和批次大小

**主要方法**:
- `NewAdaptiveConfigCalculator()` - 创建自适应配置计算器
- `CalculateForTask(tableCount, avgTableSize)` - 为任务计算自适应配置
- `CalculateForTable(stats)` - 为单个表计算批次大小
- `GetTableStats(db, database, table)` - 获取表的统计信息
- `GetDefaultConfig()` - 获取默认配置

---

## 5. 任务管理相关文件

### 3.1 task_service.go - 任务服务
**作用**: 管理同步任务的CRUD操作

### 3.2 task_control_service.go - 任务控制服务
**作用**: 控制任务的启动、停止、暂停等操作

### 3.3 task_execution_manager.go - 任务执行管理器
**作用**: 管理任务执行实例，包括context和goroutine

### 3.4 task_progress_manager.go - 任务进度管理器
**作用**: 管理任务进度信息，支持内存缓存

### 3.5 task_progress_service.go - 任务进度服务
**作用**: 提供任务进度的查询和更新接口

### 3.6 task_sse_service.go - 任务SSE服务
**作用**: 提供任务进度的实时推送功能

### 3.7 task_log_service.go - 任务日志服务
**作用**: 管理任务执行日志的记录和查询

### 3.8 task_foreign_key_sorter.go - 任务外键排序器
**作用**: 对同步任务中的表进行外键依赖排序

---

## 4. 数据源管理相关文件

### 4.1 datasource_service.go - 数据源服务
**作用**: 管理数据源的CRUD操作

**主要方法**:
- `NewDataSourceService()` - 创建数据源服务
- `Create(req)` - 创建数据源
- `List()` - 获取数据源列表
- `GetByID(id)` - 根据ID获取数据源
- `Update(id, req)` - 更新数据源
- `Delete(id)` - 删除数据源
- `validate(req)` - 验证请求

### 4.2 datasource_test_service.go - 数据源测试服务
**作用**: 测试数据源连接是否正常

**主要方法**:
- `TestConnection(req)` - 测试数据源连接
- `TestConnectionByID(id)` - 根据ID测试数据源连接
- `testMySQLConnection(req)` - 测试MySQL连接
- `testElasticsearchConnection(req)` - 测试Elasticsearch连接

### 4.3 datasource_sse_service.go - 数据源SSE服务
**作用**: 提供数据源健康检查的实时推送功能

**主要方法**:
- `NewDataSourceSSEService()` - 获取数据源SSE服务单例
- `StartHealthCheck()` - 启动后台健康检查
- `checkAllDataSources()` - 检查所有数据源
- `testSingleDataSource(dsID)` - 测试单个数据源
- `AddClient(client)` - 添加客户端
- `RemoveClient(client)` - 移除客户端
- `SendCachedResults(client)` - 发送缓存的测试结果

### 4.4 credential_service.go - 凭据服务
**作用**: 管理数据库连接凭据的CRUD操作

**主要方法**:
- `NewCredentialService()` - 创建凭据服务
- `Create(req)` - 创建凭据
- `List()` - 获取凭据列表
- `GetByID(id)` - 根据ID获取凭据
- `Update(id, req)` - 更新凭据
- `Delete(id)` - 删除凭据
- `GetDecryptedPassword(id)` - 获取解密后的密码

---

## 5. 工具类文件

### 5.1 adaptive_config_calculator.go - 自适应配置计算器
**作用**: 根据系统资源和表大小计算最优的线程数和批次大小

**主要方法**:
- `NewAdaptiveConfigCalculator()` - 创建自适应配置计算器
- `CalculateForTask(tableCount, avgTableSize)` - 为任务计算自适应配置
- `CalculateForTable(stats)` - 为单个表计算批次大小
- `GetTableStats(db, database, table)` - 获取表的统计信息
- `GetDefaultConfig()` - 获取默认配置

### 5.2 foreign_key_analyzer.go - 外键分析器
**作用**: 分析表之间的外键依赖关系，提供拓扑排序功能

**主要方法**:
- `AnalyzeForeignKeys(database, tables)` - 分析数据库中的外键关系
- `TopologicalSort(tables, relations)` - 拓扑排序，返回删除顺序
- `SortTablesForDrop(db, database, tables)` - 对表进行排序，返回删除顺序
- `SortTablesForDropWithCheck()` - 对表进行排序，返回删除顺序和是否有外键依赖
- `SortTablesForDropWithFKList()` - 对表进行排序，返回删除顺序、是否有外键依赖、外键表列表

### 5.3 config_cache_service.go - 配置缓存服务
**作用**: 管理任务配置在Redis中的缓存

**主要方法**:
- `NewConfigCacheService()` - 创建配置缓存服务
- `LoadTaskConfigToRedis(taskID)` - 从MySQL加载任务配置到Redis
- `GetTaskConfigFromRedis(taskID)` - 从Redis读取任务配置
- `DeleteTaskConfigFromRedis(taskID)` - 删除Redis中的任务配置
- `InitAllTaskConfigs()` - 服务启动时加载所有任务配置到Redis
- `ReloadTaskConfig(taskID)` - 重新加载任务配置
- `GetTaskConfigWithFallback(taskID)` - 获取任务配置（优先Redis，失败则从MySQL）
- `ClearAllTaskConfigs()` - 清除所有任务配置
- `GetTaskConfigTTL(taskID)` - 获取配置的TTL
### 5.4 table_structure_parser.go - 表结构解析器
**作用**: 解析CREATE TABLE语句，提取字段、索引、外键等信息

**主要方法**:
- `NewTableStructureParser()` - 创建表结构解析器
- `Parse(createSQL)` - 解析CREATE TABLE语句
- `splitDefinitions(content)` - 分割定义（处理逗号分隔）
- `parseField(def, structure)` - 解析字段定义
- `parsePrimaryKey(def, structure)` - 解析主键定义
- `parseIndex(def, structure)` - 解析索引定义
- `parseForeignKey(def, structure)` - 解析外键定义

### 5.5 table_structure_modifier.go - 表结构修改器
**作用**: 修改表结构定义，支持字段过滤等功能

**主要方法**:
- `NewTableStructureModifier()` - 创建表结构修改器
- `FilterFieldsAndRebuild(createSQL, selectedFields, newTableName)` - 过滤字段并重建CREATE TABLE语句
- `buildCreateSQL()` - 构建CREATE TABLE语句
- `replaceTableName()` - 替换表名
- `GenerateAlterSQL()` - 生成ALTER TABLE语句

### 5.6 table_structure_alter.go - 表结构对比和ALTER服务
**作用**: 对比源表和目标表结构差异，生成ALTER语句

**主要方法**:
- `NewTableStructureAlterService()` - 创建表结构对比服务
- `CompareAndAlter()` - 对比并执行ALTER
- `getCreateTableSQL()` - 获取表的CREATE TABLE语句
- `tableExists()` - 检查表是否存在
- `generateAlterSQLs()` - 生成ALTER SQL语句
- `extractFieldNames()` - 提取字段名列表
- `normalizeFieldDef()` - 标准化字段定义

### 5.7 table_name_validator.go - 表名验证器
**作用**: 校验数据库名和表名是否合法，防止SQL注入

**主要方法**:
- `ValidateTableName(name)` - 校验表名是否合法
- `ValidateDatabaseName(name)` - 校验数据库名是否合法

### 5.8 system_resource_detector.go - 系统资源检测器
**作用**: 检测系统CPU、内存等资源，为自适应配置提供依据

**主要方法**:
- `NewSystemResourceDetector()` - 创建系统资源检测器
- `DetectResources()` - 检测系统资源
- `GetRecommendedThreadCount()` - 获取推荐的线程数

### 5.9 mysql_service.go - MySQL元数据服务
**作用**: 查询MySQL数据库的元数据信息（数据库列表、表列表、字段列表）

**主要方法**:
- `NewMySQLMetadataService()` - 创建MySQL元数据服务
- `GetDatabases(host, port, username, password)` - 获取数据库列表
- `GetTables(host, port, username, password, database)` - 获取指定数据库的表列表
- `GetDatabasesWithTables()` - 获取所有数据库及其表列表（树形结构）
- `GetTableColumns()` - 获取指定表的字段列表

### 5.10 log_file_watcher.go - 日志文件监听器
**作用**: 监听任务日志文件的变化，实时推送新日志内容

**主要方法**:
- `NewLogFileWatcher(taskID, category, client, done)` - 创建日志文件监听器
- `Start()` - 启动监听
- `sendInitialLogs()` - 发送初始日志（最后1000行）
- `ensureFileExists()` - 确保文件存在
- `watchLoop()` - 监听循环
- `readNewContent()` - 读取新内容

### 5.11 incremental_stats_service.go - 增量统计服务
**作用**: 管理增量同步的统计信息，包括任务统计和表统计

**主要方法**:
- `NewIncrementalStatsService()` - 创建增量统计服务
- `InitTaskStats(taskID)` - 初始化任务统计
- `InitTableStats(taskID, dbName, tableName)` - 初始化表统计
- `UpdateTableStats()` - 更新表统计
- `UpdateTaskStats()` - 更新任务统计
- `GetTaskStats(taskID)` - 获取任务统计
- `GetTableStatsList(taskID)` - 获取表统计列表
- `ClearTaskStats(taskID)` - 清除任务统计
- `GetDatabaseStatsList(taskID)` - 获取数据库统计列表

---

## 6. 任务管理详细文件

### 6.1 task_service.go - 任务服务
**作用**: 管理同步任务的CRUD操作

**主要方法**:
- `NewTaskService()` - 创建任务服务
- `Create(req)` - 创建任务
- `List()` - 获取任务列表
- `GetByID(id)` - 根据ID获取任务
- `UpdateConfig(id, req)` - 更新任务配置
- `Delete(id)` - 删除任务
- `clearRuntimeData(taskID)` - 清除任务的运行时数据
- `validateCreateRequest(req)` - 验证创建请求
- `validateDataSources()` - 验证数据源

### 6.2 task_control_service.go - 任务控制服务
**作用**: 控制任务的启动、停止、暂停等操作

### 6.3 task_execution_manager.go - 任务执行管理器
**作用**: 管理任务执行实例，包括context和goroutine

### 6.4 task_progress_manager.go - 任务进度管理器
**作用**: 管理任务进度信息，支持内存缓存

### 6.5 task_progress_service.go - 任务进度服务
**作用**: 提供任务进度的查询和更新接口

### 6.6 task_sse_service.go - 任务SSE服务
**作用**: 提供任务进度的实时推送功能

### 6.7 task_log_service.go - 任务日志服务
**作用**: 管理任务执行日志的记录和查询

### 6.8 task_foreign_key_sorter.go - 任务外键排序器
**作用**: 对同步任务中的表进行外键依赖排序

---

## 7. 文件依赖关系

### 7.1 全量同步调用链
```
task_full_sync_starter.go
└── sync_engine.go (公共同步工具)
    ├── sync_init.go (公共同步工具 - 创库创表)
    ├── sync_helpers.go (公共同步工具 - 映射转换)
    ├── mysql_reader.go (公共同步工具 - 数据处理)
    ├── mysql_writer.go (公共同步工具 - 数据处理)
    ├── adaptive_config_calculator.go (公共同步工具)
    ├── foreign_key_analyzer.go (公共同步工具 - 创库创表)
    ├── task_foreign_key_sorter.go (公共同步工具 - 创库创表)
    └── table_structure_*.go (公共同步工具 - 创库创表)
```

### 7.2 增量同步调用链
```
incremental_sync.go
├── binlog_listener.go
├── binlog_queue.go
├── incremental_consumer.go
│   ├── incremental_consumer_event_handler.go
│   └── incremental_consumer_mapper.go (映射转换)
├── incremental_stats_service.go
└── sync_engine.go (公共同步工具，执行全量同步阶段)
    ├── sync_init.go (公共同步工具 - 创库创表)
    ├── sync_helpers.go (公共同步工具 - 映射转换)
    ├── mysql_reader.go (公共同步工具 - 数据处理)
    └── mysql_writer.go (公共同步工具 - 数据处理)
```

### 7.3 数据源管理调用链
```
datasource_service.go
├── datasource_test_service.go
├── datasource_sse_service.go
├── credential_service.go
└── mysql_service.go
```

### 7.4 任务管理调用链
```
task_service.go
├── task_control_service.go
├── task_execution_manager.go
├── task_progress_manager.go
├── task_progress_service.go
├── task_sse_service.go
├── task_log_service.go
└── config_cache_service.go
```

---

## 8. 总结

services目录共包含**35个文件**，按功能分类：

- **全量同步相关**: 1个文件（纯全量逻辑）
- **增量同步相关**: 8个文件（纯增量逻辑）
- **表结构同步相关**: 3个文件（表结构处理专用）
- **公共同步工具**: 8个文件（创库创表、映射转换、数据处理）
- **任务管理相关**: 8个文件（包含配置缓存）
- **数据源管理相关**: 5个文件（包含MySQL元数据服务）
- **系统工具类文件**: 3个文件

这些文件构成了完整的数据同步系统，支持MySQL到MySQL的全量和增量同步，具备完善的任务管理、进度监控、日志记录等功能。

**核心设计特点**:
1. **模块化设计**: 每个文件职责单一，便于维护
2. **公共工具复用**: 创库创表、映射转换、数据处理等公共功能被全量和增量同步共享
3. **表结构独立**: 表结构解析、修改、对比等功能独立成模块
4. **差异化处理**: 全量同步使用批量读写，增量同步使用Binlog事件处理
5. **可扩展性**: 支持多种数据源类型和同步模式
6. **高性能**: 自适应配置、并发处理、批量操作
7. **可靠性**: 外键依赖处理、错误重试、进度恢复
8. **实时性**: SSE推送、Binlog监听、实时统计

**四大核心逻辑**:
1. **创库创表**: sync_init.go, foreign_key_analyzer.go, task_foreign_key_sorter.go
2. **表结构处理**: table_structure_parser.go, table_structure_modifier.go, table_structure_alter.go
3. **映射转换**: sync_helpers.go, incremental_consumer_mapper.go
4. **数据处理**: mysql_reader.go, mysql_writer.go, sync_engine.go