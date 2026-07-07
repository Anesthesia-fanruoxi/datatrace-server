# DataTrace 数据同步系统

DataTrace 是一个基于 Go + Gin 的 MySQL 数据同步平台，支持全量同步和基于 Binlog/GTID 的增量实时同步，提供 Web UI 配置向导和实时监控。

## 🚀 快速开始

### 环境要求

- Go 1.21+
- MySQL 5.7+
- Redis

### 运行项目

```bash
# 1. 配置（编辑 config.yaml）
# 2. 创建数据库
mysql -u root -p -e "CREATE DATABASE datatrace CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
# 3. 启动
go run main.go
```

服务默认监听 `http://localhost:8080`，健康检查 `GET /health`。

## 📁 项目结构

```
datatrace-server/
├── main.go                    # 程序入口，依赖注入
├── config.yaml                # 配置文件
├── api/                       # API 处理器
│   ├── credential_api.go      # 凭据管理
│   ├── datasource_api.go      # 数据源管理
│   ├── task_api.go            # 任务 CRUD
│   └── task_control_api.go    # 任务控制（启停/进度/日志）
├── services/                  # 业务逻辑层
│   ├── sync_engine.go         # 全量同步引擎
│   ├── sync_init.go           # 目标库表初始化（DDL 过滤）
│   ├── incremental_sync.go    # 增量同步（Binlog 监听）
│   ├── binlog_listener.go     # Binlog 事件监听
│   ├── binlog_queue.go        # Binlog 事件队列
│   ├── incremental_consumer.go        # 增量消费处理
│   ├── incremental_consumer_mapper.go # 增量事件字段映射
│   ├── incremental_stats_service.go   # 增量统计服务
│   ├── task_control_service.go        # 任务生命周期控制
│   ├── task_execution_manager.go      # 任务执行调度
│   ├── task_full_sync_starter.go      # 全量同步启动器
│   ├── task_progress_manager.go       # 进度管理（Redis）
│   ├── task_service.go                # 任务配置服务
│   ├── task_table_status_service.go   # 表级状态服务
│   ├── task_log_service.go            # 日志文件服务
│   ├── datasource_service.go          # 数据源管理服务
│   ├── credential_service.go          # 凭据管理服务
│   ├── health_check_service.go        # 数据源健康检查
│   ├── mysql_reader.go                # MySQL 读取
│   ├── mysql_writer.go                # MySQL 写入
│   ├── foreign_key_analyzer.go        # 外键依赖分析（拓扑排序）
│   ├── adaptive_config_calculator.go  # 自适应并发配置
│   ├── sse_hub.go                     # SSE 推送中心
│   ├── event_bus.go                   # 事件总线
│   └── cache_store.go                 # 缓存抽象接口
├── models/                    # 数据模型
│   ├── credential.go          # 凭据模型
│   ├── datasource.go          # 数据源模型
│   ├── sync_task.go           # 任务模型
│   └── task_target.go         # 目标库配置模型
├── routers/router.go          # 路由配置
├── common/                    # 公共组件
│   ├── response.go            # 统一响应封装
│   └── logger.go              # 日志中间件
├── config/config.go           # 配置管理
├── database/                  # 数据库初始化
│   ├── mysql.go
│   └── redis.go
└── utils/crypto.go            # 加密工具
```

## 📡 API 接口

### 凭据管理 `/api/v1/credentials`

```
GET    /          # 列表
GET    /:id       # 详情
POST   /          # 创建
PUT    /:id       # 更新
DELETE /:id       # 删除
POST   /:id/test  # 连通性测试
```

### 数据源管理 `/api/v1/datasources`

```
GET    /                          # 列表
GET    /:id                       # 详情
POST   /                          # 创建
PUT    /:id                       # 更新
DELETE /:id                       # 删除
POST   /test                      # 测试连通性（按 payload）
POST   /:id/test                  # 测试连通性（按 ID）
GET    /:id/databases             # 查询库列表
GET    /:id/tables                # 查询表列表
GET    /:id/database-tables       # 查询所有库表
GET    /:id/tables/:db/:tbl/columns  # 查询表字段
GET    /health                    # 健康检查列表
POST   /:id/health                # 健康检查
```

### 任务管理 `/api/v1/tasks`

```
GET    /              # 列表
GET    /:id           # 详情
POST   /              # 创建
PUT    /:id           # 更新
DELETE /:id           # 删除
GET    /stats         # 统计概览
GET    /:id/config    # 任务配置详情
GET    /:id/config-view  # 配置视图（含字段元数据）
PUT    /:id/config    # 更新配置
POST   /:id/start     # 启动任务
POST   /:id/stop      # 停止任务
POST   /:id/pause     # 暂停任务
POST   /:id/resume    # 恢复任务
GET    /:id/progress  # 同步进度
GET    /:id/logs      # 任务日志
DELETE /:id/logs      # 清空日志
GET    /:id/table-status  # 表级状态
GET    /:id/step-status   # 步骤状态
```

### SSE 实时推送 `/api/v1/sse`

```
GET    /sse           # SSE 统一端点（按 taskId 过滤）
```

## 🏗️ 核心架构

```
用户操作 → TaskControlAPI → TaskControlService
                                    ↓
                        ┌───────────┴──────────┐
                        ↓                      ↓
                  全量同步                   增量同步
            TaskFullSyncStarter        BinlogListener
                  ↓                      ↓
            SyncEngine              BinlogQueue → IncrementalConsumer
                  ↓                      ↓
         MySQLReader → MySQLWriter    MySQLWriter
                  ↓                      ↓
            ProgressManager → SSEHub（实时推送）
```

**全量同步流程**：
1. 分析外键依赖，拓扑排序建表顺序
2. 在目标库创建表（支持字段过滤，只建选中的字段）
3. 分批读取源表数据，写入目标表

**增量同步流程**：
1. 监听源库 Binlog 事件（INSERT/UPDATE/DELETE）
2. 事件入队列，消费者按表分发
3. 根据字段映射转换后写入目标库
4. 统计 TPS / 累计事件数，SSE 实时推送

## ⚙️ 配置说明

```yaml
database:
  host: localhost
  port: 3306
  username: root
  password: root
  database: datatrace
  max_open_conns: 100
  max_idle_conns: 10

redis:
  host: localhost
  port: 6379
  password: ""
  db: 0

server:
  port: 8080
  mode: debug

security:
  encryption_key: "your-32-byte-secret-key-here!!!"  # 修改为自定义密钥
```

## 📄 License

MIT License
