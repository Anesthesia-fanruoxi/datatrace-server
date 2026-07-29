package app

import (
	"context"
	"datatrace/config"
	"datatrace/models"
	"datatrace/services"
	"datatrace/utils"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// AppContainer 依赖注入容器，管理所有 Service 生命周期
type AppContainer struct {
	Config *config.Config

	// Infrastructure
	DB       *gorm.DB
	Redis    *redis.Client
	Cache    services.CacheStore
	EventBus *services.EventBus
	SSEHub   *services.SSEHub
	Crypto   *utils.CryptoService

	// Services
	CredentialSvc   *services.CredentialService
	DataSourceSvc   *services.DataSourceService
	TaskSvc         *services.TaskService
	ProgressMgr     *services.TaskProgressManager
	ExecMgr         *services.TaskExecutionManager
	SyncEngine      *services.SyncEngine
	FullSyncStarter *services.FullSyncStarter
	IncrStatsSvc    *services.IncrementalStatsService
	IncrSync        *services.IncrementalSync
	TaskControlSvc  *services.TaskControlService
	HealthCheckSvc  *services.HealthCheckService
	TaskLogSvc      *services.TaskLogService
	TableStatusSvc  *services.TaskTableStatusService
	OverviewSvc     *services.OverviewService

	// 生命周期
	cancel context.CancelFunc
}

// NewAppContainer 创建并初始化 AppContainer
func NewAppContainer(cfg *config.Config, db *gorm.DB, redisClient *redis.Client) (*AppContainer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 1. 创建 CacheStore（Redis 或 Memory 降级）
	cache := services.NewCacheStore(&cfg.Redis, redisClient)

	// 2. 创建 EventBus
	eventBus := services.NewEventBus()

	// 3. 创建 SSEHub
	sseHub := services.NewSSEHub()

	// 4. 创建 CryptoService
	crypto := utils.NewCryptoService([]byte(cfg.Security.EncryptionKey))

	app := &AppContainer{
		Config:   cfg,
		DB:       db,
		Redis:    redisClient,
		Cache:    cache,
		EventBus: eventBus,
		SSEHub:   sseHub,
		Crypto:   crypto,
		cancel:   cancel,
	}

	// 5. 自动迁移数据库表
	if err := app.migrateDB(); err != nil {
		cancel()
		return nil, fmt.Errorf("数据库迁移失败: %w", err)
	}

	// 6. 初始化 Services
	app.initServices()

	// 7. 注册 EventBus → SSEHub 桥接
	app.registerBridges()

	_ = ctx // 预留给后续后台任务使用

	return app, nil
}

// migrateDB 自动迁移表结构
func (app *AppContainer) migrateDB() error {
	return app.DB.AutoMigrate(
		&models.Credential{},
		&models.DataSource{},
		&models.SyncTask{},
		&models.TaskTargetSource{},
	)
}

// initServices 初始化所有业务 Service
func (app *AppContainer) initServices() {
	app.CredentialSvc = services.NewCredentialService(app.DB, app.Crypto)
	app.DataSourceSvc = services.NewDataSourceService(app.DB, app.Crypto, app.CredentialSvc)
	app.TaskSvc = services.NewTaskService(app.DB)

	// 表状态服务（Redis Hash 存储）
	app.TableStatusSvc = services.NewTaskTableStatusService(app.Cache, app.EventBus)

	// 同步引擎相关
	app.ProgressMgr = services.NewTaskProgressManager(app.EventBus)
	app.SyncEngine = services.NewSyncEngine(app.ProgressMgr, app.EventBus)
	app.FullSyncStarter = services.NewFullSyncStarter(app.DB, app.SyncEngine, app.ProgressMgr, app.EventBus, app.TableStatusSvc)
	app.ExecMgr = services.NewTaskExecutionManager()

	// 增量同步相关
	app.IncrStatsSvc = services.NewIncrementalStatsService(app.Cache)
	app.IncrSync = services.NewIncrementalSync(app.DB, app.EventBus, app.IncrStatsSvc, app.FullSyncStarter, app.TableStatusSvc)

	// 任务日志文件服务（必须在 TaskControlSvc 之前创建）
	app.TaskLogSvc = services.NewTaskLogService("")

	app.TaskControlSvc = services.NewTaskControlService(
		app.DB, app.TaskSvc, app.CredentialSvc, app.DataSourceSvc,
		app.ExecMgr, app.FullSyncStarter, app.IncrSync, app.EventBus, app.TaskLogSvc, app.TableStatusSvc,
	)

	// 健康检查服务
	app.HealthCheckSvc = services.NewHealthCheckService(app.DB, app.DataSourceSvc, app.EventBus)

	// 概览聚合服务（依赖 TaskSvc / DataSourceSvc / HealthCheckSvc）
	app.OverviewSvc = services.NewOverviewService(app.TaskSvc, app.DataSourceSvc, app.HealthCheckSvc)
}

// registerBridges 注册 EventBus → SSEHub 桥接
func (app *AppContainer) registerBridges() {
	// 任务进度 → SSE
	app.EventBus.Subscribe("task.progress", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if taskID, ok := m["task_id"].(string); ok {
				topic := fmt.Sprintf("task:%s:progress", taskID)
				if progress, ok := m["progress"]; ok {
					app.SSEHub.PublishJSON(topic, "progress", progress)
				}
			}
		}
	})

	// 任务详情 → SSE（自动附加 target_conns）
	app.EventBus.Subscribe("task.detail", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if taskID, ok := m["task_id"].(string); ok {
				topic := fmt.Sprintf("task:%s:detail", taskID)
				// 从 DB 加载 target_conns
				app.enrichTaskDetail(taskID, m)
				app.SSEHub.PublishJSON(topic, "task_detail", m)
			}
		}
	})

	// 任务日志 → SSE + 文件持久化
	app.EventBus.Subscribe("task.log", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if taskID, ok := m["task_id"].(string); ok {
				category, _ := m["category"].(string)
				message, _ := m["message"].(string)

				// 写入任务日志文件
				app.TaskLogSvc.AppendLog(taskID, category, message)

				// 推送到 SSE
				topic := fmt.Sprintf("task:%s:logs", taskID)
				// 附加时间戳
				m["time"] = time.Now().Format("2006-01-02 15:04:05")
				app.SSEHub.PublishJSON(topic, "log", m)
			}
		}
	})

	// 表同步状态 → SSE
	app.EventBus.Subscribe("task.table_status", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if taskID, ok := m["task_id"].(string); ok {
				topic := fmt.Sprintf("task:%s:table_status", taskID)
				m["time"] = time.Now().Format("2006-01-02 15:04:05")
				app.SSEHub.PublishJSON(topic, "table_status", m)
			}
		}
	})

	// 步骤状态 → SSE
	app.EventBus.Subscribe("task.step_status", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if taskID, ok := m["task_id"].(string); ok {
				topic := fmt.Sprintf("task:%s:step_status", taskID)
				app.SSEHub.PublishJSON(topic, "step_status", m)
			}
		}
	})

	// 增量统计 → SSE
	app.EventBus.Subscribe("incremental.stats", func(data interface{}) {
		if m, ok := data.(map[string]interface{}); ok {
			if taskID, ok := m["task_id"].(string); ok {
				topic := fmt.Sprintf("task:%s:incremental", taskID)
				app.SSEHub.PublishJSON(topic, "incremental_stats", m)
			}
		}
	})

	// 健康检查 → SSE
	app.EventBus.Subscribe("health.check", func(data interface{}) {
		app.SSEHub.PublishJSON("health:all", "health_check", data)
	})
}

// enrichTaskDetail 为 task.detail 事件附加 target_conns 信息
func (app *AppContainer) enrichTaskDetail(taskID string, m map[string]interface{}) {
	var task models.SyncTask
	if err := app.DB.First(&task, "id = ?", taskID).Error; err != nil {
		return
	}

	// 解析 config 获取 target_ids
	var targetIDs []string
	if task.Config != "" && task.Config != "{}" {
		var config struct {
			TargetIDs []string `json:"target_ids"`
			TargetID  string   `json:"target_id"`
		}
		if json.Unmarshal([]byte(task.Config), &config) == nil {
			targetIDs = config.TargetIDs
			if len(targetIDs) == 0 && config.TargetID != "" {
				targetIDs = []string{config.TargetID}
			}
		}
	}
	if len(targetIDs) == 0 && task.TargetID != "" {
		targetIDs = []string{task.TargetID}
	}

	// 加载目标数据源名称
	type targetConn struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	var conns []targetConn
	for _, tid := range targetIDs {
		var ds models.DataSource
		if app.DB.First(&ds, "id = ?", tid).Error == nil {
			conns = append(conns, targetConn{ID: ds.ID, Name: ds.Name})
		}
	}
	if len(conns) > 0 {
		m["target_conns"] = conns
	}
	m["target_id"] = task.TargetID
	m["sync_mode"] = task.SyncMode
}

// Start 启动应用
func (app *AppContainer) Start() error {
	// 启动健康检查（30 秒间隔）
	if app.HealthCheckSvc != nil {
		app.HealthCheckSvc.Start(30 * time.Second)
	}
	log.Println("✅ AppContainer 初始化完成")
	return nil
}

// Shutdown 关闭应用，释放资源
func (app *AppContainer) Shutdown() error {
	log.Println("🛑 AppContainer 正在关闭...")

	// 停止健康检查
	if app.HealthCheckSvc != nil {
		app.HealthCheckSvc.Stop()
	}

	// 停止所有运行中的任务
	if app.ExecMgr != nil {
		app.ExecMgr.ShutdownAll()
	}

	app.cancel()

	// 关闭 CacheStore
	if err := app.Cache.Close(); err != nil {
		log.Printf("关闭 CacheStore 失败: %v", err)
	}

	// 关闭 Redis
	if app.Redis != nil {
		if err := app.Redis.Close(); err != nil {
			log.Printf("关闭 Redis 失败: %v", err)
		}
	}

	// 关闭 MySQL
	sqlDB, err := app.DB.DB()
	if err == nil {
		sqlDB.Close()
	}

	// 关闭 TaskLogService
	if app.TaskLogSvc != nil {
		app.TaskLogSvc.Close()
	}

	log.Println("✅ AppContainer 已关闭")
	return nil
}
