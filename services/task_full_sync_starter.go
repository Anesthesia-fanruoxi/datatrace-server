package services

import (
	"context"
	"database/sql"
	"datatrace/common"
	"datatrace/models"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"gorm.io/gorm"
)

// TargetInfo 目标数据源信息
type TargetInfo struct {
	ID   string // DataSource ID
	DSN  string
	Name string
}

// FullSyncStarter 全量同步编排器
type FullSyncStarter struct {
	db             *gorm.DB
	engine         *SyncEngine
	progressMgr    *TaskProgressManager
	eventBus       *EventBus
	tableStatusSvc *TaskTableStatusService
}

// NewFullSyncStarter 创建全量同步编排器
func NewFullSyncStarter(db *gorm.DB, engine *SyncEngine, progressMgr *TaskProgressManager, eventBus *EventBus, tableStatusSvc *TaskTableStatusService) *FullSyncStarter {
	return &FullSyncStarter{
		db:             db,
		engine:         engine,
		progressMgr:    progressMgr,
		eventBus:       eventBus,
		tableStatusSvc: tableStatusSvc,
	}
}

// StartFullSync 启动全量同步（多目标并行）
func (s *FullSyncStarter) StartFullSync(ctx context.Context, taskID string, sourceDSN string, targets []TargetInfo) error {
	common.LogInfo("【全量同步】开始启动任务 %s，共 %d 个目标", taskID, len(targets))

	// 初始化步骤状态
	s.publishStep(taskID, "initialize", "running")
	s.publishStep(taskID, "create_structure", "pending")
	s.publishStep(taskID, "sync_data", "pending")
	s.publishStep(taskID, "completed", "pending")

	// 加载任务配置
	var task SyncTaskModel
	if err := s.db.First(&task, "id = ?", taskID).Error; err != nil {
		common.LogError("【全量同步】任务 %s 不存在: %v", taskID, err)
		return fmt.Errorf("任务不存在: %w", err)
	}

	var config TaskConfig
	if err := json.Unmarshal([]byte(task.Config), &config); err != nil {
		common.LogError("【全量同步】任务 %s 配置解析失败: %v", taskID, err)
		return fmt.Errorf("配置解析失败: %w", err)
	}

	// 自动计算并发数（I/O 密集型，取 CPU 核心数）
	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}

	tableExistsStrategy := config.SyncConfig.TableExistsStrategy
	if tableExistsStrategy == "" {
		tableExistsStrategy = "truncate"
	}
	strategyText := map[string]string{"drop": "删除重建", "truncate": "清空重建", "append": "追加写入", "structure_only": "仅结构"}[tableExistsStrategy]

	s.publishLog(taskID, "sync_start", fmt.Sprintf("同步模式：全量同步 | 并发数：%d（自动） | 批次：动态 | 表策略：%s", workers, strategyText))
	s.publishLog(taskID, "sync_start", fmt.Sprintf("任务：%s | 数据库映射：%d 个 | 目标数：%d 个", task.Name, len(config.DatabaseMappings), len(targets)))

	// 统计总表数
	totalTables := 0
	for _, db := range config.DatabaseMappings {
		totalTables += len(db.Tables)
	}
	s.publishLog(taskID, "sync_start", fmt.Sprintf("共同步 %d 张表", totalTables))

	// 连接源库
	common.LogInfo("【全量同步】正在连接源库...")
	sourceDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		common.LogError("【全量同步】连接源库失败: %v", err)
		return fmt.Errorf("连接源库失败: %w", err)
	}
	defer sourceDB.Close()

	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pingCancel()
	if err := sourceDB.PingContext(pingCtx); err != nil {
		common.LogError("【全量同步】❌ 源库 Ping 失败: %v", err)
		return fmt.Errorf("源库连接失败: %w", err)
	}
	common.LogInfo("【全量同步】✅ 源库 Ping 成功，连接正常")

	// 初始化完成
	s.publishStep(taskID, "initialize", "completed")
	s.publishStep(taskID, "create_structure", "running")

	reader := NewMySQLReader(sourceDB)

	// Step 1: 构建表同步任务列表
	var allTables []string
	var syncTasks []TableSyncTask

	for _, db := range config.DatabaseMappings {
		common.LogInfo("【全量同步】  数据库: %s -> %s, tables=%d", db.SourceDB, db.TargetDB, len(db.Tables))
		for _, tbl := range db.Tables {
			tableKey := TableKey(db.SourceDB, tbl.SourceTable)
			allTables = append(allTables, tableKey)
			syncTasks = append(syncTasks, TableSyncTask{
				SourceDB:    db.SourceDB,
				SourceTable: tbl.SourceTable,
				TargetDB:    db.TargetDB,
				TargetTable: tbl.TargetTable,
				Fields:      tbl.Columns,
				BatchSize:   0, // 由 SyncEngine 动态计算
			})
		}
	}

	// 初始化表状态到 Redis
	if s.tableStatusSvc != nil {
		var tableInfos []TableStatusInfo
		for _, t := range syncTasks {
			tableInfos = append(tableInfos, TableStatusInfo{
				SourceTable: t.SourceTable,
				TargetTable: t.TargetTable,
			})
		}
		if err := s.tableStatusSvc.InitTaskTables(taskID, tableInfos); err != nil {
			log.Printf("[FullSync] 初始化表状态失败: %v", err)
		}
	}

	// 仅同步结构模式
	if config.SyncConfig.SyncStructureOnly {
		s.publishLog(taskID, "initialize", "仅同步结构模式，跳过数据同步")
		var wg sync.WaitGroup
		for _, target := range targets {
			wg.Add(1)
			go func(t TargetInfo) {
				defer wg.Done()
				s.initTargetStructure(ctx, taskID, t, &config, sourceDSN)
				s.progressMgr.InitTargetProgress(taskID, t.ID, t.Name, allTables)
				for _, tbl := range allTables {
					s.progressMgr.CompleteTargetTable(taskID, t.ID, tbl)
				}
				s.progressMgr.CompleteTarget(taskID, t.ID)
			}(target)
		}
		wg.Wait()
		s.progressMgr.CompleteTask(taskID)
		return nil
	}

	// Step 2: 外键拓扑排序
	common.LogInfo("【全量同步】Step 2: 分析表依赖关系")
	var tableNames []string
	for _, db := range config.DatabaseMappings {
		for _, tbl := range db.Tables {
			tableNames = append(tableNames, tbl.SourceTable)
		}
	}
	var firstSourceDB string
	if len(config.DatabaseMappings) > 0 {
		firstSourceDB = config.DatabaseMappings[0].SourceDB
	}
	fks, err := reader.GetForeignKeys(ctx, firstSourceDB, tableNames)
	if err != nil {
		common.LogWarn("【全量同步】⚠️ 获取外键信息失败: %v", err)
	}
	topo := AnalyzeForeignKeys(tableNames, fks)
	if len(topo.Cycles) > 0 {
		s.publishLog(taskID, "sync_data", fmt.Sprintf("检测到循环依赖: %v", topo.Cycles))
	}

	// Step 3: 初始化进度
	for _, target := range targets {
		s.progressMgr.InitTargetProgress(taskID, target.ID, target.Name, allTables)
	}

	// 步骤：创建库表完成，开始同步数据
	s.publishStep(taskID, "create_structure", "completed")
	s.publishStep(taskID, "sync_data", "running")

	// Step 4: 并行同步每个目标
	var wg sync.WaitGroup
	errCh := make(chan error, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(t TargetInfo) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					common.LogError("【全量同步】❌ 目标 %s 同步异常: %v", t.Name, r)
					errCh <- fmt.Errorf("目标 %s 同步异常: %v", t.Name, r)
				}
			}()

			if err := s.syncOneTarget(ctx, taskID, t, sourceDSN, &config, syncTasks, topo); err != nil {
				common.LogError("【全量同步】❌ 目标 %s 同步失败: %v", t.Name, err)
				s.progressMgr.FailTarget(taskID, t.ID)
				errCh <- err
			}
		}(target)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		s.publishLog(taskID, "validate", fmt.Sprintf("全量同步完成，%d 个目标失败", len(errs)))
		s.publishStep(taskID, "sync_data", "completed")
		return fmt.Errorf("部分目标同步失败: %v", errs[0])
	}

	s.progressMgr.CompleteTask(taskID)
	s.publishLog(taskID, "validate", fmt.Sprintf("全量同步完成，共同步 %d 个目标，%d 张表", len(targets), len(syncTasks)))
	s.publishStep(taskID, "sync_data", "completed")
	s.publishStep(taskID, "completed", "completed")
	return nil
}

// syncOneTarget 同步到单个目标
func (s *FullSyncStarter) syncOneTarget(ctx context.Context, taskID string, target TargetInfo, sourceDSN string, config *TaskConfig, syncTasks []TableSyncTask, topo *TopologyResult) error {
	// 连接目标库
	targetDB, err := sql.Open("mysql", target.DSN)
	if err != nil {
		return fmt.Errorf("连接目标库 %s 失败: %w", target.Name, err)
	}
	defer targetDB.Close()

	sourceDB2, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		return fmt.Errorf("重新打开源库连接失败: %w", err)
	}
	defer sourceDB2.Close()
	reader := NewMySQLReader(sourceDB2)
	writer := NewMySQLWriter(targetDB)

	// 初始化目标库表结构（内部自动解析 DDL 外键依赖并拓扑排序）
	initializer := NewSyncInitializer(reader, writer)
	initializer.SetPublishFn(s.publishLog)
	if err := initializer.InitAllTargets(ctx, config, target.DSN, taskID); err != nil {
		return fmt.Errorf("初始化目标 %s 失败: %w", target.Name, err)
	}

	// ─── 第三步：同步数据 ───
	s.publishLog(taskID, "sync_data", "━━ 第三步：同步数据 ━━")

	// 同步独立表（并发）
	if len(topo.IndependentTables) > 0 {
		workers := runtime.NumCPU()
		if workers < 2 {
			workers = 2
		}
		var independentTasks []TableSyncTask
		for _, t := range syncTasks {
			for _, ind := range topo.IndependentTables {
				if t.SourceTable == ind {
					independentTasks = append(independentTasks, t)
				}
			}
		}
		s.engine.SyncTablesParallelForTarget(ctx, reader, writer, independentTasks, workers, taskID, target.ID, config.SyncConfig.TableTimeoutMinutes, s.publishLog, s.publishTableStatus)
	}

	// 同步有外键依赖的表（按拓扑序串行）
	if len(topo.OrderedTables) > 0 {
		var orderedTasks []TableSyncTask
		for _, ordered := range topo.OrderedTables {
			for _, t := range syncTasks {
				if t.SourceTable == ordered {
					orderedTasks = append(orderedTasks, t)
				}
			}
		}
		for _, t := range orderedTasks {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			s.engine.SyncTableForTarget(ctx, reader, writer,
				t.SourceDB, t.SourceTable, t.TargetDB, t.TargetTable,
				t.Fields, t.BatchSize, taskID, target.ID, config.SyncConfig.TableTimeoutMinutes, s.publishLog, s.publishTableStatus)
		}
	}

	s.progressMgr.CompleteTarget(taskID, target.ID)
	return nil
}

// initTargetStructure 仅初始化目标库结构
func (s *FullSyncStarter) initTargetStructure(ctx context.Context, taskID string, target TargetInfo, config *TaskConfig, sourceDSN string) {
	targetDBConn, err := sql.Open("mysql", target.DSN)
	if err != nil {
		log.Printf("[FullSync] 连接目标 %s 失败: %v", target.Name, err)
		return
	}
	defer targetDBConn.Close()

	sourceDB2, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		log.Printf("[FullSync] 连接目标 %s 失败: %v", target.Name, err)
		return
	}
	defer sourceDB2.Close()
	reader := NewMySQLReader(sourceDB2)
	writer := NewMySQLWriter(targetDBConn)
	initializer := NewSyncInitializer(reader, writer)
	initializer.SetPublishFn(s.publishLog)
	if err := initializer.InitAllTargets(ctx, config, target.DSN, taskID); err != nil {
		log.Printf("[FullSync] 初始化目标 %s 结构失败: %v", target.Name, err)
	}
}

func (s *FullSyncStarter) publishLog(taskID, category, message string) {
	log.Printf("[Task:%s][%s] %s", taskID, category, message)
	s.eventBus.Publish("task.log", map[string]interface{}{
		"task_id":  taskID,
		"category": category,
		"message":  message,
	})
}

// publishTableStatus 更新表状态到 Redis 并发布 SSE 事件
func (s *FullSyncStarter) publishTableStatus(taskID, targetTable, sourceTable, status string) {
	if s.tableStatusSvc != nil {
		s.tableStatusSvc.UpdateTableStatus(taskID, targetTable, sourceTable, status)
	}
}

// publishStep 更新步骤状态
func (s *FullSyncStarter) publishStep(taskID, step, status string) {
	if s.tableStatusSvc != nil {
		s.tableStatusSvc.UpdateStep(taskID, step, status)
	}
}

// SyncTaskModel 内部使用的任务模型别名（避免循环导入）
type SyncTaskModel = models.SyncTask
