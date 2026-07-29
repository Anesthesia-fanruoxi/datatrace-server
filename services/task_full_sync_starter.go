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
	s.publishLog(taskID, "sync_start", fmt.Sprintf("任务：%s | 目标数：%d 个", task.Name, len(targets)))

	// 收集每个目标的库表映射（支持多目标独立映射）
	targetMappings := make(map[string][]DatabaseMapping) // targetID -> mappings
	srcTablesByDB := make(map[string][]string)           // sourceDB -> [sourceTable…] 用于按库分组查外键
	seenSrcTbl := make(map[string]bool)                  // 去重用，key 为 sourceDB.sourceTable
	totalTables := 0

	for _, target := range targets {
		mappings := config.GetEffectiveMappings(target.ID)
		targetMappings[target.ID] = mappings
		for _, db := range mappings {
			totalTables += len(db.Tables)
			for _, tbl := range db.Tables {
				k := TableKey(db.SourceDB, tbl.SourceTable)
				if seenSrcTbl[k] {
					continue
				}
				seenSrcTbl[k] = true
				srcTablesByDB[db.SourceDB] = append(srcTablesByDB[db.SourceDB], tbl.SourceTable)
			}
		}
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

	// 构建所有源的 tableKey 集合（用于进度初始化和 FK 分析）
	var allTableKeys []string
	seenKeys := make(map[string]bool)
	for _, mappings := range targetMappings {
		for _, db := range mappings {
			for _, tbl := range db.Tables {
				key := TableKey(db.SourceDB, tbl.SourceTable)
				if !seenKeys[key] {
					seenKeys[key] = true
					allTableKeys = append(allTableKeys, key)
				}
			}
		}
	}

	// 初始化表状态到 Redis（合并所有目标的表）
	if s.tableStatusSvc != nil {
		var tableInfos []TableStatusInfo
		for _, mappings := range targetMappings {
			for _, db := range mappings {
				for _, tbl := range db.Tables {
					tableInfos = append(tableInfos, TableStatusInfo{
						SourceTable: tbl.SourceTable,
						TargetTable: tbl.TargetTable,
					})
				}
			}
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
				s.progressMgr.InitTargetProgress(taskID, t.ID, t.Name, allTableKeys)
				for _, tbl := range allTableKeys {
					s.progressMgr.CompleteTargetTable(taskID, t.ID, tbl)
				}
				s.progressMgr.CompleteTarget(taskID, t.ID)
			}(target)
		}
		wg.Wait()
		s.progressMgr.CompleteTask(taskID)
		return nil
	}

	// Step 2: 外键拓扑排序（按源库分组查外键，合并后使用 sourceDB.sourceTable 复合 key）
	common.LogInfo("【全量同步】Step 2: 分析表依赖关系")
	var allFKKeys []ForeignKeyInfo
	var allSrcKeys []string
	for srcDB, tbls := range srcTablesByDB {
		fks, err := reader.GetForeignKeys(ctx, srcDB, tbls)
		if err != nil {
			common.LogWarn("【全量同步】⚠️ 获取源库 %s 外键信息失败: %v", srcDB, err)
		}
		for _, fk := range fks {
			// MySQL InnoDB 外键限同库，父子表都属于 srcDB
			allFKKeys = append(allFKKeys, ForeignKeyInfo{
				ChildTable:  TableKey(srcDB, fk.ChildTable),
				ParentTable: TableKey(srcDB, fk.ParentTable),
			})
		}
		for _, t := range tbls {
			allSrcKeys = append(allSrcKeys, TableKey(srcDB, t))
		}
	}
	topo := AnalyzeForeignKeys(allSrcKeys, allFKKeys)
	if len(topo.Cycles) > 0 {
		s.publishLog(taskID, "sync_data", fmt.Sprintf("检测到循环依赖: %v", topo.Cycles))
	}

	// Step 3: 初始化进度
	for _, target := range targets {
		s.progressMgr.InitTargetProgress(taskID, target.ID, target.Name, allTableKeys)
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

			if err := s.syncOneTarget(ctx, taskID, t, sourceDSN, &config, topo); err != nil {
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
	s.publishLog(taskID, "validate", fmt.Sprintf("全量同步完成，共同步 %d 个目标，%d 张表", len(targets), totalTables))
	s.publishStep(taskID, "sync_data", "completed")
	s.publishStep(taskID, "completed", "completed")
	return nil
}

// syncOneTarget 同步到单个目标（使用每目标独立映射）
func (s *FullSyncStarter) syncOneTarget(ctx context.Context, taskID string, target TargetInfo, sourceDSN string, config *TaskConfig, topo *TopologyResult) error {
	// 获取此目标的独立库表映射
	mappings := config.GetEffectiveMappings(target.ID)

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

	// 构建此目标的 syncTasks（每目标独立的库表映射）
	var syncTasks []TableSyncTask
	for _, db := range mappings {
		common.LogInfo("【全量同步】  目标 %s 数据库: %s -> %s, tables=%d", target.Name, db.SourceDB, db.TargetDB, len(db.Tables))
		for _, tbl := range db.Tables {
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

	// 初始化目标库表结构（内部自动解析 DDL 外键依赖并拓扑排序）
	initializer := NewSyncInitializer(reader, writer)
	initializer.SetPublishFn(s.publishLog)
	if err := initializer.InitAllTargets(ctx, config, mappings, target.DSN, taskID); err != nil {
		return fmt.Errorf("初始化目标 %s 失败: %w", target.Name, err)
	}

	// ─── 第三步：同步数据 ───
	s.publishLog(taskID, "sync_data", fmt.Sprintf("━━ 目标 %s: 开始同步数据 ━━", target.Name))

	var allResults []SyncTableResult

	// 同步独立表（并发）
	if len(topo.IndependentTables) > 0 {
		workers := runtime.NumCPU()
		if workers < 2 {
			workers = 2
		}
		independentSet := make(map[string]bool, len(topo.IndependentTables))
		for _, k := range topo.IndependentTables {
			independentSet[k] = true
		}
		var independentTasks []TableSyncTask
		for _, t := range syncTasks {
			if independentSet[TableKey(t.SourceDB, t.SourceTable)] {
				independentTasks = append(independentTasks, t)
			}
		}
		results := s.engine.SyncTablesParallelForTarget(ctx, reader, writer, independentTasks, workers, taskID, target.ID, config.SyncConfig.TableTimeoutMinutes, s.publishLog, s.publishTableStatus)
		allResults = append(allResults, results...)
	}

	// 同步有外键依赖的表（按拓扑序串行）
	if len(topo.OrderedTables) > 0 {
		// 构建 tableKey -> TableSyncTask 映射（注意可能多个 syncTask 映射至同一 tableKey当前模型下不会）
		key2task := make(map[string]TableSyncTask, len(syncTasks))
		for _, t := range syncTasks {
			key2task[TableKey(t.SourceDB, t.SourceTable)] = t
		}
		for _, orderedKey := range topo.OrderedTables {
			t, ok := key2task[orderedKey]
			if !ok {
				continue
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			r := s.engine.SyncTableForTarget(ctx, reader, writer,
				t.SourceDB, t.SourceTable, t.TargetDB, t.TargetTable,
				t.Fields, t.BatchSize, taskID, target.ID, config.SyncConfig.TableTimeoutMinutes, s.publishLog, s.publishTableStatus)
			if r != nil {
				allResults = append(allResults, *r)
			}
		}
	}

	// 汇总失败统计
	var failedCount int
	for _, r := range allResults {
		if r.Error != nil {
			failedCount++
		}
	}
	if failedCount > 0 {
		s.publishLog(taskID, "sync_data", fmt.Sprintf("⚠️ 目标 %s: %d/%d 张表同步失败", target.Name, failedCount, len(allResults)))
		s.progressMgr.CompleteTarget(taskID, target.ID)
		return fmt.Errorf("目标 %s 有 %d 张表同步失败", target.Name, failedCount)
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
	mappings := config.GetEffectiveMappings(target.ID)
	if err := initializer.InitAllTargets(ctx, config, mappings, target.DSN, taskID); err != nil {
		log.Printf("[FullSync] 初始化目标 %s 结构失败: %v", target.Name, err)
	}
}

func (s *FullSyncStarter) publishLog(taskID, category, message string) {
	// 任务日志已经通过 event bus 派发到 SSE（前端可见）并由 TaskLogService 写入
	// logs/tasks/<taskID>.log，这里不再镜像到 stdout／主日志，避免控制台刷屏
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
