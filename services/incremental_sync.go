package services

import (
	"context"
	"database/sql"
	"datatrace/models"
	"fmt"
	"log"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"gorm.io/gorm"
)

// IncrementalSync 增量同步总控
type IncrementalSync struct {
	listener       *BinlogListener
	consumers      []*IncrementalConsumer
	queues         []BinlogQueue
	targetDBs      []*sql.DB
	db             *gorm.DB
	eventBus       *EventBus
	statsSvc       *IncrementalStatsService
	fullSync       *FullSyncStarter
	tableStatusSvc *TaskTableStatusService
	mu             sync.Mutex
	running        bool
	taskID         string
	binlogPos      mysql.Position
	gtidSet        mysql.GTIDSet
	currentCfg     *TaskConfig
}

// NewIncrementalSync 创建增量同步总控
func NewIncrementalSync(db *gorm.DB, eventBus *EventBus, statsSvc *IncrementalStatsService, fullSync *FullSyncStarter, tableStatusSvc *TaskTableStatusService) *IncrementalSync {
	return &IncrementalSync{
		db:             db,
		eventBus:       eventBus,
		statsSvc:       statsSvc,
		fullSync:       fullSync,
		tableStatusSvc: tableStatusSvc,
	}
}

// Start 启动增量同步（多目标）
func (s *IncrementalSync) Start(ctx context.Context, taskID string, config *TaskConfig, sourceDSN string, targets []TargetInfo) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("增量同步已在运行")
	}
	s.mu.Unlock()

	log.Printf("[IncrementalSync] 启动 task=%s targets=%d", taskID, len(targets))
	s.taskID = taskID

	// Step 1: 连接源库，检查 binlog_format=ROW
	sourceDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		return fmt.Errorf("连接源库失败: %w", err)
	}
	if err := s.checkBinlogFormat(sourceDB); err != nil {
		sourceDB.Close()
		return err
	}
	s.log(taskID, "incremental", "Step1: 源库连接成功, binlog_format=ROW 已确认")

	// Step 2: 捕获 Binlog 快照点
	pos, gtidSet, err := s.captureSnapshot(sourceDB, config.SyncConfig.BinlogPositionMode)
	if err != nil {
		sourceDB.Close()
		return fmt.Errorf("捕获 Binlog 快照失败: %w", err)
	}
	s.binlogPos = pos
	s.gtidSet = gtidSet
	if gtidSet != nil {
		s.log(taskID, "incremental", fmt.Sprintf("Step2: GTID 快照点 %s", gtidSet.String()))
	} else {
		s.log(taskID, "incremental", fmt.Sprintf("Step2: Binlog 快照点 %s:%d", pos.Name, pos.Pos))
	}

	// Step 3: 为每个目标创建事件队列
	s.queues = make([]BinlogQueue, len(targets))
	for i := range targets {
		s.queues[i] = NewMemoryQueue(10000)
	}
	s.log(taskID, "incremental", fmt.Sprintf("Step3: 创建 %d 个事件队列", len(targets)))

	// Step 4: 构建映射器（所有目标共享相同的源→目标映射）
	mapper := BuildMapperFromConfig(config)
	s.log(taskID, "incremental", "Step4: 构建源→目标映射器")
	s.currentCfg = config

	// Step 5: 为每个目标连接数据库 + 创建消费引擎
	s.consumers = make([]*IncrementalConsumer, len(targets))
	s.targetDBs = make([]*sql.DB, len(targets))

	strategy := config.SyncConfig.ErrorStrategy
	if strategy == "" {
		strategy = "skip"
	}

	for i, target := range targets {
		targetDB, err := sql.Open("mysql", target.DSN)
		if err != nil {
			sourceDB.Close()
			s.cleanupResources()
			return fmt.Errorf("连接目标数据库 %s 失败: %w", target.Name, err)
		}
		s.targetDBs[i] = targetDB

		consumer := NewIncrementalConsumer(s.queues[i], targetDB, mapper, s.eventBus, taskID, strategy)
		s.consumers[i] = consumer
		s.log(taskID, "incremental", fmt.Sprintf("Step5: 目标 %s 消费引擎已创建 (策略=%s)", target.Name, strategy))
	}

	// Step 6: 创建 Binlog 监听器 + 设置过滤表
	listener := NewBinlogListener(s.queues, s.eventBus, taskID)
	s.listener = listener
	tables := s.buildFilterTables(config)
	listener.SetFilterTables(tables)
	s.log(taskID, "incremental", fmt.Sprintf("Step6: 创建监听器, 过滤表 %d 个", len(tables)))

	// Step 7: 先执行一次全量同步（确保基础数据一致）
	s.log(taskID, "incremental", "Step7: 开始全量同步（增量前置）...")
	sourceDB.Close() // 全量同步内部会自己连接
	if err := s.fullSync.StartFullSync(ctx, taskID, sourceDSN, targets); err != nil {
		s.cleanupResources()
		return fmt.Errorf("增量前置全量同步失败: %w", err)
	}
	s.log(taskID, "incremental", "Step7: 全量同步完成，切换到增量模式")

	// 覆盖全量同步设置的“完成”步骤，增量的最终步骤是“增量同步中”
	if s.tableStatusSvc != nil {
		s.tableStatusSvc.UpdateStep(taskID, "completed", "skipped")
		s.tableStatusSvc.UpdateStep(taskID, "incremental", "running")
	}

	// Step 8: 启动所有消费引擎（异步）
	for i, consumer := range s.consumers {
		go func(c *IncrementalConsumer, idx int) {
			if err := c.Consume(ctx); err != nil && ctx.Err() == nil {
				log.Printf("[IncrementalSync] 消费引擎 %d 异常退出: %v", idx, err)
				s.log(taskID, "incremental", fmt.Sprintf("消费引擎 %d 异常: %v", idx, err))
			}
		}(consumer, i)
	}
	s.log(taskID, "incremental", fmt.Sprintf("Step8: %d 个消费引擎已启动", len(s.consumers)))

	// Step 9: 启动 Binlog 监听器
	canalCfg := buildCanalConfig(sourceDSN)
	useGTID := config.SyncConfig.BinlogPositionMode == "gtid" && gtidSet != nil
	if useGTID {
		s.log(taskID, "incremental", fmt.Sprintf("Step9: GTID 模式，快照点 %s", gtidSet.String()))
	}
	if err := listener.StartFrom(ctx, canalCfg, s.binlogPos); err != nil {
		s.cleanupResources()
		return fmt.Errorf("启动 Binlog 监听器失败: %w", err)
	}
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()
	s.log(taskID, "incremental", fmt.Sprintf("Step9: 增量同步已启动，从快照点开始消费，共 %d 个目标", len(targets)))

	log.Printf("[IncrementalSync] 增量同步已启动 task=%s pos=%s:%d targets=%d", taskID, pos.Name, pos.Pos, len(targets))
	return nil
}

// Stop 停止增量同步（持久化断点）
func (s *IncrementalSync) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}
	log.Printf("[IncrementalSync] 停止 task=%s", s.taskID)

	// 持久化当前位点
	if s.listener != nil {
		pos := s.listener.GetPosition()
		s.persistPosition(pos)
		s.listener.Stop()
	}

	s.cleanupResources()
	s.running = false
}

// persistPosition 保存 Binlog 位点到任务表
func (s *IncrementalSync) persistPosition(pos mysql.Position) {
	if s.db == nil || s.taskID == "" {
		return
	}
	updates := map[string]interface{}{
		"snapshot_binlog_file": pos.Name,
		"snapshot_binlog_pos":  pos.Pos,
	}
	if s.gtidSet != nil {
		updates["snapshot_gtid"] = s.gtidSet.String()
	}
	s.db.Model(&models.SyncTask{}).Where("id = ?", s.taskID).Updates(updates)
	s.log(s.taskID, "incremental", fmt.Sprintf("断点已持久化: %s:%d", pos.Name, pos.Pos))
}

// IsRunning 检查是否运行中
func (s *IncrementalSync) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// GetPosition 获取当前 Binlog 位置
func (s *IncrementalSync) GetPosition() mysql.Position {
	if s.listener != nil {
		return s.listener.GetPosition()
	}
	return s.binlogPos
}

// cleanupResources 清理资源（队列、连接）
func (s *IncrementalSync) cleanupResources() {
	for _, q := range s.queues {
		if q != nil {
			q.Close()
		}
	}
	for _, db := range s.targetDBs {
		if db != nil {
			db.Close()
		}
	}
	s.queues = nil
	s.consumers = nil
	s.targetDBs = nil
}

// checkBinlogFormat 检查源库 binlog_format 是否为 ROW
func (s *IncrementalSync) checkBinlogFormat(db *sql.DB) error {
	var name, value string
	rows, err := db.Query("SHOW VARIABLES LIKE 'binlog_format'")
	if err != nil {
		return fmt.Errorf("查询 binlog_format 失败: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&name, &value); err != nil {
			return fmt.Errorf("读取 binlog_format 失败: %w", err)
		}
	}
	if value != "ROW" {
		return fmt.Errorf("binlog_format=%s，增量同步要求 ROW 格式", value)
	}
	return nil
}

// captureSnapshot 捕获当前 Binlog 位置/GTID 作为快照点
func (s *IncrementalSync) captureSnapshot(db *sql.DB, mode string) (mysql.Position, mysql.GTIDSet, error) {
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, nil, fmt.Errorf("SHOW MASTER STATUS 失败: %w", err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	if len(cols) < 2 {
		return mysql.Position{}, nil, fmt.Errorf("SHOW MASTER STATUS 返回列不足")
	}

	var file string
	var pos uint32
	var gtidStr sql.NullString
	var dummy sql.RawBytes
	vals := make([]interface{}, len(cols))
	vals[0] = &file
	vals[1] = &pos
	for i := 2; i < len(cols); i++ {
		if i == 4 && len(cols) > 4 {
			vals[i] = &gtidStr
		} else {
			vals[i] = &dummy
		}
	}

	if rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return mysql.Position{}, nil, err
		}
	}

	rawPos := mysql.Position{Name: file, Pos: pos}

	// GTID 模式：解析 GTID 集合
	if mode == "gtid" && gtidStr.Valid && gtidStr.String != "" {
		gs, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtidStr.String)
		if err != nil {
			return rawPos, nil, fmt.Errorf("解析 GTID 集合失败: %w", err)
		}
		return rawPos, gs, nil
	}

	return rawPos, nil, nil
}

// checkGTIDMode 检测源库是否支持 GTID
func (s *IncrementalSync) checkGTIDMode(db *sql.DB) (bool, error) {
	var name, value string
	rows, err := db.Query("SHOW VARIABLES LIKE 'gtid_mode'")
	if err != nil {
		return false, nil // 查询失败说明不支持
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&name, &value); err != nil {
			return false, nil
		}
	}
	return value == "ON", nil
}

func (s *IncrementalSync) buildFilterTables(config *TaskConfig) []string {
	var tables []string
	for _, db := range config.DatabaseMappings {
		for _, tbl := range db.Tables {
			tables = append(tables, fmt.Sprintf("%s.%s", db.SourceDB, tbl.SourceTable))
		}
	}
	return tables
}

func (s *IncrementalSync) log(taskID, category, message string) {
	log.Printf("[IncrementalSync][%s] %s", category, message)
	s.eventBus.Publish("task.log", map[string]interface{}{
		"task_id": taskID, "category": category, "message": message,
	})
}

// ---- 运行时表变更 ----

// AddTable 运行时新增同步表
func (s *IncrementalSync) AddTable(sourceDB, table string) {
	if !s.running || s.listener == nil {
		return
	}
	key := fmt.Sprintf("%s.%s", sourceDB, table)
	s.listener.AddFilterTable(key)
	s.rebuildMapper()
	s.log(s.taskID, "incremental", fmt.Sprintf("运行时新增表: %s", key))
}

// RemoveTable 运行时移除同步表
func (s *IncrementalSync) RemoveTable(sourceDB, table string) {
	if !s.running || s.listener == nil {
		return
	}
	key := fmt.Sprintf("%s.%s", sourceDB, table)
	s.listener.RemoveFilterTable(key)
	s.rebuildMapper()
	s.log(s.taskID, "incremental", fmt.Sprintf("运行时移除表: %s", key))
}

// UpdateConfig 运行时更新配置（重建映射器）
func (s *IncrementalSync) UpdateConfig(config *TaskConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	s.currentCfg = config
	// 更新监听器过滤表
	tables := s.buildFilterTables(config)
	s.listener.SetFilterTables(tables)
	// 重建所有 consumer 的映射器
	s.rebuildMapperUnsafe()
	s.log(s.taskID, "incremental", fmt.Sprintf("运行时配置更新，过滤表 %d 个", len(tables)))
}

// rebuildMapper 重建映射器（带锁）
func (s *IncrementalSync) rebuildMapper() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebuildMapperUnsafe()
}

// rebuildMapperUnsafe 重建映射器（调用方需持锁）
func (s *IncrementalSync) rebuildMapperUnsafe() {
	if s.currentCfg == nil {
		return
	}
	mapper := BuildMapperFromConfig(s.currentCfg)
	for _, c := range s.consumers {
		c.UpdateMapper(mapper)
	}
}
