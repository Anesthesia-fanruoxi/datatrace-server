package services

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// SyncEngine 全量同步引擎
type SyncEngine struct {
	progressMgr *TaskProgressManager
	eventBus    *EventBus
}

// NewSyncEngine 创建同步引擎
func NewSyncEngine(progressMgr *TaskProgressManager, eventBus *EventBus) *SyncEngine {
	return &SyncEngine{
		progressMgr: progressMgr,
		eventBus:    eventBus,
	}
}

// SyncTableResult 单表同步结果
type SyncTableResult struct {
	Database   string
	Table      string
	TotalRows  int64
	SyncedRows int64
	Error      error
}

// SyncTable 同步单张表
func (e *SyncEngine) SyncTable(ctx context.Context, reader *MySQLReader, writer *MySQLWriter,
	sourceDB, sourceTable, targetDB, targetTable string,
	fields []string, batchSize int, taskID string, tableTimeoutMin int) *SyncTableResult {

	// 单表超时
	if tableTimeoutMin > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(tableTimeoutMin)*time.Minute)
		defer cancel()
	}

	tableKey := TableKey(sourceDB, sourceTable)
	result := &SyncTableResult{Database: sourceDB, Table: sourceTable}

	// 获取列信息
	columns, err := reader.GetColumns(ctx, sourceDB, sourceTable)
	if err != nil {
		result.Error = fmt.Errorf("获取列信息失败: %w", err)
		return result
	}

	// 确定要同步的字段
	syncColumns := filterColumns(columns, fields)
	colNames := make([]string, len(syncColumns))
	for i, c := range syncColumns {
		colNames[i] = c.Name
	}

	// 获取总行数
	totalRows, err := reader.GetRowCount(ctx, sourceDB, sourceTable)
	if err != nil {
		result.Error = fmt.Errorf("获取行数失败: %w", err)
		return result
	}
	result.TotalRows = totalRows

	// 标记表开始
	e.progressMgr.StartTable(taskID, tableKey, totalRows)

	// 动态计算批次大小
	batchSize = reader.EstimateBatchSize(ctx, sourceDB, sourceTable, batchSize)
	log.Printf("【SyncTable】动态批次: %s.%s -> %d 行/批", sourceDB, sourceTable, batchSize)

	// 仅同步结构
	if batchSize == 0 {
		result.SyncedRows = 0
		e.progressMgr.CompleteTable(taskID, tableKey)
		return result
	}

	// 分批读取 + 写入
	var syncedRows int64
	offset := 0
	for {
		select {
		case <-ctx.Done():
			result.SyncedRows = syncedRows
			result.Error = ctx.Err()
			return result
		default:
		}

		rows, err := reader.ReadBatch(ctx, sourceDB, sourceTable, colNames, offset, batchSize)
		if err != nil {
			result.Error = fmt.Errorf("读取数据失败 (offset=%d): %w", offset, err)
			result.SyncedRows = syncedRows
			e.progressMgr.FailTable(taskID, tableKey, err.Error())
			return result
		}

		if len(rows) == 0 {
			break
		}

		if err := writer.BatchInsert(ctx, targetDB, targetTable, colNames, rows); err != nil {
			result.Error = fmt.Errorf("写入数据失败 (offset=%d): %w", offset, err)
			result.SyncedRows = syncedRows
			e.progressMgr.FailTable(taskID, tableKey, err.Error())
			return result
		}

		syncedRows += int64(len(rows))
		offset += batchSize
		e.progressMgr.UpdateTableProgress(taskID, tableKey, syncedRows)
	}

	result.SyncedRows = syncedRows
	e.progressMgr.CompleteTable(taskID, tableKey)
	return result
}

// SyncTablesParallel 并发同步多张表
func (e *SyncEngine) SyncTablesParallel(ctx context.Context, reader *MySQLReader, writer *MySQLWriter,
	tables []TableSyncTask, workers int, taskID string, tableTimeoutMin int) []SyncTableResult {

	if workers <= 0 {
		workers = 1
	}

	results := make([]SyncTableResult, len(tables))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for i, t := range tables {
		select {
		case <-ctx.Done():
			results[i] = SyncTableResult{
				Database: t.SourceDB, Table: t.SourceTable,
				Error: ctx.Err(),
			}
			continue
		default:
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(idx int, task TableSyncTask) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[SyncEngine] panic syncing %s.%s: %v", task.SourceDB, task.SourceTable, r)
				}
			}()

			result := e.SyncTable(ctx, reader, writer,
				task.SourceDB, task.SourceTable,
				task.TargetDB, task.TargetTable,
				task.Fields, task.BatchSize, taskID, tableTimeoutMin)
			results[idx] = *result
		}(i, t)
	}

	wg.Wait()
	return results
}

// TableSyncTask 单表同步任务参数
type TableSyncTask struct {
	SourceDB    string
	SourceTable string
	TargetDB    string
	TargetTable string
	Fields      []string
	BatchSize   int
}

// SyncTableForTarget 同步单表（带 per-target 进度追踪）
func (e *SyncEngine) SyncTableForTarget(ctx context.Context, reader *MySQLReader, writer *MySQLWriter,
	sourceDB, sourceTable, targetDB, targetTable string,
	fields []string, batchSize int, taskID, targetID string, tableTimeoutMin int,
	publishFn func(string, string, string),
	onTableComplete func(taskID, targetTable, sourceTable, status string)) *SyncTableResult {

	log.Printf("【SyncTableForTarget】开始同步: %s.%s -> %s.%s, batchSize=%d", sourceDB, sourceTable, targetDB, targetTable, batchSize)

	// 单表超时
	if tableTimeoutMin > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(tableTimeoutMin)*time.Minute)
		defer cancel()
	}

	tableKey := TableKey(sourceDB, sourceTable)
	result := &SyncTableResult{Database: sourceDB, Table: sourceTable}

	columns, err := reader.GetColumns(ctx, sourceDB, sourceTable)
	if err != nil {
		log.Printf("【SyncTableForTarget】❌ 获取列信息失败: %v", err)
		result.Error = fmt.Errorf("获取列信息失败: %w", err)
		return result
	}
	log.Printf("【SyncTableForTarget】获取到 %d 列: %v", len(columns), columns)

	syncColumns := filterColumns(columns, fields)
	colNames := make([]string, len(syncColumns))
	for i, c := range syncColumns {
		colNames[i] = c.Name
	}
	log.Printf("【SyncTableForTarget】过滤后列数: %d, 字段过滤参数: %v", len(syncColumns), fields)

	totalRows, err := reader.GetRowCount(ctx, sourceDB, sourceTable)
	if err != nil {
		log.Printf("【SyncTableForTarget】❌ 获取行数失败: %v", err)
		result.Error = fmt.Errorf("获取行数失败: %w", err)
		return result
	}
	result.TotalRows = totalRows
	log.Printf("【SyncTableForTarget】源表总行数: %d", totalRows)

	e.progressMgr.StartTargetTable(taskID, targetID, tableKey, totalRows)

	// 动态计算批次大小
	batchSize = reader.EstimateBatchSize(ctx, sourceDB, sourceTable, batchSize)
	log.Printf("【SyncTableForTarget】动态批次: %s.%s -> %d 行/批", sourceDB, sourceTable, batchSize)

	if batchSize == 0 {
		result.SyncedRows = 0
		e.progressMgr.CompleteTargetTable(taskID, targetID, tableKey)
		return result
	}

	var syncedRows int64
	offset := 0
	for {
		select {
		case <-ctx.Done():
			result.SyncedRows = syncedRows
			result.Error = ctx.Err()
			return result
		default:
		}

		rows, err := reader.ReadBatch(ctx, sourceDB, sourceTable, colNames, offset, batchSize)
		if err != nil {
			log.Printf("【SyncTableForTarget】❌ 读取数据失败 (offset=%d): %v", offset, err)
			result.Error = fmt.Errorf("读取数据失败 (offset=%d): %w", offset, err)
			result.SyncedRows = syncedRows
			e.progressMgr.FailTargetTable(taskID, targetID, tableKey, err.Error())
			return result
		}

		if len(rows) == 0 {
			log.Printf("【SyncTableForTarget】读取完毕，无更多数据")
			break
		}
		log.Printf("【SyncTableForTarget】读取到 %d 行 (offset=%d)", len(rows), offset)

		if err := writer.BatchInsert(ctx, targetDB, targetTable, colNames, rows); err != nil {
			log.Printf("【SyncTableForTarget】❌ 写入数据失败 (offset=%d): %v", offset, err)
			result.Error = fmt.Errorf("写入数据失败 (offset=%d): %w", offset, err)
			result.SyncedRows = syncedRows
			e.progressMgr.FailTargetTable(taskID, targetID, tableKey, err.Error())
			return result
		}
		log.Printf("【SyncTableForTarget】已写入 %d 行（累计 %d/%d）", len(rows), syncedRows+int64(len(rows)), totalRows)

		syncedRows += int64(len(rows))
		offset += batchSize
		e.progressMgr.UpdateTargetTableProgress(taskID, targetID, tableKey, syncedRows)
	}

	log.Printf("【SyncTableForTarget】✅ 表 %s.%s 同步完成，共同步 %d 行", sourceDB, sourceTable, syncedRows)
	result.SyncedRows = syncedRows
	e.progressMgr.CompleteTargetTable(taskID, targetID, tableKey)

	// 发布用户可见的同步完成日志
	status := "completed"
	if result.Error != nil {
		status = "failed"
	}
	if publishFn != nil {
		if targetTable == sourceTable {
			publishFn(taskID, "sync_data", fmt.Sprintf("同步表 %s 完成", targetTable))
		} else {
			publishFn(taskID, "sync_data", fmt.Sprintf("同步表 %s（源表名：%s）完成", targetTable, sourceTable))
		}
	}
	// 发布表状态事件
	if onTableComplete != nil {
		onTableComplete(taskID, targetTable, sourceTable, status)
	}
	return result
}

// SyncTablesParallelForTarget 并发同步多张表（带 per-target 进度）
func (e *SyncEngine) SyncTablesParallelForTarget(ctx context.Context, reader *MySQLReader, writer *MySQLWriter,
	tables []TableSyncTask, workers int, taskID, targetID string, tableTimeoutMin int,
	publishFn func(string, string, string),
	onTableComplete func(taskID, targetTable, sourceTable, status string)) []SyncTableResult {

	if workers <= 0 {
		workers = 1
	}

	results := make([]SyncTableResult, len(tables))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for i, t := range tables {
		select {
		case <-ctx.Done():
			results[i] = SyncTableResult{
				Database: t.SourceDB, Table: t.SourceTable,
				Error: ctx.Err(),
			}
			continue
		default:
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(idx int, task TableSyncTask) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[SyncEngine] panic syncing %s.%s: %v", task.SourceDB, task.SourceTable, r)
				}
			}()

			result := e.SyncTableForTarget(ctx, reader, writer,
				task.SourceDB, task.SourceTable,
				task.TargetDB, task.TargetTable,
				task.Fields, task.BatchSize, taskID, targetID, tableTimeoutMin, publishFn, onTableComplete)
			results[idx] = *result
		}(i, t)
	}

	wg.Wait()
	return results
}

// filterColumns 根据字段选择过滤列
func filterColumns(allColumns []ColumnInfo, selectedFields []string) []ColumnInfo {
	if len(selectedFields) == 0 {
		return allColumns
	}

	fieldSet := make(map[string]bool)
	for _, f := range selectedFields {
		fieldSet[f] = true
	}

	var filtered []ColumnInfo
	for _, c := range allColumns {
		if fieldSet[c.Name] {
			filtered = append(filtered, c)
		}
	}
	return filtered
}
