package services

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// TableProgress 单表同步进度
type TableProgress struct {
	Database   string    `json:"database"`
	Table      string    `json:"table"`
	TotalRows  int64     `json:"total_rows"`
	SyncedRows int64     `json:"synced_rows"`
	Status     string    `json:"status"` // pending/running/completed/failed
	StartTime  time.Time `json:"start_time,omitempty"`
	EndTime    time.Time `json:"end_time,omitempty"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
}

// TargetProgress 单目标进度
type TargetProgress struct {
	TargetID        string                    `json:"target_id"`
	TargetName      string                    `json:"target_name"`
	Status          string                    `json:"status"` // running/completed/failed
	TotalTables     int                       `json:"total_tables"`
	CompletedTables int                       `json:"completed_tables"`
	TotalRows       int64                     `json:"total_rows"`
	SyncedRows      int64                     `json:"synced_rows"`
	Tables          map[string]*TableProgress `json:"tables"` // key: "db.table"
}

// TaskProgress 任务级进度
type TaskProgress struct {
	TaskID          string                    `json:"task_id"`
	Status          string                    `json:"status"` // running/paused/completed/failed
	TotalTables     int                       `json:"total_tables"`
	CompletedTables int                       `json:"completed_tables"`
	TotalRows       int64                     `json:"total_rows"`
	SyncedRows      int64                     `json:"synced_rows"`
	Tables          map[string]*TableProgress `json:"tables"` // key: "db.table"（兼容无目标维度）
	TargetStats     []*TargetProgress         `json:"target_stats,omitempty"`
	StartTime       time.Time                 `json:"start_time"`
}

// TaskProgressManager 进度管理（内存存储 + EventBus 发布）
type TaskProgressManager struct {
	mu       sync.RWMutex
	tasks    map[string]*TaskProgress // taskID → progress
	eventBus *EventBus
}

// NewTaskProgressManager 创建进度管理器
func NewTaskProgressManager(eventBus *EventBus) *TaskProgressManager {
	return &TaskProgressManager{
		tasks:    make(map[string]*TaskProgress),
		eventBus: eventBus,
	}
}

// InitProgress 初始化任务进度
func (m *TaskProgressManager) InitProgress(taskID string, tables []string) *TaskProgress {
	m.mu.Lock()
	defer m.mu.Unlock()

	progress := &TaskProgress{
		TaskID:      taskID,
		Status:      "running",
		TotalTables: len(tables),
		Tables:      make(map[string]*TableProgress),
		StartTime:   time.Now(),
	}

	for _, t := range tables {
		progress.Tables[t] = &TableProgress{
			Database: extractDB(t),
			Table:    extractTable(t),
			Status:   "pending",
		}
	}

	m.tasks[taskID] = progress
	m.publish(taskID)
	return progress
}

// UpdateTableProgress 更新单表进度
func (m *TaskProgressManager) UpdateTableProgress(taskID, tableKey string, syncedRows int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.tasks[taskID]
	if !ok {
		return
	}

	tp, ok := p.Tables[tableKey]
	if !ok {
		return
	}

	tp.SyncedRows = syncedRows
	p.SyncedRows = recalcTotalSynced(p)
	m.publish(taskID)
}

// CompleteTable 标记表完成
func (m *TaskProgressManager) CompleteTable(taskID, tableKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.tasks[taskID]
	if !ok {
		return
	}

	if tp, ok := p.Tables[tableKey]; ok {
		tp.Status = "completed"
		tp.EndTime = time.Now()
		p.CompletedTables++
	}
	m.publish(taskID)
}

// FailTable 标记表失败
func (m *TaskProgressManager) FailTable(taskID, tableKey string, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.tasks[taskID]
	if !ok {
		return
	}

	if tp, ok := p.Tables[tableKey]; ok {
		tp.Status = "failed"
		tp.ErrorMsg = errMsg
		tp.EndTime = time.Now()
	}
	m.publish(taskID)
}

// StartTable 标记表开始
func (m *TaskProgressManager) StartTable(taskID, tableKey string, totalRows int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.tasks[taskID]
	if !ok {
		return
	}

	if tp, ok := p.Tables[tableKey]; ok {
		tp.Status = "running"
		tp.TotalRows = totalRows
		tp.StartTime = time.Now()
	}
	m.publish(taskID)
}

// CompleteTask 标记任务完成
func (m *TaskProgressManager) CompleteTask(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, ok := m.tasks[taskID]; ok {
		p.Status = "completed"
	}
	m.publish(taskID)
}

// FailTask 标记任务失败
func (m *TaskProgressManager) FailTask(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, ok := m.tasks[taskID]; ok {
		p.Status = "failed"
	}
	m.publish(taskID)
}

// GetProgress 获取任务进度
func (m *TaskProgressManager) GetProgress(taskID string) *TaskProgress {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks[taskID]
}

// RemoveProgress 移除进度（任务停止后清理）
func (m *TaskProgressManager) RemoveProgress(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, taskID)
}

// ---- per-target 进度方法 ----

// InitTargetProgress 初始化单个目标的进度
func (m *TaskProgressManager) InitTargetProgress(taskID, targetID, targetName string, tables []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.tasks[taskID]
	if !ok {
		return
	}

	tp := &TargetProgress{
		TargetID:    targetID,
		TargetName:  targetName,
		Status:      "running",
		TotalTables: len(tables),
		Tables:      make(map[string]*TableProgress),
	}
	for _, t := range tables {
		tp.Tables[t] = &TableProgress{
			Database: extractDB(t),
			Table:    extractTable(t),
			Status:   "pending",
		}
	}
	p.TargetStats = append(p.TargetStats, tp)
	m.publish(taskID)
}

// StartTargetTable 标记目标中某表开始
func (m *TaskProgressManager) StartTargetTable(taskID, targetID, tableKey string, totalRows int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tp := m.findTarget(taskID, targetID)
	if tp == nil {
		return
	}
	if t, ok := tp.Tables[tableKey]; ok {
		t.Status = "running"
		t.TotalRows = totalRows
		t.StartTime = time.Now()
	}
	tp.SyncedRows = recalcTargetSynced(tp)
	m.publish(taskID)
}

// UpdateTargetTableProgress 更新目标中某表进度
func (m *TaskProgressManager) UpdateTargetTableProgress(taskID, targetID, tableKey string, syncedRows int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tp := m.findTarget(taskID, targetID)
	if tp == nil {
		return
	}
	if t, ok := tp.Tables[tableKey]; ok {
		t.SyncedRows = syncedRows
	}
	tp.SyncedRows = recalcTargetSynced(tp)
	m.publish(taskID)
}

// CompleteTargetTable 标记目标中某表完成
func (m *TaskProgressManager) CompleteTargetTable(taskID, targetID, tableKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tp := m.findTarget(taskID, targetID)
	if tp == nil {
		return
	}
	if t, ok := tp.Tables[tableKey]; ok {
		t.Status = "completed"
		t.EndTime = time.Now()
		tp.CompletedTables++
	}
	tp.SyncedRows = recalcTargetSynced(tp)
	m.publish(taskID)
}

// FailTargetTable 标记目标中某表失败
func (m *TaskProgressManager) FailTargetTable(taskID, targetID, tableKey string, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tp := m.findTarget(taskID, targetID)
	if tp == nil {
		return
	}
	if t, ok := tp.Tables[tableKey]; ok {
		t.Status = "failed"
		t.ErrorMsg = errMsg
		t.EndTime = time.Now()
	}
	m.publish(taskID)
}

// CompleteTarget 标记单个目标完成
func (m *TaskProgressManager) CompleteTarget(taskID, targetID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tp := m.findTarget(taskID, targetID)
	if tp == nil {
		return
	}
	tp.Status = "completed"

	// 检查是否所有目标都完成
	allDone := true
	for _, ts := range m.tasks[taskID].TargetStats {
		if ts.Status != "completed" {
			allDone = false
			break
		}
	}
	if allDone {
		m.tasks[taskID].Status = "completed"
	}
	m.publish(taskID)
}

// FailTarget 标记单个目标失败
func (m *TaskProgressManager) FailTarget(taskID, targetID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tp := m.findTarget(taskID, targetID)
	if tp == nil {
		return
	}
	tp.Status = "failed"
	m.publish(taskID)
}

func (m *TaskProgressManager) findTarget(taskID, targetID string) *TargetProgress {
	p, ok := m.tasks[taskID]
	if !ok {
		return nil
	}
	for _, tp := range p.TargetStats {
		if tp.TargetID == targetID {
			return tp
		}
	}
	return nil
}

func recalcTargetSynced(tp *TargetProgress) int64 {
	var total int64
	for _, t := range tp.Tables {
		total += t.SyncedRows
	}
	tp.TotalRows = 0
	for _, t := range tp.Tables {
		tp.TotalRows += t.TotalRows
	}
	return total
}

func (m *TaskProgressManager) publish(taskID string) {
	p := m.tasks[taskID]
	if p == nil {
		return
	}
	data, _ := json.Marshal(p)
	m.eventBus.Publish("task.progress", map[string]interface{}{
		"task_id":  taskID,
		"progress": json.RawMessage(data),
	})
}

func recalcTotalSynced(p *TaskProgress) int64 {
	var total int64
	for _, tp := range p.Tables {
		total += tp.SyncedRows
	}
	return total
}

func extractDB(tableKey string) string {
	for i, c := range tableKey {
		if c == '.' {
			return tableKey[:i]
		}
	}
	return tableKey
}

func extractTable(tableKey string) string {
	for i, c := range tableKey {
		if c == '.' {
			return tableKey[i+1:]
		}
	}
	return tableKey
}

// TableKey 生成表唯一键
func TableKey(db, table string) string {
	return fmt.Sprintf("%s.%s", db, table)
}
