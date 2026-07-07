package services

import (
	"context"
	"fmt"
	"time"
)

const (
	tableStatusTTL = 24 * time.Hour
)

// TaskTableStatusService 管理任务表同步状态（Redis Hash 存储）
// Key: task:{taskID}:tables  →  Hash{ targetTable: status }
type TaskTableStatusService struct {
	cache    CacheStore
	eventBus *EventBus
}

// NewTaskTableStatusService 创建表状态服务
func NewTaskTableStatusService(cache CacheStore, eventBus *EventBus) *TaskTableStatusService {
	return &TaskTableStatusService{
		cache:    cache,
		eventBus: eventBus,
	}
}

func tableStatusKey(taskID string) string {
	return fmt.Sprintf("task:%s:tables", taskID)
}

// InitTaskTables 初始化所有表为 pending 状态
func (s *TaskTableStatusService) InitTaskTables(taskID string, tables []TableStatusInfo) error {
	ctx := context.Background()
	key := tableStatusKey(taskID)

	// 先清除旧数据
	s.cache.Del(ctx, key)

	fields := make(map[string]interface{}, len(tables))
	for _, t := range tables {
		fields[t.TargetTable] = "pending"
	}
	if err := s.cache.HSet(ctx, key, fields); err != nil {
		return fmt.Errorf("初始化表状态失败: %w", err)
	}
	return nil
}

// UpdateTableStatus 更新单张表的状态，并发布 SSE 事件
func (s *TaskTableStatusService) UpdateTableStatus(taskID, targetTable, sourceTable, status string) error {
	ctx := context.Background()
	key := tableStatusKey(taskID)

	if err := s.cache.HSet(ctx, key, map[string]interface{}{
		targetTable: status,
	}); err != nil {
		return fmt.Errorf("更新表状态失败: %w", err)
	}

	// 发布事件 → SSE 推送
	s.eventBus.Publish("task.table_status", map[string]interface{}{
		"task_id":      taskID,
		"target_table": targetTable,
		"source_table": sourceTable,
		"status":       status,
	})

	return nil
}

// GetAllTableStatus 获取所有表的状态
func (s *TaskTableStatusService) GetAllTableStatus(taskID string) (map[string]string, error) {
	ctx := context.Background()
	key := tableStatusKey(taskID)
	result, err := s.cache.HGetAll(ctx, key)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ClearTaskTables 清除任务的表状态（任务启动时调用）
func (s *TaskTableStatusService) ClearTaskTables(taskID string) error {
	ctx := context.Background()
	return s.cache.Del(ctx, tableStatusKey(taskID))
}

// TableStatusInfo 表状态初始化信息
type TableStatusInfo struct {
	SourceTable string `json:"source_table"`
	TargetTable string `json:"target_table"`
}

// ============================================================
// 步骤状态管理
// Key: task:{taskID}:steps → Hash{ stepName: status }
// ============================================================

func stepStatusKey(taskID string) string {
	return fmt.Sprintf("task:%s:steps", taskID)
}

// InitSteps 初始化步骤列表（全部为 pending）
func (s *TaskTableStatusService) InitSteps(taskID string, steps []string) error {
	ctx := context.Background()
	key := stepStatusKey(taskID)
	s.cache.Del(ctx, key)

	fields := make(map[string]interface{}, len(steps))
	for _, step := range steps {
		fields[step] = "pending"
	}
	return s.cache.HSet(ctx, key, fields)
}

// UpdateStep 更新单个步骤状态，并发布 SSE 事件
func (s *TaskTableStatusService) UpdateStep(taskID, step, status string) error {
	ctx := context.Background()
	key := stepStatusKey(taskID)

	if err := s.cache.HSet(ctx, key, map[string]interface{}{step: status}); err != nil {
		return fmt.Errorf("更新步骤状态失败: %w", err)
	}

	s.eventBus.Publish("task.step_status", map[string]interface{}{
		"task_id": taskID,
		"step":    step,
		"status":  status,
	})
	return nil
}

// GetAllStepStatus 获取所有步骤状态
func (s *TaskTableStatusService) GetAllStepStatus(taskID string) (map[string]string, error) {
	ctx := context.Background()
	key := stepStatusKey(taskID)
	return s.cache.HGetAll(ctx, key)
}

// ClearSteps 清除步骤状态
func (s *TaskTableStatusService) ClearSteps(taskID string) error {
	ctx := context.Background()
	return s.cache.Del(ctx, stepStatusKey(taskID))
}
