package services

import (
	"context"
	"sync"
)

// TaskExecution 运行中的任务实例
type TaskExecution struct {
	TaskID string
	Cancel context.CancelFunc
	Ctx    context.Context
}

// TaskExecutionManager 运行实例管理
type TaskExecutionManager struct {
	mu      sync.RWMutex
	running map[string]*TaskExecution // taskID → execution
}

// NewTaskExecutionManager 创建运行实例管理器
func NewTaskExecutionManager() *TaskExecutionManager {
	return &TaskExecutionManager{
		running: make(map[string]*TaskExecution),
	}
}

// Register 注册运行中的任务
func (m *TaskExecutionManager) Register(taskID string, ctx context.Context, cancel context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running[taskID] = &TaskExecution{
		TaskID: taskID,
		Cancel: cancel,
		Ctx:    ctx,
	}
}

// Unregister 移除运行中的任务
func (m *TaskExecutionManager) Unregister(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.running, taskID)
}

// Cancel 取消任务（停止/暂停）
func (m *TaskExecutionManager) Cancel(taskID string) bool {
	m.mu.RLock()
	exec, ok := m.running[taskID]
	m.mu.RUnlock()

	if !ok {
		return false
	}
	exec.Cancel()
	return true
}

// IsRunning 检查任务是否运行中
func (m *TaskExecutionManager) IsRunning(taskID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.running[taskID]
	return ok
}

// GetRunningTasks 获取所有运行中的任务 ID
func (m *TaskExecutionManager) GetRunningTasks() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.running))
	for id := range m.running {
		ids = append(ids, id)
	}
	return ids
}

// ShutdownAll 停止所有运行中的任务
func (m *TaskExecutionManager) ShutdownAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, exec := range m.running {
		exec.Cancel()
	}
}
