package services

import (
	"datatrace/models"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// TaskService 任务管理服务
type TaskService struct {
	db *gorm.DB
}

// NewTaskService 创建任务服务
func NewTaskService(db *gorm.DB) *TaskService {
	return &TaskService{db: db}
}

// TaskConfig 任务配置（JSON 存储在 sync_tasks.config）
type TaskConfig struct {
	DatabaseMappings []DatabaseMapping `json:"database_mappings"`
	SyncConfig       SyncConfig        `json:"sync_config"`
	Runtime          RuntimeConfig     `json:"runtime"`
}

// RuntimeConfig 运行时性能参数
type RuntimeConfig struct {
	BatchSize int `json:"batch_size"`
	Workers   int `json:"workers"`
}

// DatabaseMapping 库映射配置（前端格式）
type DatabaseMapping struct {
	SourceDB     string         `json:"source_db"`
	TargetDB     string         `json:"target_db"`
	Expanded     bool           `json:"expanded"`
	Tables       []TableMapping `json:"tables"`
	TablesLoaded bool           `json:"tables_loaded"`
}

// TableMapping 表映射配置（前端格式）
type TableMapping struct {
	SourceTable   string       `json:"source_table"`
	TargetTable   string       `json:"target_table"`
	Selected      bool         `json:"selected"`
	Columns       []string     `json:"columns"`        // 空数组 = 同步全部字段
	ColumnsLoaded bool         `json:"columns_loaded"` // 是否已加载字段列表
	ColumnList    []ColumnInfo `json:"column_list"`    // 字段列表（可选）
}

// SyncConfig 同步策略配置
type SyncConfig struct {
	ErrorStrategy       string `json:"error_strategy"`        // skip/pause
	TableExistsStrategy string `json:"table_exists_strategy"` // drop/truncate/append/structure_only
	SyncStructureOnly   bool   `json:"sync_structure_only"`
	BinlogPositionMode  string `json:"binlog_position_mode"`  // position/gtid
	TableTimeoutMinutes int    `json:"table_timeout_minutes"` // 单表同步超时（分钟，0=不限）
}

// CreateTaskRequest 创建任务请求
type CreateTaskRequest struct {
	Name     string      `json:"name" binding:"required"`
	Mode     string      `json:"mode" binding:"required,oneof=full incremental full_incremental"`
	SourceID string      `json:"source_id" binding:"required"`
	TargetID string      `json:"target_id" binding:"required"`
	Config   *TaskConfig `json:"config"`
	Remark   string      `json:"remark"`
}

// UpdateTaskRequest 更新任务请求
type UpdateTaskRequest struct {
	Name     string      `json:"name"`
	Mode     string      `json:"mode"`
	SourceID string      `json:"source_id"`
	TargetID string      `json:"target_id"`
	Config   *TaskConfig `json:"config"`
}

// UpdateTaskConfigRequest 更新任务配置请求（向导提交）
type UpdateTaskConfigRequest struct {
	SourceID         string            `json:"source_id"`
	TargetID         string            `json:"target_id"`
	DatabaseMappings []DatabaseMapping `json:"database_mappings"`
	SyncConfig       SyncConfig        `json:"sync_config"`
	Runtime          RuntimeConfig     `json:"runtime"`
}

// Create 创建任务
func (s *TaskService) Create(req *CreateTaskRequest) (*models.SyncTask, error) {
	configJSON := "{}"
	if req.Config != nil {
		// 统计 tables 数量（用于调试）
		totalTables := 0
		for _, db := range req.Config.DatabaseMappings {
			for _, t := range db.Tables {
				if t.Selected {
					totalTables++
				}
			}
		}
		log.Printf("【CreateTask】接收到配置: %d 个数据库映射，共 %d 个表（selected）", len(req.Config.DatabaseMappings), totalTables)

		data, err := json.Marshal(req.Config)
		if err != nil {
			return nil, fmt.Errorf("配置序列化失败: %w", err)
		}
		configJSON = string(data)
		log.Printf("【CreateTask】配置已序列化，长度: %d 字符", len(configJSON))
		// 打印前 500 字符（用于调试）
		if len(configJSON) > 500 {
			log.Printf("【CreateTask】配置内容（前 500 字符）: %s...", configJSON[:500])
		} else {
			log.Printf("【CreateTask】配置内容: %s", configJSON)
		}
	}

	task := &models.SyncTask{
		ID:        uuid.New().String(),
		Name:      req.Name,
		SourceID:  req.SourceID,
		TargetID:  req.TargetID,
		SyncMode:  req.Mode,
		Remark:    req.Remark,
		Status:    "configured", // 创建时已有配置
		IsRunning: false,
		Config:    configJSON,
	}

	if err := s.db.Create(task).Error; err != nil {
		return nil, fmt.Errorf("创建任务失败: %w", err)
	}
	return task, nil
}

// List 获取任务列表
func (s *TaskService) List() ([]models.SyncTask, error) {
	var tasks []models.SyncTask
	if err := s.db.Order("created_at DESC").Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

// GetByID 根据 ID 获取任务
func (s *TaskService) GetByID(id string) (*models.SyncTask, error) {
	var task models.SyncTask
	if err := s.db.First(&task, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &task, nil
}

// Update 更新任务（仅 idle 状态可更新）
func (s *TaskService) Update(id string, req *UpdateTaskRequest) (*models.SyncTask, error) {
	var task models.SyncTask
	if err := s.db.First(&task, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("任务不存在: %w", err)
	}

	// 运行中不允许修改
	if task.IsRunning {
		return nil, fmt.Errorf("任务正在运行中，无法修改")
	}

	updates := map[string]interface{}{}
	if req.Name != "" {
		updates["name"] = req.Name
	}
	if req.Mode != "" {
		updates["sync_mode"] = req.Mode
	}
	if req.SourceID != "" {
		updates["source_id"] = req.SourceID
	}
	if req.TargetID != "" {
		updates["target_id"] = req.TargetID
	}
	if req.Config != nil {
		configJSON, err := json.Marshal(req.Config)
		if err != nil {
			return nil, fmt.Errorf("配置序列化失败: %w", err)
		}
		updates["config"] = string(configJSON)
	}
	updates["updated_at"] = time.Now()

	if err := s.db.Model(&task).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("更新任务失败: %w", err)
	}

	return &task, nil
}

// Delete 删除任务（运行中不可删除）
func (s *TaskService) Delete(id string) error {
	var task models.SyncTask
	if err := s.db.First(&task, "id = ?", id).Error; err != nil {
		return fmt.Errorf("任务不存在: %w", err)
	}

	if task.IsRunning {
		return fmt.Errorf("任务正在运行中，无法删除")
	}

	if err := s.db.Delete(&task).Error; err != nil {
		return fmt.Errorf("删除任务失败: %w", err)
	}
	return nil
}

// GetConfig 获取任务配置
func (s *TaskService) GetConfig(id string) (*TaskConfig, error) {
	var task models.SyncTask
	if err := s.db.First(&task, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("任务不存在: %w", err)
	}

	var config TaskConfig
	if task.Config != "" {
		if err := json.Unmarshal([]byte(task.Config), &config); err != nil {
			return nil, fmt.Errorf("配置解析失败: %w", err)
		}
	}
	return &config, nil
}

// UpdateStatus 更新任务状态
func (s *TaskService) UpdateStatus(id string, status string) error {
	return s.db.Model(&models.SyncTask{}).Where("id = ?", id).
		Update("status", status).Error
}

// UpdateRunningState 更新任务运行状态
func (s *TaskService) UpdateRunningState(id string, isRunning bool, step string) error {
	updates := map[string]interface{}{
		"is_running": isRunning,
	}
	if step != "" {
		updates["current_step"] = step
	}
	return s.db.Model(&models.SyncTask{}).Where("id = ?", id).Updates(updates).Error
}

// GetStats 获取任务统计
func (s *TaskService) GetStats() (map[string]int64, error) {
	stats := map[string]int64{}

	var total, running, idle int64
	s.db.Model(&models.SyncTask{}).Count(&total)
	s.db.Model(&models.SyncTask{}).Where("is_running = ?", true).Count(&running)
	s.db.Model(&models.SyncTask{}).Where("status = ?", "idle").Count(&idle)

	stats["total"] = total
	stats["running"] = running
	stats["idle"] = idle
	return stats, nil
}

// ConfigViewResult 配置视图结果（含关联信息）
type ConfigViewResult struct {
	Task         interface{}         `json:"task"`
	Config       *TaskConfig         `json:"config,omitempty"`
	RealTimeInfo map[string][]string `json:"real_time_info,omitempty"`
}

// GetConfigView 获取任务配置视图（含数据源名称、多目标信息）
func (s *TaskService) GetConfigView(id string) (*ConfigViewResult, error) {
	var task models.SyncTask
	if err := s.db.First(&task, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("任务不存在: %w", err)
	}

	// 加载源数据源
	var sourceDS models.DataSource
	if task.SourceID != "" {
		s.db.First(&sourceDS, "id = ?", task.SourceID)
	}

	// 解析配置
	var config TaskConfig
	if task.Config != "" && task.Config != "{}" {
		json.Unmarshal([]byte(task.Config), &config)
	}
	allTargetIDs := []string{}
	if task.TargetID != "" {
		allTargetIDs = []string{task.TargetID}
	}

	var targetConns []map[string]interface{}
	for _, tid := range allTargetIDs {
		var ds models.DataSource
		if s.db.First(&ds, "id = ?", tid).Error == nil {
			targetConns = append(targetConns, map[string]interface{}{
				"id":   ds.ID,
				"name": ds.Name,
			})
		}
	}

	taskView := map[string]interface{}{
		"id":           task.ID,
		"name":         task.Name,
		"source_type":  task.SourceType,
		"target_type":  task.TargetType,
		"source_conn":  map[string]interface{}{"name": sourceDS.Name},
		"target_conns": targetConns,
	}

	return &ConfigViewResult{
		Task:   taskView,
		Config: &config,
	}, nil
}

// UpdateConfig 更新任务配置（向导提交）
func (s *TaskService) UpdateConfig(id string, req *UpdateTaskConfigRequest) error {
	var task models.SyncTask
	if err := s.db.First(&task, "id = ?", id).Error; err != nil {
		return fmt.Errorf("任务不存在: %w", err)
	}
	if task.IsRunning {
		return fmt.Errorf("任务正在运行中，无法修改配置")
	}

	// 更新 DB 字段
	updates := map[string]interface{}{}
	if req.SourceID != "" {
		updates["source_id"] = req.SourceID
	}
	if req.TargetID != "" {
		updates["target_id"] = req.TargetID
	}
	if req.SyncConfig.ErrorStrategy != "" {
		updates["sync_mode"] = "incremental" // 根据策略判断，或让前端传
	}
	updates["updated_at"] = time.Now()

	// 只把属于 config JSON 的字段序列化
	configObj := TaskConfig{
		DatabaseMappings: req.DatabaseMappings,
		SyncConfig:       req.SyncConfig,
		Runtime:          req.Runtime,
	}
	configJSON, err := json.Marshal(configObj)
	if err != nil {
		return fmt.Errorf("配置序列化失败: %w", err)
	}
	updates["config"] = string(configJSON)

	// 如果有配置，标记为 configured
	if len(req.DatabaseMappings) > 0 {
		updates["status"] = "configured"
	}

	if len(updates) > 0 {
		if err := s.db.Model(&task).Updates(updates).Error; err != nil {
			return fmt.Errorf("更新配置失败: %w", err)
		}
	}
	return nil
}
