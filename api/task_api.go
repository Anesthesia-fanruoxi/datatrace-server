package api

import (
	"datatrace/common"
	"datatrace/models"
	"datatrace/services"

	"encoding/json"
	"github.com/gin-gonic/gin"
	"time"
)

// TaskAPI 任务管理 API
type TaskAPI struct {
	service *services.TaskService
}

// NewTaskAPI 创建任务 API
func NewTaskAPI(svc *services.TaskService) *TaskAPI {
	return &TaskAPI{service: svc}
}

// taskResponse 任务响应 DTO，Config 以对象形式输出
type taskResponse struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Remark      string          `json:"remark"`
	SourceID    string          `json:"source_id"`
	TargetID    string          `json:"target_id"`
	SourceType  string          `json:"source_type"`
	TargetType  string          `json:"target_type"`
	Config      json.RawMessage `json:"config"`
	Status      string          `json:"status"`
	IsRunning   bool            `json:"is_running"`
	SyncMode    string          `json:"sync_mode"`
	CurrentStep string          `json:"current_step"`
	QueueType   string          `json:"queue_type"`

	IncrementalEnabled       bool   `json:"incremental_enabled"`
	SnapshotBinlogFile       string `json:"snapshot_binlog_file"`
	SnapshotBinlogPos        uint32 `json:"snapshot_binlog_pos"`
	CurrentBinlogFile        string `json:"current_binlog_file"`
	CurrentBinlogPos         uint32 `json:"current_binlog_pos"`
	FullSyncCompleted        bool   `json:"full_sync_completed"`
	IncrementalLag           int    `json:"incremental_lag"`
	IncrementalEventsTotal   int64  `json:"incremental_events_total"`
	IncrementalEventsApplied int64  `json:"incremental_events_applied"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// toTaskResponse 将 model 转换为响应 DTO，Config 直接以 JSON 对象输出
func toTaskResponse(task *models.SyncTask) taskResponse {
	configRaw := json.RawMessage("{}")
	if task.Config != "" && task.Config != "{}" {
		if json.Valid([]byte(task.Config)) {
			configRaw = json.RawMessage(task.Config)
		}
	}

	return taskResponse{
		ID:                       task.ID,
		Name:                     task.Name,
		Remark:                   task.Remark,
		SourceID:                 task.SourceID,
		TargetID:                 task.TargetID,
		SourceType:               task.SourceType,
		TargetType:               task.TargetType,
		Config:                   configRaw,
		Status:                   task.Status,
		IsRunning:                task.IsRunning,
		SyncMode:                 task.SyncMode,
		CurrentStep:              task.CurrentStep,
		QueueType:                task.QueueType,
		IncrementalEnabled:       task.IncrementalEnabled,
		SnapshotBinlogFile:       task.SnapshotBinlogFile,
		SnapshotBinlogPos:        task.SnapshotBinlogPos,
		CurrentBinlogFile:        task.CurrentBinlogFile,
		CurrentBinlogPos:         task.CurrentBinlogPos,
		FullSyncCompleted:        task.FullSyncCompleted,
		IncrementalLag:           task.IncrementalLag,
		IncrementalEventsTotal:   task.IncrementalEventsTotal,
		IncrementalEventsApplied: task.IncrementalEventsApplied,
		CreatedAt:                task.CreatedAt,
		UpdatedAt:                task.UpdatedAt,
	}
}

// List 获取任务列表
func (api *TaskAPI) List(c *gin.Context) {
	tasks, err := api.service.List()
	if err != nil {
		common.InternalServerError(c, "获取任务列表失败: "+err.Error())
		return
	}
	list := make([]taskResponse, len(tasks))
	for i, t := range tasks {
		list[i] = toTaskResponse(&t)
	}
	common.Success(c, list)
}

// Get 获取单个任务
func (api *TaskAPI) Get(c *gin.Context) {
	id := c.Param("id")
	task, err := api.service.GetByID(id)
	if err != nil {
		common.NotFound(c, "任务不存在")
		return
	}
	common.Success(c, toTaskResponse(task))
}

// Create 创建任务
func (api *TaskAPI) Create(c *gin.Context) {
	var req services.CreateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.BadRequest(c, "参数错误: "+err.Error())
		return
	}

	// 验证：至少选择一个表
	if req.Config != nil {
		totalTables := 0
		for _, db := range req.Config.DatabaseMappings {
			totalTables += len(db.Tables)
		}
		if totalTables == 0 {
			common.BadRequest(c, "请至少选择一个要同步的表")
			return
		}
	}

	task, err := api.service.Create(&req)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, toTaskResponse(task))
}

// Update 更新任务
func (api *TaskAPI) Update(c *gin.Context) {
	id := c.Param("id")
	var req services.UpdateTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.BadRequest(c, "参数错误: "+err.Error())
		return
	}

	// 验证：至少选择一个表
	if req.Config != nil {
		totalTables := 0
		for _, db := range req.Config.DatabaseMappings {
			totalTables += len(db.Tables)
		}
		if totalTables == 0 {
			common.BadRequest(c, "请至少选择一个要同步的表")
			return
		}
	}

	task, err := api.service.Update(id, &req)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, toTaskResponse(task))
}

// Delete 删除任务
func (api *TaskAPI) Delete(c *gin.Context) {
	id := c.Param("id")
	if err := api.service.Delete(id); err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, nil)
}

// GetConfig 获取任务配置
func (api *TaskAPI) GetConfig(c *gin.Context) {
	id := c.Param("id")
	config, err := api.service.GetConfig(id)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, config)
}

// GetStats 获取任务统计
func (api *TaskAPI) GetStats(c *gin.Context) {
	stats, err := api.service.GetStats()
	if err != nil {
		common.InternalServerError(c, "获取统计失败: "+err.Error())
		return
	}
	common.Success(c, stats)
}

// GetConfigView 获取任务配置视图
func (api *TaskAPI) GetConfigView(c *gin.Context) {
	id := c.Param("id")
	result, err := api.service.GetConfigView(id)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, result)
}

// UpdateConfig 更新任务配置（向导提交）
func (api *TaskAPI) UpdateConfig(c *gin.Context) {
	id := c.Param("id")
	var req services.UpdateTaskConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.BadRequest(c, "参数错误: "+err.Error())
		return
	}
	if err := api.service.UpdateConfig(id, &req); err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, gin.H{"message": "配置已更新"})
}
