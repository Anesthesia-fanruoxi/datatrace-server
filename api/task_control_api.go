package api

import (
	"datatrace/common"
	"datatrace/services"

	"github.com/gin-gonic/gin"
)

// TaskControlAPI 任务控制 API
type TaskControlAPI struct {
	service        *services.TaskControlService
	logSvc         *services.TaskLogService
	tableStatusSvc *services.TaskTableStatusService
}

// NewTaskControlAPI 创建任务控制 API
func NewTaskControlAPI(svc *services.TaskControlService, logSvc *services.TaskLogService, tableStatusSvc *services.TaskTableStatusService) *TaskControlAPI {
	return &TaskControlAPI{service: svc, logSvc: logSvc, tableStatusSvc: tableStatusSvc}
}

// Start 启动任务
func (api *TaskControlAPI) Start(c *gin.Context) {
	id := c.Param("id")
	if err := api.service.Start(id); err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, gin.H{"message": "任务已启动"})
}

// Stop 停止任务
func (api *TaskControlAPI) Stop(c *gin.Context) {
	id := c.Param("id")
	if err := api.service.Stop(id); err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, gin.H{"message": "任务已停止"})
}

// Pause 暂停任务
func (api *TaskControlAPI) Pause(c *gin.Context) {
	id := c.Param("id")
	if err := api.service.Pause(id); err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, gin.H{"message": "任务已暂停"})
}

// Resume 恢复任务
func (api *TaskControlAPI) Resume(c *gin.Context) {
	id := c.Param("id")
	if err := api.service.Resume(id); err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, gin.H{"message": "任务已恢复"})
}

// GetProgress 获取任务进度
func (api *TaskControlAPI) GetProgress(c *gin.Context) {
	id := c.Param("id")
	progress := api.service.GetProgress(id)
	if progress == nil {
		common.Success(c, gin.H{"message": "暂无进度信息"})
		return
	}
	common.Success(c, progress)
}

// GetLogs 获取任务日志（从文件读取）
func (api *TaskControlAPI) GetLogs(c *gin.Context) {
	id := c.Param("id")
	if api.logSvc == nil {
		common.Success(c, []services.TaskLogEntry{})
		return
	}
	entries := api.logSvc.GetLogs(id)
	common.Success(c, entries)
}

// ClearLogs 清空任务日志
func (api *TaskControlAPI) ClearLogs(c *gin.Context) {
	id := c.Param("id")
	if api.logSvc != nil {
		api.logSvc.ClearLogs(id)
	}
	common.Success(c, gin.H{"message": "日志已清空"})
}

// GetTableStatus 获取任务表同步状态
func (api *TaskControlAPI) GetTableStatus(c *gin.Context) {
	id := c.Param("id")
	if api.tableStatusSvc == nil {
		common.Success(c, gin.H{})
		return
	}
	status, err := api.tableStatusSvc.GetAllTableStatus(id)
	if err != nil || len(status) == 0 {
		common.Success(c, gin.H{})
		return
	}
	common.Success(c, status)
}

// GetStepStatus 获取任务步骤状态
func (api *TaskControlAPI) GetStepStatus(c *gin.Context) {
	id := c.Param("id")
	status := api.service.GetStepStatus(id)
	if len(status) == 0 {
		common.Success(c, gin.H{})
		return
	}
	common.Success(c, status)
}
