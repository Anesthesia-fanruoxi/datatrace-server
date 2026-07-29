package services

import (
	"context"
	"datatrace/models"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"gorm.io/gorm"
)

// TaskControlService 任务启停控制
type TaskControlService struct {
	db             *gorm.DB
	taskSvc        *TaskService
	credSvc        *CredentialService
	dsSvc          *DataSourceService
	execMgr        *TaskExecutionManager
	syncStarter    *FullSyncStarter
	incrSync       *IncrementalSync
	eventBus       *EventBus
	logSvc         *TaskLogService
	tableStatusSvc *TaskTableStatusService
	stoppedMu      sync.Mutex
	stopped        map[string]bool // taskID → 是否被手动停止
}

// NewTaskControlService 创建任务控制服务
func NewTaskControlService(
	db *gorm.DB,
	taskSvc *TaskService,
	credSvc *CredentialService,
	dsSvc *DataSourceService,
	execMgr *TaskExecutionManager,
	syncStarter *FullSyncStarter,
	incrSync *IncrementalSync,
	eventBus *EventBus,
	logSvc *TaskLogService,
	tableStatusSvc *TaskTableStatusService,
) *TaskControlService {
	return &TaskControlService{
		db:             db,
		taskSvc:        taskSvc,
		credSvc:        credSvc,
		dsSvc:          dsSvc,
		execMgr:        execMgr,
		syncStarter:    syncStarter,
		incrSync:       incrSync,
		eventBus:       eventBus,
		logSvc:         logSvc,
		tableStatusSvc: tableStatusSvc,
	}
}

// Start 启动任务
func (s *TaskControlService) Start(taskID string) error {
	if s.execMgr.IsRunning(taskID) {
		return fmt.Errorf("任务已在运行中")
	}

	task, err := s.taskSvc.GetByID(taskID)
	if err != nil {
		return fmt.Errorf("任务不存在: %w", err)
	}

	// 启动时清除旧日志和步骤状态
	if s.logSvc != nil {
		s.logSvc.ClearLogs(taskID)
	}
	if s.tableStatusSvc != nil {
		s.tableStatusSvc.ClearSteps(taskID)
		s.tableStatusSvc.ClearTaskTables(taskID)
	}

	// 获取源 DSN
	sourceDSN, err := s.buildDSN(task.SourceID)
	if err != nil {
		return fmt.Errorf("获取源库连接失败: %w", err)
	}

	// 解析配置获取目标 ID（优先用 config.Targets，兼容旧数据用 task.TargetID）
	var config TaskConfig
	if task.Config != "" && task.Config != "{}" {
		json.Unmarshal([]byte(task.Config), &config)
	}
	targetIDs := config.GetTargetIDs()
	if len(targetIDs) == 0 && task.TargetID != "" {
		targetIDs = []string{task.TargetID}
	}
	if len(targetIDs) == 0 {
		return fmt.Errorf("未配置目标数据源")
	}

	// 构建所有目标 DSN
	var targets []TargetInfo
	for _, tid := range targetIDs {
		info, err := s.buildTargetInfo(tid)
		if err != nil {
			return fmt.Errorf("获取目标库 %s 连接失败: %w", tid, err)
		}
		targets = append(targets, info)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.execMgr.Register(taskID, ctx, cancel)

	// 更新运行状态
	s.taskSvc.UpdateRunningState(taskID, true, "initialize")
	s.taskSvc.UpdateStatus(taskID, "running")
	s.eventBus.Publish("task.detail", map[string]interface{}{
		"task_id":      taskID,
		"is_running":   true,
		"current_step": "initialize",
	})

	// 异步执行同步
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[TaskControl] task %s panic: %v", taskID, r)
			}
		}()
		defer s.execMgr.Unregister(taskID)

		var syncErr error
		switch task.SyncMode {
		case "full":
			syncErr = s.syncStarter.StartFullSync(ctx, taskID, sourceDSN, targets)
		case "incremental":
			syncErr = s.startIncremental(ctx, task, taskID, sourceDSN, targets)
		default:
			syncErr = fmt.Errorf("不支持的同步模式: %s", task.SyncMode)
		}

		// 检查是否被手动停止
		s.stoppedMu.Lock()
		wasStopped := s.stopped[taskID]
		if wasStopped {
			delete(s.stopped, taskID) // 清理
		}
		s.stoppedMu.Unlock()

		if wasStopped {
			return // Stop 已处理状态更新
		}

		// 更新最终状态
		var finalStatus string
		if syncErr != nil {
			if ctx.Err() != nil {
				s.taskSvc.UpdateRunningState(taskID, false, "")
				s.taskSvc.UpdateStatus(taskID, "idle")
				finalStatus = "idle"
			} else {
				s.taskSvc.UpdateRunningState(taskID, false, "")
				s.taskSvc.UpdateStatus(taskID, "failed")
				finalStatus = "failed"
			}
		} else {
			s.taskSvc.UpdateRunningState(taskID, false, "completed")
			s.taskSvc.UpdateStatus(taskID, "completed")
			finalStatus = "completed"
		}

		s.eventBus.Publish("task.detail", map[string]interface{}{
			"task_id":    taskID,
			"is_running": false,
			"status":     finalStatus,
		})
	}()

	return nil
}

// Stop 停止任务
func (s *TaskControlService) Stop(taskID string) error {
	// 标记为手动停止（防止 goroutine defer 覆盖状态）
	s.stoppedMu.Lock()
	if s.stopped == nil {
		s.stopped = make(map[string]bool)
	}
	s.stopped[taskID] = true
	s.stoppedMu.Unlock()

	// 尝试取消运行中的任务
	s.execMgr.Cancel(taskID)

	// 强制重置 DB 状态
	s.taskSvc.UpdateRunningState(taskID, false, "")
	s.taskSvc.UpdateStatus(taskID, "idle")

	s.eventBus.Publish("task.detail", map[string]interface{}{
		"task_id":    taskID,
		"is_running": false,
		"status":     "idle",
	})
	return nil
}

// Pause 暂停任务（仅全量模式支持）
func (s *TaskControlService) Pause(taskID string) error {
	task, err := s.taskSvc.GetByID(taskID)
	if err != nil {
		return err
	}
	if task.SyncMode != "full" {
		return fmt.Errorf("增量同步不支持暂停")
	}
	// 暂停 = 取消当前 context
	s.execMgr.Cancel(taskID)
	s.taskSvc.UpdateRunningState(taskID, false, "paused")
	s.taskSvc.UpdateStatus(taskID, "paused")
	return nil
}

// Resume 恢复任务（重新开始全量同步）
func (s *TaskControlService) Resume(taskID string) error {
	task, err := s.taskSvc.GetByID(taskID)
	if err != nil {
		return err
	}
	if task.SyncMode != "full" {
		return fmt.Errorf("增量同步不支持恢复")
	}
	if task.Status != "paused" {
		return fmt.Errorf("任务不在暂停状态")
	}
	// 恢复 = 重新启动
	return s.Start(taskID)
}

// GetProgress 获取任务进度
func (s *TaskControlService) GetProgress(taskID string) *TaskProgress {
	return s.syncStarter.progressMgr.GetProgress(taskID)
}

// GetStepStatus 获取任务步骤状态
func (s *TaskControlService) GetStepStatus(taskID string) map[string]string {
	if s.tableStatusSvc == nil {
		return nil
	}
	result, err := s.tableStatusSvc.GetAllStepStatus(taskID)
	if err != nil {
		return nil
	}
	return result
}

// startIncremental 启动增量同步
func (s *TaskControlService) startIncremental(ctx context.Context, task *models.SyncTask, taskID, sourceDSN string, targets []TargetInfo) error {
	var config TaskConfig
	if err := json.Unmarshal([]byte(task.Config), &config); err != nil {
		return fmt.Errorf("解析任务配置失败: %w", err)
	}
	return s.incrSync.Start(ctx, taskID, &config, sourceDSN, targets)
}

// buildDSN 根据数据源 ID 构建连接字符串
func (s *TaskControlService) buildDSN(dsID string) (string, error) {
	var ds models.DataSource
	if err := s.db.First(&ds, "id = ?", dsID).Error; err != nil {
		return "", fmt.Errorf("数据源不存在: %w", err)
	}

	var password string
	if ds.CredentialID != nil && *ds.CredentialID != "" {
		cred, err := s.credSvc.GetByIDWithPassword(*ds.CredentialID)
		if err != nil {
			return "", err
		}
		decrypted, err := s.credSvc.DecryptPassword(cred)
		if err != nil {
			return "", err
		}
		password = decrypted
	} else {
		decrypted, err := s.credSvc.DecryptString(ds.Password)
		if err != nil {
			return "", err
		}
		password = decrypted
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		ds.Username, password, ds.Host, ds.Port, ds.DatabaseName), nil
}

// buildTargetInfo 构建目标数据源信息（含 DSN + 名称）
func (s *TaskControlService) buildTargetInfo(dsID string) (TargetInfo, error) {
	var ds models.DataSource
	if err := s.db.First(&ds, "id = ?", dsID).Error; err != nil {
		return TargetInfo{}, fmt.Errorf("数据源不存在: %w", err)
	}

	var password string
	if ds.CredentialID != nil && *ds.CredentialID != "" {
		cred, err := s.credSvc.GetByIDWithPassword(*ds.CredentialID)
		if err != nil {
			return TargetInfo{}, err
		}
		decrypted, err := s.credSvc.DecryptPassword(cred)
		if err != nil {
			return TargetInfo{}, err
		}
		password = decrypted
	} else {
		decrypted, err := s.credSvc.DecryptString(ds.Password)
		if err != nil {
			return TargetInfo{}, err
		}
		password = decrypted
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		ds.Username, password, ds.Host, ds.Port, ds.DatabaseName)

	return TargetInfo{
		ID:   ds.ID,
		DSN:  dsn,
		Name: ds.Name,
	}, nil
}
