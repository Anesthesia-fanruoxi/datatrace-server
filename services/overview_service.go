package services

import (
	"sort"

	"datatrace/models"
)

// OverviewStats 概览页顶部统计
type OverviewStats struct {
	RunningTasks     int `json:"running_tasks"`
	TotalTasks       int `json:"total_tasks"`
	TotalDatasources int `json:"total_datasources"`
}

// DatasourceHealth 数据源健康条目
type DatasourceHealth struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Status    string `json:"status"` // healthy/unhealthy/unknown
	LatencyMs *int64 `json:"latency_ms"`
}

// StatusCount 任务状态计数
type StatusCount struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
}

// TaskSummary 任务完成摘要
type TaskSummary struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	SyncMode  string `json:"sync_mode"`
	Status    string `json:"status"`
	UpdatedAt string `json:"updated_at"`
}

// OverviewData 概览页聚合数据（一次请求返回全部内容）
type OverviewData struct {
	Stats       OverviewStats      `json:"stats"`
	Datasources []DatasourceHealth `json:"datasources"`
	StatusDist  []StatusCount      `json:"status_dist"`
	RecentTasks []TaskSummary      `json:"recent_tasks"`
}

// OverviewService 概览聚合服务：所有数据获取、计算、处理都在后端完成
type OverviewService struct {
	taskSvc   *TaskService
	dsSvc     *DataSourceService
	healthSvc *HealthCheckService
}

// NewOverviewService 创建概览聚合服务
func NewOverviewService(taskSvc *TaskService, dsSvc *DataSourceService, healthSvc *HealthCheckService) *OverviewService {
	return &OverviewService{taskSvc: taskSvc, dsSvc: dsSvc, healthSvc: healthSvc}
}

// statusOrder 状态分布的展示顺序
var statusOrder = []string{"running", "idle", "completed", "failed", "paused", "stopped"}

// recentTaskLimit 任务完成情况最多返回条数
const recentTaskLimit = 8

// GetOverview 一次性聚合概览页所需的全部数据
func (s *OverviewService) GetOverview() *OverviewData {
	data := &OverviewData{
		Datasources: []DatasourceHealth{},
		StatusDist:  []StatusCount{},
		RecentTasks: []TaskSummary{},
	}

	s.fillTasks(data)
	s.fillDatasources(data)

	return data
}

// fillTasks 填充任务统计、状态分布与最近任务
func (s *OverviewService) fillTasks(data *OverviewData) {
	tasks, err := s.taskSvc.List()
	if err != nil {
		return
	}

	data.Stats.TotalTasks = len(tasks)

	// 状态计数
	counts := make(map[string]int)
	for _, t := range tasks {
		if t.Status == "running" {
			data.Stats.RunningTasks++
		}
		counts[t.Status]++
	}
	for _, st := range statusOrder {
		if c := counts[st]; c > 0 {
			data.StatusDist = append(data.StatusDist, StatusCount{Status: st, Count: c})
		}
	}

	// 按更新时间倒序，取前 N 个作为"任务完成情况"
	sorted := make([]models.SyncTask, len(tasks))
	copy(sorted, tasks)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].UpdatedAt.After(sorted[j].UpdatedAt) })
	if len(sorted) > recentTaskLimit {
		sorted = sorted[:recentTaskLimit]
	}
	for _, t := range sorted {
		data.RecentTasks = append(data.RecentTasks, TaskSummary{
			ID:        t.ID,
			Name:      t.Name,
			SyncMode:  t.SyncMode,
			Status:    t.Status,
			UpdatedAt: t.UpdatedAt.Format("2006-01-02 15:04:05"),
		})
	}
}

// fillDatasources 填充数据源数量与健康状态
func (s *OverviewService) fillDatasources(data *OverviewData) {
	dss, err := s.dsSvc.List()
	if err != nil {
		return
	}

	data.Stats.TotalDatasources = len(dss)

	health := map[string]*HealthStatus{}
	if s.healthSvc != nil {
		health = s.healthSvc.GetAllStatuses()
	}

	for _, ds := range dss {
		item := DatasourceHealth{ID: ds.ID, Name: ds.Name, Status: "unknown"}
		if h, ok := health[ds.ID]; ok && h != nil {
			item.Status = h.Status
			if h.LastCheckedAt != nil {
				lat := h.LatencyMs
				item.LatencyMs = &lat
			}
		}
		data.Datasources = append(data.Datasources, item)
	}
}
