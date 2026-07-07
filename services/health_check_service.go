package services

import (
	"context"
	"database/sql"
	"datatrace/models"
	"fmt"
	"sync"
	"time"

	"gorm.io/gorm"
)

// HealthStatus 数据源健康状态
type HealthStatus struct {
	DataSourceID  string     `json:"datasource_id"`
	Status        string     `json:"status"`          // healthy/unhealthy/unknown
	LatencyMs     int64      `json:"latency_ms"`      // 连接延迟毫秒
	LastCheckedAt *time.Time `json:"last_checked_at"` // 最后检查时间
	ErrorMessage  string     `json:"error_message,omitempty"`
}

// HealthCheckService 数据源健康检查服务
type HealthCheckService struct {
	db       *gorm.DB
	dsSvc    *DataSourceService
	eventBus *EventBus
	mu       sync.RWMutex
	statuses map[string]*HealthStatus
	cancel   context.CancelFunc
}

// NewHealthCheckService 创建健康检查服务
func NewHealthCheckService(db *gorm.DB, dsSvc *DataSourceService, eventBus *EventBus) *HealthCheckService {
	return &HealthCheckService{
		db:       db,
		dsSvc:    dsSvc,
		eventBus: eventBus,
		statuses: make(map[string]*HealthStatus),
	}
}

// Start 启动定时健康检查（默认 30 秒间隔）
func (s *HealthCheckService) Start(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.loop(ctx, interval)
}

// Stop 停止健康检查
func (s *HealthCheckService) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *HealthCheckService) loop(ctx context.Context, interval time.Duration) {
	// 启动时立即检查一次
	s.checkAll(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkAll(ctx)
		}
	}
}

func (s *HealthCheckService) checkAll(ctx context.Context) {
	var datasources []models.DataSource
	if err := s.db.Find(&datasources).Error; err != nil {
		return
	}

	var wg sync.WaitGroup
	for _, ds := range datasources {
		wg.Add(1)
		go func(d models.DataSource) {
			defer wg.Done()
			s.checkOne(ctx, d)
		}(ds)
	}
	wg.Wait()

	// 广播所有状态
	s.eventBus.Publish("health.check", s.GetAllStatuses())
}

func (s *HealthCheckService) checkOne(ctx context.Context, ds models.DataSource) {
	status := &HealthStatus{
		DataSourceID: ds.ID,
		Status:       "unknown",
	}

	dsn, err := s.dsSvc.GetConnectionDSN(&ds)
	if err != nil {
		status.Status = "unhealthy"
		status.ErrorMessage = fmt.Sprintf("构建连接失败: %v", err)
		s.setStatus(ds.ID, status)
		return
	}

	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	start := time.Now()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		status.Status = "unhealthy"
		status.ErrorMessage = fmt.Sprintf("打开连接失败: %v", err)
		s.setStatus(ds.ID, status)
		return
	}
	defer db.Close()

	if err := db.PingContext(checkCtx); err != nil {
		status.Status = "unhealthy"
		status.ErrorMessage = fmt.Sprintf("Ping 失败: %v", err)
		s.setStatus(ds.ID, status)
		return
	}

	status.Status = "healthy"
	status.LatencyMs = time.Since(start).Milliseconds()
	now := time.Now()
	status.LastCheckedAt = &now
	s.setStatus(ds.ID, status)
}

func (s *HealthCheckService) setStatus(dsID string, status *HealthStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[dsID] = status
}

// GetStatus 获取单个数据源健康状态
func (s *HealthCheckService) GetStatus(dsID string) *HealthStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if st, ok := s.statuses[dsID]; ok {
		return st
	}
	return &HealthStatus{DataSourceID: dsID, Status: "unknown"}
}

// GetAllStatuses 获取所有数据源健康状态
func (s *HealthCheckService) GetAllStatuses() map[string]*HealthStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*HealthStatus, len(s.statuses))
	for k, v := range s.statuses {
		result[k] = v
	}
	return result
}

// CheckNow 立即检查单个数据源
func (s *HealthCheckService) CheckNow(dsID string) (*HealthStatus, error) {
	var ds models.DataSource
	if err := s.db.First(&ds, "id = ?", dsID).Error; err != nil {
		return nil, fmt.Errorf("数据源不存在: %w", err)
	}
	s.checkOne(context.Background(), ds)
	return s.GetStatus(dsID), nil
}
