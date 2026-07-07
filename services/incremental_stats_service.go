package services

import (
	"context"
	"encoding/json"
	"time"
)

// IncrementalStats 增量统计信息
type IncrementalStats struct {
	TaskID         string `json:"task_id"`
	EventsReceived int64  `json:"events_received"`
	EventsApplied  int64  `json:"events_applied"`
	EventsFailed   int64  `json:"events_failed"`
	QueueLength    int    `json:"queue_length"`
	LastEventTime  string `json:"last_event_time"`
}

// IncrementalStatsService 增量统计服务
type IncrementalStatsService struct {
	store CacheStore
}

// NewIncrementalStatsService 创建增量统计服务
func NewIncrementalStatsService(store CacheStore) *IncrementalStatsService {
	return &IncrementalStatsService{store: store}
}

func (s *IncrementalStatsService) statsKey(taskID string) string {
	return "incremental_stats:" + taskID
}

func (s *IncrementalStatsService) hashKey(taskID string) string {
	return "incremental_counters:" + taskID
}

// UpdateStats 更新统计
func (s *IncrementalStatsService) UpdateStats(ctx context.Context, stats *IncrementalStats) error {
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	return s.store.Set(ctx, s.statsKey(stats.TaskID), data, 24*time.Hour)
}

// GetStats 获取统计
func (s *IncrementalStatsService) GetStats(ctx context.Context, taskID string) (*IncrementalStats, error) {
	data, err := s.store.Get(ctx, s.statsKey(taskID))
	if err != nil {
		return &IncrementalStats{TaskID: taskID}, nil
	}
	if data == "" {
		return &IncrementalStats{TaskID: taskID}, nil
	}
	var stats IncrementalStats
	if err := json.Unmarshal([]byte(data), &stats); err != nil {
		return nil, err
	}
	return &stats, nil
}

// IncrReceived 增加接收计数（使用 HIncrBy）
func (s *IncrementalStatsService) IncrReceived(ctx context.Context, taskID string) error {
	_, err := s.store.HIncrBy(ctx, s.hashKey(taskID), "received", 1)
	return err
}

// IncrApplied 增加应用计数
func (s *IncrementalStatsService) IncrApplied(ctx context.Context, taskID string) error {
	_, err := s.store.HIncrBy(ctx, s.hashKey(taskID), "applied", 1)
	return err
}

// IncrFailed 增加失败计数
func (s *IncrementalStatsService) IncrFailed(ctx context.Context, taskID string) error {
	_, err := s.store.HIncrBy(ctx, s.hashKey(taskID), "failed", 1)
	return err
}

// GetCounters 获取计数器
func (s *IncrementalStatsService) GetCounters(ctx context.Context, taskID string) (map[string]string, error) {
	return s.store.HGetAll(ctx, s.hashKey(taskID))
}

// ResetStats 重置统计
func (s *IncrementalStatsService) ResetStats(ctx context.Context, taskID string) error {
	keys := []string{
		s.statsKey(taskID),
		s.hashKey(taskID),
	}
	for _, key := range keys {
		s.store.Del(ctx, key)
	}
	return nil
}
