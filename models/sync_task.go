package models

import (
	"time"
)

// SyncTask 同步任务模型
type SyncTask struct {
	ID          string `gorm:"primaryKey;size:36" json:"id"`
	Name        string `gorm:"size:100;not null" json:"name"`
	Remark      string `gorm:"size:500" json:"remark"`
	SourceID    string `gorm:"size:36;index" json:"source_id"`                    // 可空，不使用外键
	TargetID    string `gorm:"size:36;index" json:"target_id"`                    // 保留第一个目标源（兼容）
	SourceType  string `gorm:"size:20;not null" json:"source_type"`               // mysql/elasticsearch
	TargetType  string `gorm:"size:20;not null" json:"target_type"`               // mysql/elasticsearch
	Config      string `gorm:"type:text;not null" json:"config"`                  // JSON格式配置（包含TargetIDs多目标）
	Status      string `gorm:"size:20;not null;default:idle;index" json:"status"` // idle/configured（配置状态）
	IsRunning   bool   `gorm:"not null;default:false;index" json:"is_running"`    // 是否正在运行
	SyncMode    string `gorm:"size:20;not null;default:full" json:"sync_mode"`    // full/incremental
	CurrentStep string `gorm:"size:50;default:''" json:"current_step"`            // 当前步骤: initialize/sync_data/validate
	QueueType   string `gorm:"size:20;default:memory" json:"queue_type"`          // 队列类型: memory/redis

	// 增量同步相关字段
	IncrementalEnabled       bool   `gorm:"default:false" json:"incremental_enabled"`    // 是否启用增量同步
	SnapshotBinlogFile       string `gorm:"size:100" json:"snapshot_binlog_file"`        // 快照点Binlog文件
	SnapshotBinlogPos        uint32 `gorm:"default:0" json:"snapshot_binlog_pos"`        // 快照点Binlog位置
	CurrentBinlogFile        string `gorm:"size:100" json:"current_binlog_file"`         // 当前Binlog文件
	CurrentBinlogPos         uint32 `gorm:"default:0" json:"current_binlog_pos"`         // 当前Binlog位置
	FullSyncCompleted        bool   `gorm:"default:false" json:"full_sync_completed"`    // 全量同步是否完成
	IncrementalLag           int    `gorm:"default:0" json:"incremental_lag"`            // 增量延迟(秒)
	IncrementalEventsTotal   int64  `gorm:"default:0" json:"incremental_events_total"`   // 增量事件总数
	IncrementalEventsApplied int64  `gorm:"default:0" json:"incremental_events_applied"` // 已应用事件数

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// 关联（不存储到数据库，禁用外键约束）
	SourceConn  *DataSource   `gorm:"foreignKey:SourceID;references:ID;constraint:-" json:"source_conn,omitempty"`
	TargetConn  *DataSource   `gorm:"foreignKey:TargetID;references:ID;constraint:-" json:"target_conn,omitempty"`
	TargetConns []*DataSource `gorm:"-" json:"target_conns,omitempty"` // 多个目标源（运行时加载）
}

// TableName 指定表名
func (SyncTask) TableName() string {
	return "sync_tasks"
}
