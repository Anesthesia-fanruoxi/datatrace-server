package models

// TaskTargetSource 任务-目标源关联表（多目标支持）
type TaskTargetSource struct {
	ID              string `gorm:"primaryKey;size:36" json:"id"`
	TaskID          string `gorm:"size:36;not null;index" json:"task_id"`   // 关联 SyncTask.ID
	TargetID        string `gorm:"size:36;not null;index" json:"target_id"` // 关联 DataSource.ID
	IsPrimary       bool   `gorm:"default:false" json:"is_primary"`         // 是否主目标（兼容旧逻辑）
	DatabaseMapping string `gorm:"type:text" json:"database_mapping"`       // JSON: 该目标的库表映射配置
	CreatedAt       string `json:"created_at"`
}

// TableName 指定表名
func (TaskTargetSource) TableName() string {
	return "task_target_sources"
}
