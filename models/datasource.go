package models

import (
	"time"
)

// DataSource 数据源模型
type DataSource struct {
	ID           string    `gorm:"primaryKey;size:36" json:"id"`
	Name         string    `gorm:"size:100;not null;index" json:"name"`
	Type         string    `gorm:"size:20;not null;index" json:"type"` // mysql/elasticsearch
	Host         string    `gorm:"size:255;not null" json:"host"`
	Port         int       `gorm:"not null" json:"port"`
	CredentialID *string   `gorm:"size:36;index" json:"credential_id"` // 凭据ID（可选）
	Username     string    `gorm:"size:100" json:"username"`           // 用户名（凭据为空时必填）
	Password     string    `gorm:"size:255" json:"password"`           // 加密存储（凭据为空时必填）
	DatabaseName string    `gorm:"size:100" json:"database_name"`      // MySQL专用
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// TableName 指定表名
func (DataSource) TableName() string {
	return "data_sources"
}
