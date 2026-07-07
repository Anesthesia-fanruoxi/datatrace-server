package models

import (
	"time"
)

// Credential 凭据模型
type Credential struct {
	ID          string    `gorm:"primaryKey;size:36" json:"id"`
	Name        string    `gorm:"size:100;not null;uniqueIndex" json:"name"` // 凭据名称，唯一
	Description string    `gorm:"size:255" json:"description"`               // 描述
	Username    string    `gorm:"size:100;not null" json:"username"`         // 用户名
	Password    string    `gorm:"size:255;not null" json:"password"`         // 加密存储的密码
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// TableName 指定表名
func (Credential) TableName() string {
	return "credentials"
}
