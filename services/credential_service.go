package services

import (
	"datatrace/models"
	"datatrace/utils"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// CredentialService 凭据管理服务
type CredentialService struct {
	db     *gorm.DB
	crypto *utils.CryptoService
}

// NewCredentialService 创建凭据服务
func NewCredentialService(db *gorm.DB, crypto *utils.CryptoService) *CredentialService {
	return &CredentialService{db: db, crypto: crypto}
}

// CreateRequest 创建凭据请求
type CreateCredentialRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Username    string `json:"username" binding:"required"`
	Password    string `json:"password" binding:"required"`
}

// UpdateCredentialRequest 更新凭据请求
type UpdateCredentialRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Username    string `json:"username"`
	Password    string `json:"password"`
}

// Create 创建凭据
func (s *CredentialService) Create(req *CreateCredentialRequest) (*models.Credential, error) {
	encryptedPwd, err := s.crypto.Encrypt(req.Password)
	if err != nil {
		return nil, fmt.Errorf("密码加密失败: %w", err)
	}

	cred := &models.Credential{
		ID:          uuid.New().String(),
		Name:        req.Name,
		Description: req.Description,
		Username:    req.Username,
		Password:    encryptedPwd,
	}

	if err := s.db.Create(cred).Error; err != nil {
		return nil, fmt.Errorf("创建凭据失败: %w", err)
	}
	return cred, nil
}

// List 获取凭据列表（密码脱敏）
func (s *CredentialService) List() ([]models.Credential, error) {
	var creds []models.Credential
	if err := s.db.Order("created_at DESC").Find(&creds).Error; err != nil {
		return nil, err
	}
	// 密码脱敏
	for i := range creds {
		creds[i].Password = "******"
	}
	return creds, nil
}

// GetByID 根据 ID 获取凭据
func (s *CredentialService) GetByID(id string) (*models.Credential, error) {
	var cred models.Credential
	if err := s.db.First(&cred, "id = ?", id).Error; err != nil {
		return nil, err
	}
	cred.Password = "******"
	return &cred, nil
}

// GetByIDWithPassword 根据 ID 获取凭据（含解密密码，内部使用）
func (s *CredentialService) GetByIDWithPassword(id string) (*models.Credential, error) {
	var cred models.Credential
	if err := s.db.First(&cred, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &cred, nil
}

// DecryptPassword 解密凭据密码
func (s *CredentialService) DecryptPassword(cred *models.Credential) (string, error) {
	return s.crypto.Decrypt(cred.Password)
}

// DecryptString 解密任意加密字符串
func (s *CredentialService) DecryptString(encrypted string) (string, error) {
	return s.crypto.Decrypt(encrypted)
}

// Update 更新凭据
func (s *CredentialService) Update(id string, req *UpdateCredentialRequest) (*models.Credential, error) {
	var cred models.Credential
	if err := s.db.First(&cred, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("凭据不存在: %w", err)
	}

	updates := map[string]interface{}{}
	if req.Name != "" {
		updates["name"] = req.Name
	}
	if req.Description != "" {
		updates["description"] = req.Description
	}
	if req.Username != "" {
		updates["username"] = req.Username
	}
	if req.Password != "" {
		encryptedPwd, err := s.crypto.Encrypt(req.Password)
		if err != nil {
			return nil, fmt.Errorf("密码加密失败: %w", err)
		}
		updates["password"] = encryptedPwd
	}
	updates["updated_at"] = time.Now()

	if err := s.db.Model(&cred).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("更新凭据失败: %w", err)
	}

	cred.Password = "******"
	return &cred, nil
}

// Delete 删除凭据（被数据源引用时拒绝）
func (s *CredentialService) Delete(id string) error {
	// 检查是否被数据源引用
	var count int64
	s.db.Model(&models.DataSource{}).Where("credential_id = ?", id).Count(&count)
	if count > 0 {
		return fmt.Errorf("该凭据正被 %d 个数据源使用，无法删除", count)
	}

	if err := s.db.Delete(&models.Credential{}, "id = ?", id).Error; err != nil {
		return fmt.Errorf("删除凭据失败: %w", err)
	}
	return nil
}
