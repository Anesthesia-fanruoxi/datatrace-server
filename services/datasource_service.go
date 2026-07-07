package services

import (
	"context"
	"database/sql"
	"datatrace/models"
	"datatrace/utils"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// DataSourceService 数据源管理服务
type DataSourceService struct {
	db      *gorm.DB
	crypto  *utils.CryptoService
	credSvc *CredentialService
}

// NewDataSourceService 创建数据源服务
func NewDataSourceService(db *gorm.DB, crypto *utils.CryptoService, credSvc *CredentialService) *DataSourceService {
	return &DataSourceService{db: db, crypto: crypto, credSvc: credSvc}
}

// CreateDataSourceRequest 创建数据源请求
type CreateDataSourceRequest struct {
	Name         string  `json:"name" binding:"required"`
	Type         string  `json:"type" binding:"required,oneof=mysql"`
	Host         string  `json:"host" binding:"required"`
	Port         int     `json:"port" binding:"required,min=1,max=65535"`
	CredentialID *string `json:"credential_id"`
	Username     string  `json:"username"`
	Password     string  `json:"password"`
	DatabaseName string  `json:"database_name" binding:"required"`
}

// UpdateDataSourceRequest 更新数据源请求
type UpdateDataSourceRequest struct {
	Name         string  `json:"name"`
	Host         string  `json:"host"`
	Port         int     `json:"port"`
	CredentialID *string `json:"credential_id"`
	Username     string  `json:"username"`
	Password     string  `json:"password"`
	DatabaseName string  `json:"database_name"`
}

// Create 创建数据源
func (s *DataSourceService) Create(req *CreateDataSourceRequest) (*models.DataSource, error) {
	ds := &models.DataSource{
		ID:           uuid.New().String(),
		Name:         req.Name,
		Type:         req.Type,
		Host:         req.Host,
		Port:         req.Port,
		DatabaseName: req.DatabaseName,
		CredentialID: req.CredentialID,
	}

	// 如果没有指定凭据，使用用户名密码
	if req.CredentialID == nil {
		if req.Username == "" {
			return nil, fmt.Errorf("用户名不能为空")
		}
		if req.Password == "" {
			return nil, fmt.Errorf("密码不能为空")
		}
		ds.Username = req.Username
		encryptedPwd, err := s.crypto.Encrypt(req.Password)
		if err != nil {
			return nil, fmt.Errorf("密码加密失败: %w", err)
		}
		ds.Password = encryptedPwd
	}

	if err := s.db.Create(ds).Error; err != nil {
		return nil, fmt.Errorf("创建数据源失败: %w", err)
	}
	return ds, nil
}

// List 获取数据源列表
func (s *DataSourceService) List() ([]models.DataSource, error) {
	var datasources []models.DataSource
	if err := s.db.Order("created_at DESC").Find(&datasources).Error; err != nil {
		return nil, err
	}
	// 密码脱敏
	for i := range datasources {
		datasources[i].Password = "******"
	}
	return datasources, nil
}

// GetByID 根据 ID 获取数据源
func (s *DataSourceService) GetByID(id string) (*models.DataSource, error) {
	var ds models.DataSource
	if err := s.db.First(&ds, "id = ?", id).Error; err != nil {
		return nil, err
	}
	ds.Password = "******"
	return &ds, nil
}

// Update 更新数据源
func (s *DataSourceService) Update(id string, req *UpdateDataSourceRequest) (*models.DataSource, error) {
	var ds models.DataSource
	if err := s.db.First(&ds, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("数据源不存在: %w", err)
	}

	updates := map[string]interface{}{}
	if req.Name != "" {
		updates["name"] = req.Name
	}
	if req.Host != "" {
		updates["host"] = req.Host
	}
	if req.Port > 0 {
		updates["port"] = req.Port
	}
	if req.CredentialID != nil {
		updates["credential_id"] = req.CredentialID
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
	if req.DatabaseName != "" {
		updates["database_name"] = req.DatabaseName
	}
	updates["updated_at"] = time.Now()

	if err := s.db.Model(&ds).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("更新数据源失败: %w", err)
	}

	ds.Password = "******"
	return &ds, nil
}

// Delete 删除数据源（被任务引用时拒绝）
func (s *DataSourceService) Delete(id string) error {
	// 检查是否被任务引用
	var count int64
	s.db.Model(&models.SyncTask{}).Where("source_id = ? OR target_id = ?", id, id).Count(&count)
	if count > 0 {
		return fmt.Errorf("该数据源正被 %d 个任务使用，无法删除", count)
	}

	if err := s.db.Delete(&models.DataSource{}, "id = ?", id).Error; err != nil {
		return fmt.Errorf("删除数据源失败: %w", err)
	}
	return nil
}

// TestConnection 测试数据源连接
func (s *DataSourceService) TestConnection(id string) error {
	var ds models.DataSource
	if err := s.db.First(&ds, "id = ?", id).Error; err != nil {
		return fmt.Errorf("数据源不存在: %w", err)
	}

	// 获取密码
	var password string
	if ds.CredentialID != nil && *ds.CredentialID != "" {
		cred, err := s.credSvc.GetByIDWithPassword(*ds.CredentialID)
		if err != nil {
			return fmt.Errorf("获取凭据失败: %w", err)
		}
		decrypted, err := s.credSvc.DecryptPassword(cred)
		if err != nil {
			return fmt.Errorf("解密密码失败: %w", err)
		}
		password = decrypted
	} else {
		decrypted, err := s.crypto.Decrypt(ds.Password)
		if err != nil {
			return fmt.Errorf("解密密码失败: %w", err)
		}
		password = decrypted
	}

	// 构建 DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		ds.Username, password, ds.Host, ds.Port, ds.DatabaseName)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("连接测试失败: %w", err)
	}

	return nil
}

// GetConnectionDSN 获取数据源连接字符串（内部使用）
func (s *DataSourceService) GetConnectionDSN(ds *models.DataSource) (string, error) {
	var password string
	if ds.CredentialID != nil && *ds.CredentialID != "" {
		cred, err := s.credSvc.GetByIDWithPassword(*ds.CredentialID)
		if err != nil {
			return "", fmt.Errorf("获取凭据失败: %w", err)
		}
		decrypted, err := s.credSvc.DecryptPassword(cred)
		if err != nil {
			return "", fmt.Errorf("解密密码失败: %w", err)
		}
		password = decrypted
	} else {
		decrypted, err := s.crypto.Decrypt(ds.Password)
		if err != nil {
			return "", fmt.Errorf("解密密码失败: %w", err)
		}
		password = decrypted
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		ds.Username, password, ds.Host, ds.Port, ds.DatabaseName), nil
}

// DatabaseTablesResult 数据库及表结构
type DatabaseTablesResult struct {
	Database string   `json:"database"`
	Tables   []string `json:"tables"`
}

// TableColumnResult 表字段信息
type TableColumnResult struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	IsPrimary bool   `json:"is_primary"`
}

// GetDatabaseTables 获取数据源的所有数据库和表
func (s *DataSourceService) GetDatabaseTables(id string) ([]DatabaseTablesResult, error) {
	var ds models.DataSource
	if err := s.db.First(&ds, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("数据源不存在: %w", err)
	}

	dsn, err := s.GetConnectionDSN(&ds)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer db.Close()

	// 获取数据库列表
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("查询数据库失败: %w", err)
	}
	defer rows.Close()

	var result []DatabaseTablesResult
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			continue
		}
		// 跳过系统库
		if dbName == "information_schema" || dbName == "mysql" || dbName == "performance_schema" || dbName == "sys" {
			continue
		}

		// 获取表列表
		tables, _ := getTablesInDB(db, dbName)
		if len(tables) > 0 {
			result = append(result, DatabaseTablesResult{Database: dbName, Tables: tables})
		}
	}
	return result, nil
}

func getTablesInDB(db *sql.DB, dbName string) ([]string, error) {
	db.Exec("USE `" + dbName + "`")
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

// GetTableColumns 获取指定表的字段信息
func (s *DataSourceService) GetTableColumns(id, database, table string) ([]TableColumnResult, error) {
	var ds models.DataSource
	if err := s.db.First(&ds, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("数据源不存在: %w", err)
	}

	dsn, err := s.GetConnectionDSN(&ds)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION", database, table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询字段失败: %w", err)
	}
	defer rows.Close()

	var columns []TableColumnResult
	for rows.Next() {
		var name, colType, key string
		if err := rows.Scan(&name, &colType, &key); err == nil {
			columns = append(columns, TableColumnResult{
				Name:      name,
				Type:      colType,
				IsPrimary: key == "PRI",
			})
		}
	}
	return columns, nil
}

// TestConnectionByPayload 通过原始凭据测试连接（非 ID）
func (s *DataSourceService) TestConnectionByPayload(host string, port int, username, password, databaseName string) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		username, password, host, port, databaseName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("连接测试失败: %w", err)
	}
	return nil
}
