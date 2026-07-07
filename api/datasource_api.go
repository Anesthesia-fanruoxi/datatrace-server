package api

import (
	"datatrace/common"
	"datatrace/services"

	"github.com/gin-gonic/gin"
)

// DataSourceAPI 数据源管理 API
type DataSourceAPI struct {
	service   *services.DataSourceService
	healthSvc *services.HealthCheckService
}

// NewDataSourceAPI 创建数据源 API
func NewDataSourceAPI(svc *services.DataSourceService, healthSvc *services.HealthCheckService) *DataSourceAPI {
	return &DataSourceAPI{service: svc, healthSvc: healthSvc}
}

// List 获取数据源列表
func (api *DataSourceAPI) List(c *gin.Context) {
	datasources, err := api.service.List()
	if err != nil {
		common.InternalServerError(c, "获取数据源列表失败: "+err.Error())
		return
	}
	common.Success(c, datasources)
}

// Get 获取单个数据源
func (api *DataSourceAPI) Get(c *gin.Context) {
	id := c.Param("id")
	ds, err := api.service.GetByID(id)
	if err != nil {
		common.NotFound(c, "数据源不存在")
		return
	}
	common.Success(c, ds)
}

// Create 创建数据源
func (api *DataSourceAPI) Create(c *gin.Context) {
	var req services.CreateDataSourceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.BadRequest(c, "参数错误: "+err.Error())
		return
	}

	ds, err := api.service.Create(&req)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, ds)
}

// Update 更新数据源
func (api *DataSourceAPI) Update(c *gin.Context) {
	id := c.Param("id")
	var req services.UpdateDataSourceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.BadRequest(c, "参数错误: "+err.Error())
		return
	}

	ds, err := api.service.Update(id, &req)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, ds)
}

// Delete 删除数据源
func (api *DataSourceAPI) Delete(c *gin.Context) {
	id := c.Param("id")
	if err := api.service.Delete(id); err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, nil)
}

// TestConnection 测试数据源连接
func (api *DataSourceAPI) TestConnection(c *gin.Context) {
	// 支持两种模式：按 ID 测试 或 按 payload 测试
	id := c.Param("id")
	if id != "" {
		if err := api.service.TestConnection(id); err != nil {
			common.BadRequest(c, "连接测试失败: "+err.Error())
			return
		}
		common.Success(c, gin.H{"message": "连接成功"})
		return
	}

	// 按 payload 测试
	var req struct {
		Host         string `json:"host" binding:"required"`
		Port         int    `json:"port" binding:"required"`
		Username     string `json:"username"`
		Password     string `json:"password"`
		DatabaseName string `json:"database_name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		common.BadRequest(c, "参数错误: "+err.Error())
		return
	}
	if err := api.service.TestConnectionByPayload(req.Host, req.Port, req.Username, req.Password, req.DatabaseName); err != nil {
		common.BadRequest(c, "连接测试失败: "+err.Error())
		return
	}
	common.Success(c, gin.H{"message": "连接成功"})
}

// GetDatabases 获取数据源的数据库列表
func (api *DataSourceAPI) GetDatabases(c *gin.Context) {
	id := c.Param("id")
	result, err := api.service.GetDatabaseTables(id)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, result)
}

// GetTables 获取数据源的表列表
func (api *DataSourceAPI) GetTables(c *gin.Context) {
	id := c.Param("id")
	result, err := api.service.GetDatabaseTables(id)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, result)
}

// GetDatabaseTables 获取数据源的数据库+表结构
func (api *DataSourceAPI) GetDatabaseTables(c *gin.Context) {
	id := c.Param("id")
	result, err := api.service.GetDatabaseTables(id)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, result)
}

// GetTableColumns 获取指定表的字段信息
func (api *DataSourceAPI) GetTableColumns(c *gin.Context) {
	id := c.Param("id")
	database := c.Param("database")
	table := c.Param("table")
	result, err := api.service.GetTableColumns(id, database, table)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, result)
}

// GetHealth 获取所有数据源健康状态
func (api *DataSourceAPI) GetHealth(c *gin.Context) {
	common.Success(c, api.healthSvc.GetAllStatuses())
}

// CheckHealth 立即检查单个数据源
func (api *DataSourceAPI) CheckHealth(c *gin.Context) {
	id := c.Param("id")
	status, err := api.healthSvc.CheckNow(id)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, status)
}
