package api

import (
	"datatrace/common"
	"datatrace/services"

	"github.com/gin-gonic/gin"
)

// CredentialAPI 凭据管理 API
type CredentialAPI struct {
	service *services.CredentialService
}

// NewCredentialAPI 创建凭据 API
func NewCredentialAPI(svc *services.CredentialService) *CredentialAPI {
	return &CredentialAPI{service: svc}
}

// List 获取凭据列表
func (api *CredentialAPI) List(c *gin.Context) {
	creds, err := api.service.List()
	if err != nil {
		common.InternalServerError(c, "获取凭据列表失败: "+err.Error())
		return
	}
	common.Success(c, creds)
}

// Get 获取单个凭据
func (api *CredentialAPI) Get(c *gin.Context) {
	id := c.Param("id")
	cred, err := api.service.GetByID(id)
	if err != nil {
		common.NotFound(c, "凭据不存在")
		return
	}
	common.Success(c, cred)
}

// Create 创建凭据
func (api *CredentialAPI) Create(c *gin.Context) {
	var req services.CreateCredentialRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.BadRequest(c, "参数错误: "+err.Error())
		return
	}

	cred, err := api.service.Create(&req)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, cred)
}

// Update 更新凭据
func (api *CredentialAPI) Update(c *gin.Context) {
	id := c.Param("id")
	var req services.UpdateCredentialRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.BadRequest(c, "参数错误: "+err.Error())
		return
	}

	cred, err := api.service.Update(id, &req)
	if err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, cred)
}

// Delete 删除凭据
func (api *CredentialAPI) Delete(c *gin.Context) {
	id := c.Param("id")
	if err := api.service.Delete(id); err != nil {
		common.BadRequest(c, err.Error())
		return
	}
	common.Success(c, nil)
}

// TestConnection 测试凭据连接（通过数据源间接测试）
func (api *CredentialAPI) TestConnection(c *gin.Context) {
	// 凭据本身无法直接测试连接，需要通过数据源测试
	common.Success(c, gin.H{"message": "请通过数据源测试连接功能验证凭据有效性"})
}
