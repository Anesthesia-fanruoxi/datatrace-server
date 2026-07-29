package api

import (
	"datatrace/common"
	"datatrace/services"

	"github.com/gin-gonic/gin"
)

// OverviewAPI 概览页聚合 API
type OverviewAPI struct {
	service *services.OverviewService
}

// NewOverviewAPI 创建概览 API
func NewOverviewAPI(svc *services.OverviewService) *OverviewAPI {
	return &OverviewAPI{service: svc}
}

// GetOverview 一次性返回概览页全部数据（统计、数据源健康、状态分布、任务完成情况）
func (api *OverviewAPI) GetOverview(c *gin.Context) {
	common.Success(c, api.service.GetOverview())
}
