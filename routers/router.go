package routers

import (
	"datatrace/api"
	"datatrace/app"
	"datatrace/common"
	"net/http"

	"github.com/gin-gonic/gin"
)

// SetupRouter 设置路由，接收 AppContainer 注入依赖
func SetupRouter(application *app.AppContainer) *gin.Engine {
	r := gin.New()

	// 使用自定义日志中间件（写入文件）
	r.Use(common.Logger())

	// 使用恢复中间件
	r.Use(gin.Recovery())

	// 健康检查
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// SSE 统一端点
	r.GET("/api/v1/sse", application.SSEHub.Handler())

	// API 路由组
	v1 := r.Group("/api/v1")

	// 凭据管理
	credentialAPI := api.NewCredentialAPI(application.CredentialSvc)
	credentials := v1.Group("/credentials")
	{
		credentials.GET("", credentialAPI.List)
		credentials.GET("/:id", credentialAPI.Get)
		credentials.POST("", credentialAPI.Create)
		credentials.PUT("/:id", credentialAPI.Update)
		credentials.DELETE("/:id", credentialAPI.Delete)
		credentials.POST("/:id/test", credentialAPI.TestConnection)
	}

	// 数据源管理
	datasourceAPI := api.NewDataSourceAPI(application.DataSourceSvc, application.HealthCheckSvc)
	datasources := v1.Group("/datasources")
	{
		datasources.GET("", datasourceAPI.List)
		datasources.GET("/:id", datasourceAPI.Get)
		datasources.POST("", datasourceAPI.Create)
		datasources.PUT("/:id", datasourceAPI.Update)
		datasources.DELETE("/:id", datasourceAPI.Delete)
		datasources.POST("/test", datasourceAPI.TestConnection) // 无 ID，按 payload 测试（必须放在 /:id/test 之前）
		datasources.POST("/:id/test", datasourceAPI.TestConnection)
		datasources.GET("/:id/databases", datasourceAPI.GetDatabases)
		datasources.GET("/:id/tables", datasourceAPI.GetTables)
		datasources.GET("/:id/database-tables", datasourceAPI.GetDatabaseTables)
		datasources.GET("/:id/tables/:database/:table/columns", datasourceAPI.GetTableColumns)
		datasources.GET("/health", datasourceAPI.GetHealth)
		datasources.POST("/:id/health", datasourceAPI.CheckHealth)
	}

	// 任务管理
	taskAPI := api.NewTaskAPI(application.TaskSvc)
	tasks := v1.Group("/tasks")
	{
		tasks.GET("", taskAPI.List)
		tasks.GET("/:id", taskAPI.Get)
		tasks.POST("", taskAPI.Create)
		tasks.PUT("/:id", taskAPI.Update)
		tasks.DELETE("/:id", taskAPI.Delete)
		tasks.GET("/:id/config", taskAPI.GetConfig)
		tasks.GET("/:id/config-view", taskAPI.GetConfigView)
		tasks.PUT("/:id/config", taskAPI.UpdateConfig)
		tasks.GET("/stats", taskAPI.GetStats)

		// 任务控制
		if application.TaskControlSvc != nil {
			controlAPI := api.NewTaskControlAPI(application.TaskControlSvc, application.TaskLogSvc, application.TableStatusSvc)
			tasks.POST("/:id/start", controlAPI.Start)
			tasks.POST("/:id/stop", controlAPI.Stop)
			tasks.POST("/:id/pause", controlAPI.Pause)
			tasks.POST("/:id/resume", controlAPI.Resume)
			tasks.GET("/:id/progress", controlAPI.GetProgress)
			tasks.GET("/:id/logs", controlAPI.GetLogs)
			tasks.DELETE("/:id/logs", controlAPI.ClearLogs)
			tasks.GET("/:id/table-status", controlAPI.GetTableStatus)
			tasks.GET("/:id/step-status", controlAPI.GetStepStatus)
		}
	}

	return r
}
