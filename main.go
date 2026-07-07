package main

import (
	"datatrace/app"
	"datatrace/common"
	"datatrace/config"
	"datatrace/database"
	"datatrace/routers"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	// 1. 加载配置
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	// 1.1 初始化日志系统
	logConfig := &common.LogConfig{
		File:       cfg.Logging.File,
		Level:      cfg.Logging.Level,
		Console:    cfg.Logging.Console,
		MaxSize:    int64(cfg.Logging.MaxSize) * 1024 * 1024, // MB 转字节
		MaxBackups: cfg.Logging.MaxBackups,
		MaxAge:     cfg.Logging.MaxAge,
		Compress:   cfg.Logging.Compress,
	}
	if err := common.SetupLogger(logConfig); err != nil {
		log.Printf("⚠️ 初始化日志系统失败（将继续使用控制台日志）: %v", err)
	}
	defer common.CloseLogger()

	// 2. 初始化 MySQL
	db, err := database.InitDB(&cfg.Database)
	if err != nil {
		log.Fatalf("❌ 初始化数据库失败: %v", err)
	}

	// 3. 初始化 Redis（可选，失败不阻止启动）
	redisClient, err := database.InitRedis(&cfg.Redis)
	if err != nil {
		log.Printf("⚠️ Redis 初始化失败（将降级到内存缓存）: %v", err)
	}

	// 4. 创建 AppContainer（依赖注入）
	application, err := app.NewAppContainer(cfg, db, redisClient)
	if err != nil {
		log.Fatalf("❌ 初始化 AppContainer 失败: %v", err)
	}
	defer application.Shutdown()

	// 5. 启动应用
	if err := application.Start(); err != nil {
		log.Fatalf("❌ 启动应用失败: %v", err)
	}

	// 6. 设置 Gin
	gin.SetMode(cfg.Server.Mode)
	r := routers.SetupRouter(application)

	// 7. 启动服务器
	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	log.Println("========================================")
	log.Println("🚀 DataTrace 数据同步系统（重构版）")
	log.Println("========================================")
	log.Printf("📍 监听地址: http://localhost%s", addr)
	log.Printf("🏥 健康检查: http://localhost%s/health", addr)
	log.Printf("📡 SSE 端点:  http://localhost%s/api/v1/sse?topics=...", addr)
	log.Println("========================================")

	// 优雅关闭
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("收到信号 %v，正在关闭...", sig)
		application.Shutdown()
		common.CloseLogger()
		os.Exit(0)
	}()

	// 定期轮转日志（每小时检查一次）
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			if err := common.RotateLog(); err != nil {
				log.Printf("⚠️ 日志轮转失败: %v", err)
			}
		}
	}()

	if err := r.Run(addr); err != nil {
		log.Fatalf("❌ 启动服务器失败: %v", err)
	}
}
