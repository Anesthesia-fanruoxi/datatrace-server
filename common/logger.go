package common

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	// LogFile 当前日志文件句柄
	logFile *os.File
	// logMutex 保护日志文件操作的互斥锁
	logMutex sync.Mutex
	// logConfig 日志配置
	logConfig *LogConfig
	// multiWriter 多路输出写入器
	multiWriter io.Writer
)

// LogConfig 日志配置
type LogConfig struct {
	File       string
	Level      string
	Console    bool
	MaxSize    int64 // 字节数
	MaxBackups int
	MaxAge     int
	Compress   bool
}

// DefaultLogConfig 默认日志配置
func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		File:       "logs/datatrace.log",
		Level:      "info",
		Console:    true,
		MaxSize:    100 * 1024 * 1024, // 100MB
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   true,
	}
}

// SetupLogger 初始化日志系统
func SetupLogger(cfg *LogConfig) error {
	logConfig = cfg

	// 创建日志目录
	logDir := filepath.Dir(cfg.File)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("创建日志目录失败: %w", err)
	}

	// 打开日志文件（追加模式）
	file, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("打开日志文件失败: %w", err)
	}

	logFile = file

	// 创建多路输出写入器
	writers := []io.Writer{file}
	if cfg.Console {
		writers = append(writers, os.Stdout)
	}

	multiWriter = io.MultiWriter(writers...)

	// 设置标准 log 包的输出
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 写入启动日志
	log.Printf("========== DataTrace 日志系统初始化完成 ==========")
	log.Printf("日志文件: %s", cfg.File)
	log.Printf("日志级别: %s", cfg.Level)
	log.Printf("控制台输出: %v", cfg.Console)

	return nil
}

// CloseLogger 关闭日志系统
func CloseLogger() {
	if logFile != nil {
		log.Println("========== DataTrace 正在关闭日志系统 ==========")
		logFile.Close()
	}
}

// RotateLog 日志轮转（可按需调用或定期执行）
func RotateLog() error {
	logMutex.Lock()
	defer logMutex.Unlock()

	if logFile == nil || logConfig == nil {
		return nil
	}

	// 获取当前文件信息
	fileInfo, err := logFile.Stat()
	if err != nil {
		return fmt.Errorf("获取日志文件信息失败: %w", err)
	}

	// 检查文件大小
	if fileInfo.Size() < logConfig.MaxSize {
		return nil
	}

	// 关闭当前文件
	logFile.Close()

	// 轮转日志文件
	if err := rotateLogFiles(logConfig.File, logConfig.MaxBackups); err != nil {
		return fmt.Errorf("轮转日志文件失败: %w", err)
	}

	// 重新打开文件
	file, err := os.OpenFile(logConfig.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("重新打开日志文件失败: %w", err)
	}

	logFile = file

	// 更新多路输出
	writers := []io.Writer{file}
	if logConfig.Console {
		writers = append(writers, os.Stdout)
	}
	multiWriter = io.MultiWriter(writers...)
	log.SetOutput(multiWriter)

	log.Println("========== 日志文件已轮转 ==========")
	return nil
}

// rotateLogFiles 轮转日志文件
func rotateLogFiles(logFile string, maxBackups int) error {
	// 删除最旧的备份（如果存在）
	if maxBackups > 0 {
		oldestBackup := fmt.Sprintf("%s.%d", logFile, maxBackups)
		if _, err := os.Stat(oldestBackup); err == nil {
			os.Remove(oldestBackup)
		}
	}

	// 移动现有备份
	for i := maxBackups - 1; i >= 1; i-- {
		oldName := fmt.Sprintf("%s.%d", logFile, i)
		newName := fmt.Sprintf("%s.%d", logFile, i+1)
		if _, err := os.Stat(oldName); err == nil {
			os.Rename(oldName, newName)
		}
	}

	// 将当前日志文件移动为备份
	backupName := fmt.Sprintf("%s.1", logFile)
	if _, err := os.Stat(logFile); err == nil {
		if err := os.Rename(logFile, backupName); err != nil {
			return err
		}
	}

	return nil
}

// Logger 日志中间件（Gin）
func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 开始时间
		startTime := time.Now()

		// 处理请求
		c.Next()

		// 结束时间
		endTime := time.Now()

		// 执行时间
		latencyTime := endTime.Sub(startTime)

		// 请求方式
		reqMethod := c.Request.Method

		// 请求路由
		reqUri := c.Request.RequestURI

		// 状态码
		statusCode := c.Writer.Status()

		// 请求IP
		clientIP := c.ClientIP()

		// 日志格式
		log.Printf("| %3d | %13v | %15s | %s | %s |",
			statusCode,
			latencyTime,
			clientIP,
			reqMethod,
			reqUri,
		)
	}
}

// LogInfo 信息日志
func LogInfo(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

// LogWarn 警告日志
func LogWarn(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

// LogError 错误日志
func LogError(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

// LogDebug 调试日志
func LogDebug(format string, args ...interface{}) {
	if logConfig != nil && logConfig.Level == "debug" {
		log.Printf("[DEBUG] "+format, args...)
	}
}
