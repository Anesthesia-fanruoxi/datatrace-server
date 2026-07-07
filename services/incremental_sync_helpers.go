package services

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

// buildCanalConfig 构建 Canal 配置
func buildCanalConfig(sourceDSN string) *canal.Config {
	return &canal.Config{
		Addr:     extractHost(sourceDSN),
		User:     extractUser(sourceDSN),
		Password: extractPassword(sourceDSN),
	}
}

// extractHost 从 DSN 提取 host:port
func extractHost(dsn string) string {
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '@' && i+1 < len(dsn) && dsn[i+1] == 't' {
			start := i + 5 // skip @tcp(
			for j := start; j < len(dsn); j++ {
				if dsn[j] == ')' {
					return dsn[start:j]
				}
			}
		}
	}
	return "127.0.0.1:3306"
}

// extractUser 从 DSN 提取用户名
func extractUser(dsn string) string {
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == ':' {
			return dsn[:i]
		}
	}
	return "root"
}

// extractPassword 从 DSN 提取密码
func extractPassword(dsn string) string {
	start := -1
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == ':' && start == -1 {
			start = i + 1
		}
		if dsn[i] == '@' && start != -1 {
			return dsn[start:i]
		}
	}
	return ""
}
