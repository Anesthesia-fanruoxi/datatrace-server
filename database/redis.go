package database

import (
	"context"
	"datatrace/config"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// InitRedis 初始化Redis连接，返回 *redis.Client。
// 如果 Redis 未启用或连接失败，返回 nil（不阻止启动）。
func InitRedis(cfg *config.RedisConfig) (*redis.Client, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: 5,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("Redis连接失败: %w", err)
	}

	return client, nil
}
