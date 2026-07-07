package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"datatrace/config"

	"github.com/redis/go-redis/v9"
)

// CacheStore 缓存抽象接口，Redis 为可选增强，无 Redis 时降级到 MemoryCache
type CacheStore interface {
	// 基础 KV 操作
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Del(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)

	// Hash 操作（用于增量统计等）
	HGet(ctx context.Context, key, field string) (string, error)
	HSet(ctx context.Context, key string, fields map[string]interface{}) error
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error)

	// Set 操作（用于增量表列表等）
	SAdd(ctx context.Context, key string, members ...interface{}) error
	SMembers(ctx context.Context, key string) ([]string, error)
	SRem(ctx context.Context, key string, members ...interface{}) error

	// 生命周期
	Close() error
}

// NewCacheStore 根据配置创建 CacheStore。
// Redis 可用时返回 RedisCache，否则降级到 MemoryCache。
func NewCacheStore(cfg *config.RedisConfig, redisClient *redis.Client) CacheStore {
	if cfg.Enabled && redisClient != nil {
		log.Println("✅ CacheStore: 使用 Redis")
		return NewRedisCache(redisClient)
	}
	log.Println("⚠️  CacheStore: Redis 不可用，降级到内存缓存")
	return NewMemoryCache()
}

// ============================================================
// MemoryCache — 基于 sync.Map 的内存实现
// ============================================================

type memoryEntry struct {
	value     string
	expiresAt time.Time // zero value = 不过期
}

func (e *memoryEntry) isExpired() bool {
	if e.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(e.expiresAt)
}

// MemoryCache 内存缓存实现
type MemoryCache struct {
	mu   sync.RWMutex
	data map[string]*memoryEntry
	done chan struct{}
}

// NewMemoryCache 创建内存缓存，启动后台清理 goroutine
func NewMemoryCache() *MemoryCache {
	mc := &MemoryCache{
		data: make(map[string]*memoryEntry),
		done: make(chan struct{}),
	}
	go mc.cleanupLoop()
	return mc
}

func (m *MemoryCache) cleanupLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.cleanup()
		}
	}
}

func (m *MemoryCache) cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.data {
		if v.isExpired() {
			delete(m.data, k)
		}
	}
}

func (m *MemoryCache) Get(_ context.Context, key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok || entry.isExpired() {
		return "", fmt.Errorf("key not found: %s", key)
	}
	return entry.value, nil
}

func (m *MemoryCache) Set(_ context.Context, key string, value interface{}, ttl time.Duration) error {
	var strVal string
	switch v := value.(type) {
	case string:
		strVal = v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		strVal = string(b)
	}

	entry := &memoryEntry{value: strVal}
	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}

	m.mu.Lock()
	m.data[key] = entry
	m.mu.Unlock()
	return nil
}

func (m *MemoryCache) Del(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()
	return nil
}

func (m *MemoryCache) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok || entry.isExpired() {
		return false, nil
	}
	return true, nil
}

func (m *MemoryCache) HGet(_ context.Context, key, field string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok || entry.isExpired() {
		return "", fmt.Errorf("key not found: %s", key)
	}
	var hash map[string]string
	if err := json.Unmarshal([]byte(entry.value), &hash); err != nil {
		return "", err
	}
	val, ok := hash[field]
	if !ok {
		return "", fmt.Errorf("field not found: %s.%s", key, field)
	}
	return val, nil
}

func (m *MemoryCache) HSet(_ context.Context, key string, fields map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var hash map[string]string
	if entry, ok := m.data[key]; ok && !entry.isExpired() {
		json.Unmarshal([]byte(entry.value), &hash)
	}
	if hash == nil {
		hash = make(map[string]string)
	}

	for k, v := range fields {
		switch val := v.(type) {
		case string:
			hash[k] = val
		default:
			b, _ := json.Marshal(v)
			hash[k] = string(b)
		}
	}

	b, _ := json.Marshal(hash)
	m.data[key] = &memoryEntry{value: string(b)}
	return nil
}

func (m *MemoryCache) HGetAll(_ context.Context, key string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok || entry.isExpired() {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	var hash map[string]string
	if err := json.Unmarshal([]byte(entry.value), &hash); err != nil {
		return nil, err
	}
	return hash, nil
}

func (m *MemoryCache) HIncrBy(_ context.Context, key, field string, incr int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var hash map[string]int64
	if entry, ok := m.data[key]; ok && !entry.isExpired() {
		json.Unmarshal([]byte(entry.value), &hash)
	}
	if hash == nil {
		hash = make(map[string]int64)
	}

	hash[field] += incr

	b, _ := json.Marshal(hash)
	m.data[key] = &memoryEntry{value: string(b)}
	return hash[field], nil
}

func (m *MemoryCache) SAdd(_ context.Context, key string, members ...interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	set := make(map[string]struct{})
	if entry, ok := m.data[key]; ok && !entry.isExpired() {
		json.Unmarshal([]byte(entry.value), &set)
	}

	for _, member := range members {
		var s string
		switch v := member.(type) {
		case string:
			s = v
		default:
			b, _ := json.Marshal(v)
			s = string(b)
		}
		set[s] = struct{}{}
	}

	b, _ := json.Marshal(set)
	m.data[key] = &memoryEntry{value: string(b)}
	return nil
}

func (m *MemoryCache) SMembers(_ context.Context, key string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok || entry.isExpired() {
		return nil, nil
	}
	var set map[string]struct{}
	if err := json.Unmarshal([]byte(entry.value), &set); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(set))
	for k := range set {
		result = append(result, k)
	}
	return result, nil
}

func (m *MemoryCache) SRem(_ context.Context, key string, members ...interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	set := make(map[string]struct{})
	if entry, ok := m.data[key]; ok && !entry.isExpired() {
		json.Unmarshal([]byte(entry.value), &set)
	}

	for _, member := range members {
		var s string
		switch v := member.(type) {
		case string:
			s = v
		default:
			b, _ := json.Marshal(v)
			s = string(b)
		}
		delete(set, s)
	}

	b, _ := json.Marshal(set)
	m.data[key] = &memoryEntry{value: string(b)}
	return nil
}

func (m *MemoryCache) Close() error {
	close(m.done)
	return nil
}

// ============================================================
// RedisCache — 基于 go-redis 的实现
// ============================================================

// RedisCache Redis 缓存实现
type RedisCache struct {
	client *redis.Client
}

// NewRedisCache 创建 Redis 缓存
func NewRedisCache(client *redis.Client) *RedisCache {
	return &RedisCache{client: client}
}

func (r *RedisCache) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

func (r *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

func (r *RedisCache) Del(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

func (r *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	n, err := r.client.Exists(ctx, key).Result()
	return n > 0, err
}

func (r *RedisCache) HGet(ctx context.Context, key, field string) (string, error) {
	return r.client.HGet(ctx, key, field).Result()
}

func (r *RedisCache) HSet(ctx context.Context, key string, fields map[string]interface{}) error {
	return r.client.HSet(ctx, key, fields).Err()
}

func (r *RedisCache) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return r.client.HGetAll(ctx, key).Result()
}

func (r *RedisCache) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	return r.client.HIncrBy(ctx, key, field, incr).Result()
}

func (r *RedisCache) SAdd(ctx context.Context, key string, members ...interface{}) error {
	return r.client.SAdd(ctx, key, members...).Err()
}

func (r *RedisCache) SMembers(ctx context.Context, key string) ([]string, error) {
	return r.client.SMembers(ctx, key).Result()
}

func (r *RedisCache) SRem(ctx context.Context, key string, members ...interface{}) error {
	return r.client.SRem(ctx, key, members...).Err()
}

func (r *RedisCache) Close() error {
	return r.client.Close()
}
