package services

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// BinlogEvent Binlog 事件
type BinlogEvent struct {
	Type     string                 `json:"type"` // insert/update/delete
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Data     map[string]interface{} `json:"data"`
	OldData  map[string]interface{} `json:"old_data,omitempty"` // update 时的旧值
}

// BinlogQueue 事件队列接口
type BinlogQueue interface {
	Push(event *BinlogEvent) error
	Pop(ctx context.Context) (*BinlogEvent, error)
	Ack(event *BinlogEvent) error
	Len() int
	Close() error
}

// MemoryQueue 内存队列实现
type MemoryQueue struct {
	ch     chan *BinlogEvent
	closed bool
	mu     sync.Mutex
}

// NewMemoryQueue 创建内存队列
func NewMemoryQueue(bufferSize int) *MemoryQueue {
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	return &MemoryQueue{
		ch: make(chan *BinlogEvent, bufferSize),
	}
}

func (q *MemoryQueue) Push(event *BinlogEvent) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fmt.Errorf("队列已关闭")
	}
	q.mu.Unlock()

	select {
	case q.ch <- event:
		return nil
	default:
		return fmt.Errorf("队列已满")
	}
}

func (q *MemoryQueue) Pop(ctx context.Context) (*BinlogEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case event, ok := <-q.ch:
		if !ok {
			return nil, fmt.Errorf("队列已关闭")
		}
		return event, nil
	}
}

func (q *MemoryQueue) Ack(event *BinlogEvent) error {
	return nil // 内存队列无需 ACK
}

func (q *MemoryQueue) Len() int {
	return len(q.ch)
}

func (q *MemoryQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
	return nil
}

// RedisQueue Redis Stream 队列实现
type RedisQueue struct {
	client *redis.Client
	stream string
	group  string
}

// NewRedisQueue 创建 Redis 队列
func NewRedisQueue(client *redis.Client, stream, group string) *RedisQueue {
	ctx := context.Background()
	// 创建消费组（忽略已存在错误）
	client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	return &RedisQueue{client: client, stream: stream, group: group}
}

func (q *RedisQueue) Push(event *BinlogEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return q.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]interface{}{"data": string(data)},
	}).Err()
}

func (q *RedisQueue) Pop(ctx context.Context) (*BinlogEvent, error) {
	results, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: "consumer-1",
		Streams:  []string{q.stream, ">"},
		Count:    1,
		Block:    5 * time.Second,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return nil, nil // 超时
		}
		return nil, err
	}

	if len(results) == 0 || len(results[0].Messages) == 0 {
		return nil, nil
	}

	msg := results[0].Messages[0]
	dataStr, ok := msg.Values["data"].(string)
	if !ok {
		return nil, fmt.Errorf("无效的消息数据")
	}

	var event BinlogEvent
	if err := json.Unmarshal([]byte(dataStr), &event); err != nil {
		return nil, err
	}
	return &event, nil
}

func (q *RedisQueue) Ack(event *BinlogEvent) error {
	// Redis Stream ACK 需要 message ID，这里简化处理
	return nil
}

func (q *RedisQueue) Len() int {
	info, _ := q.client.XInfoStream(context.Background(), q.stream).Result()
	if info != nil {
		return int(info.Length)
	}
	return 0
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}
