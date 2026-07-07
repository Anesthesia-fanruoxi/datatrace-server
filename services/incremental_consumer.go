package services

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

// IncrementalConsumer 增量消费引擎
type IncrementalConsumer struct {
	queue           BinlogQueue
	handler         *EventSQLHandler
	mapperPtr       atomic.Pointer[ConsumerMapper]
	eventBus        *EventBus
	taskID          string
	strategy        string // skip/pause
	eventsProcessed int64
	eventsFailed    int64
	mu              sync.Mutex
}

// NewIncrementalConsumer 创建消费引擎
func NewIncrementalConsumer(queue BinlogQueue, db *sql.DB, mapper *ConsumerMapper, eventBus *EventBus, taskID string, strategy string) *IncrementalConsumer {
	c := &IncrementalConsumer{
		queue:    queue,
		handler:  NewEventSQLHandler(db),
		eventBus: eventBus,
		taskID:   taskID,
		strategy: strategy,
	}
	c.mapperPtr.Store(mapper)
	return c
}

// Consume 消费循环（阻塞直到 ctx 取消）
func (c *IncrementalConsumer) Consume(ctx context.Context) error {
	log.Printf("[Consumer] 开始消费 task=%s strategy=%s", c.taskID, c.strategy)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Consumer] 消费停止 task=%s processed=%d failed=%d",
				c.taskID, atomic.LoadInt64(&c.eventsProcessed), atomic.LoadInt64(&c.eventsFailed))
			return ctx.Err()
		default:
		}

		event, err := c.queue.Pop(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("[Consumer] 弹出事件失败: %v", err)
			continue
		}
		if event == nil {
			continue
		}

		if err := c.processEvent(event); err != nil {
			atomic.AddInt64(&c.eventsFailed, 1)

			// 冲突错误仅警告，不视为失败
			var conflictErr *ConflictError
			if errors.As(err, &conflictErr) {
				log.Printf("[Consumer] 冲突警告 %s: %v", conflictErr.Table, conflictErr)
				c.eventBus.Publish("incremental.conflict", map[string]interface{}{
					"task_id": c.taskID, "table": conflictErr.Table,
					"event": conflictErr.Event, "detail": conflictErr.Detail,
				})
			} else {
				log.Printf("[Consumer] 处理事件失败 %s.%s %s: %v", event.Database, event.Table, event.Type, err)
				c.eventBus.Publish("incremental.error", map[string]interface{}{
					"task_id":  c.taskID,
					"database": event.Database,
					"table":    event.Table,
					"type":     event.Type,
					"error":    err.Error(),
				})

				if c.strategy == "pause" {
					return fmt.Errorf("消费失败(pause策略): %w", err)
				}
			}
			// skip 策略：继续下一条
			continue
		}

		atomic.AddInt64(&c.eventsProcessed, 1)
		c.queue.Ack(event)

		// 定期上报统计
		if atomic.LoadInt64(&c.eventsProcessed)%100 == 0 {
			c.reportStats()
		}
	}
}

func (c *IncrementalConsumer) processEvent(event *BinlogEvent) error {
	mapper := c.mapperPtr.Load()
	targetDB, targetTable := mapper.MapTable(event.Database, event.Table)
	return c.handler.ApplyEvent(event, targetDB, targetTable)
}

// UpdateMapper 原子替换映射器（运行时热更新）
func (c *IncrementalConsumer) UpdateMapper(mapper *ConsumerMapper) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapperPtr.Store(mapper)
}

func (c *IncrementalConsumer) reportStats() {
	c.eventBus.Publish("incremental.stats", map[string]interface{}{
		"task_id":          c.taskID,
		"events_processed": atomic.LoadInt64(&c.eventsProcessed),
		"events_failed":    atomic.LoadInt64(&c.eventsFailed),
		"queue_len":        c.queue.Len(),
	})
}

// GetStats 获取消费统计
func (c *IncrementalConsumer) GetStats() map[string]int64 {
	return map[string]int64{
		"events_processed": atomic.LoadInt64(&c.eventsProcessed),
		"events_failed":    atomic.LoadInt64(&c.eventsFailed),
	}
}
