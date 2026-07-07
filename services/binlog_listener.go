package services

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

// BinlogListener Binlog 监听器
type BinlogListener struct {
	canal        *canal.Canal
	queues       []BinlogQueue
	filterTables sync.Map // database.table → bool
	eventBus     *EventBus
	taskID       string
	mu           sync.Mutex
	running      bool
}

// NewBinlogListener 创建 Binlog 监听器（支持多队列扇出）
func NewBinlogListener(queues []BinlogQueue, eventBus *EventBus, taskID string) *BinlogListener {
	return &BinlogListener{
		queues:   queues,
		eventBus: eventBus,
		taskID:   taskID,
	}
}

// SetFilterTables 设置过滤表列表
func (l *BinlogListener) SetFilterTables(tables []string) {
	l.filterTables.Range(func(key, value interface{}) bool {
		l.filterTables.Delete(key)
		return true
	})
	for _, t := range tables {
		l.filterTables.Store(t, true)
	}
}

// AddFilterTable 动态添加过滤表
func (l *BinlogListener) AddFilterTable(table string) {
	l.filterTables.Store(table, true)
}

// RemoveFilterTable 动态移除过滤表
func (l *BinlogListener) RemoveFilterTable(table string) {
	l.filterTables.Delete(table)
}

func (l *BinlogListener) isFiltered(database, table string) bool {
	key := fmt.Sprintf("%s.%s", database, table)
	_, ok := l.filterTables.Load(key)
	return ok
}

// Start 启动监听
func (l *BinlogListener) Start(ctx context.Context, cfg *canal.Config) error {
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return fmt.Errorf("监听器已在运行")
	}
	l.mu.Unlock()

	cfg.ServerID = 100
	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("创建 Canal 失败: %w", err)
	}
	l.canal = c
	c.SetEventHandler(l)

	l.mu.Lock()
	l.running = true
	l.mu.Unlock()

	go l.runWithReconnect(ctx)
	return nil
}

// StartFrom 从指定位置启动
func (l *BinlogListener) StartFrom(ctx context.Context, cfg *canal.Config, pos mysql.Position) error {
	return l.startCanal(ctx, func(c *canal.Canal) error {
		return c.RunFrom(pos)
	}, cfg)
}

// startCanal 通用 canal 启动 + 重连包装
func (l *BinlogListener) startCanal(ctx context.Context, runFn func(*canal.Canal) error, cfg *canal.Config) error {
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return fmt.Errorf("监听器已在运行")
	}
	l.mu.Unlock()

	cfg.ServerID = 100
	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("创建 Canal 失败: %w", err)
	}
	l.canal = c
	c.SetEventHandler(l)

	l.mu.Lock()
	l.running = true
	l.mu.Unlock()

	go l.runGenericWithReconnect(ctx, runFn)
	return nil
}

func (l *BinlogListener) runGenericWithReconnect(ctx context.Context, runFn func(*canal.Canal) error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := runFn(l.canal)
		if err != nil {
			log.Printf("[BinlogListener] canal 运行错误: %v, 5秒后重连", err)
			l.eventBus.Publish("task.log", map[string]interface{}{
				"task_id": l.taskID, "category": "binlog",
				"message": fmt.Sprintf("Binlog 连接断开: %v, 重连中...", err),
			})
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (l *BinlogListener) runWithReconnect(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := l.canal.Run()
		if err != nil {
			log.Printf("[BinlogListener] canal 运行错误: %v, 5秒后重连", err)
			l.eventBus.Publish("task.log", map[string]interface{}{
				"task_id": l.taskID, "category": "binlog",
				"message": fmt.Sprintf("Binlog 连接断开: %v, 重连中...", err),
			})
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (l *BinlogListener) runFromWithReconnect(ctx context.Context, pos mysql.Position) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := l.canal.RunFrom(pos)
		if err != nil {
			log.Printf("[BinlogListener] canal RunFrom 错误: %v, 5秒后重连", err)
			l.eventBus.Publish("task.log", map[string]interface{}{
				"task_id": l.taskID, "category": "binlog",
				"message": fmt.Sprintf("Binlog 连接断开: %v, 重连中...", err),
			})
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// Stop 停止监听
func (l *BinlogListener) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.running {
		l.canal.Close()
		l.running = false
	}
}

// GetPosition 获取当前 Binlog 位置
func (l *BinlogListener) GetPosition() mysql.Position {
	return l.canal.SyncedPosition()
}

// ---- canal.EventHandler 实现 ----

func (l *BinlogListener) OnRow(e *canal.RowsEvent) error {
	if !l.isFiltered(e.Table.Schema, e.Table.Name) {
		return nil
	}

	for _, row := range e.Rows {
		event := &BinlogEvent{
			Database: e.Table.Schema,
			Table:    e.Table.Name,
			Data:     rowToMap(e.Table, row),
		}

		switch e.Action {
		case canal.InsertAction:
			event.Type = "insert"
		case canal.UpdateAction:
			event.Type = "update"
			if len(e.Rows) >= 2 {
				event.OldData = rowToMap(e.Table, e.Rows[0])
			}
		case canal.DeleteAction:
			event.Type = "delete"
		default:
			continue
		}

		// 扇出到所有队列
		for _, q := range l.queues {
			if err := q.Push(event); err != nil {
				log.Printf("[BinlogListener] 推入队列失败: %v", err)
			}
		}
	}
	return nil
}

func (l *BinlogListener) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	l.eventBus.Publish("binlog.position", map[string]interface{}{
		"task_id": l.taskID, "file": pos.Name, "pos": pos.Pos,
	})
	return nil
}

func (l *BinlogListener) OnTableChanged(header *replication.EventHeader, schemaName string, table string) error {
	return nil
}

func (l *BinlogListener) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (l *BinlogListener) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	return nil
}

func (l *BinlogListener) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	return nil
}

func (l *BinlogListener) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	return nil
}

func (l *BinlogListener) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	return nil
}

func (l *BinlogListener) OnTableNotFound(header *replication.EventHeader, e *replication.RowsEvent) error {
	return nil
}

func (l *BinlogListener) String() string {
	return "BinlogListener"
}

// rowToMap 将 canal 行数据转为 map
func rowToMap(table *schema.Table, row []interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for i, col := range table.Columns {
		if i < len(row) {
			m[col.Name] = row[i]
		}
	}
	return m
}
