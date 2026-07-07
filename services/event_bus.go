package services

import (
	"log"
	"sync"
)

// EventHandler 事件处理函数类型
type EventHandler func(data interface{})

// EventBus 进程内事件发布订阅总线
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[string][]EventHandler
}

// NewEventBus 创建事件总线
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string][]EventHandler),
	}
}

// Subscribe 订阅某个事件类型
func (b *EventBus) Subscribe(eventType string, handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[eventType] = append(b.subscribers[eventType], handler)
}

// Publish 发布事件，异步通知所有订阅者
func (b *EventBus) Publish(eventType string, data interface{}) {
	b.mu.RLock()
	handlers := make([]EventHandler, len(b.subscribers[eventType]))
	copy(handlers, b.subscribers[eventType])
	b.mu.RUnlock()

	for _, handler := range handlers {
		go func(h EventHandler) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[EventBus] panic in handler for event '%s': %v", eventType, r)
				}
			}()
			h(data)
		}(handler)
	}
}
