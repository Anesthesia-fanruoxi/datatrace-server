package services

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

// SSEHub 统一 SSE 推送入口
type SSEHub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan []byte]struct{} // topic → clients
	cache       map[string][]byte                   // topic → 最新快照
}

// NewSSEHub 创建统一 SSE Hub
func NewSSEHub() *SSEHub {
	return &SSEHub{
		subscribers: make(map[string]map[chan []byte]struct{}),
		cache:       make(map[string][]byte),
	}
}

// Subscribe 订阅某 topic，返回 channel 接收数据
func (h *SSEHub) Subscribe(topic string) chan []byte {
	ch := make(chan []byte, 256)

	h.mu.Lock()
	if h.subscribers[topic] == nil {
		h.subscribers[topic] = make(map[chan []byte]struct{})
	}
	h.subscribers[topic][ch] = struct{}{}

	// 推送缓存快照（新连接立即获取最新状态）
	cached, hasCache := h.cache[topic]
	h.mu.Unlock()

	if hasCache {
		go func() {
			select {
			case ch <- cached:
			default:
			}
		}()
	}

	return ch
}

// Unsubscribe 取消订阅
func (h *SSEHub) Unsubscribe(topic string, ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if clients, ok := h.subscribers[topic]; ok {
		delete(clients, ch)
		if len(clients) == 0 {
			delete(h.subscribers, topic)
		}
	}
	close(ch)
}

// Publish 发布事件到某 topic
func (h *SSEHub) Publish(topic string, data []byte) {
	h.mu.Lock()
	h.cache[topic] = data // 更新快照
	clients := make([]chan []byte, 0, len(h.subscribers[topic]))
	for ch := range h.subscribers[topic] {
		clients = append(clients, ch)
	}
	h.mu.Unlock()

	for _, ch := range clients {
		go func(c chan []byte) {
			defer func() {
				if r := recover(); r != nil {
					// channel 已关闭，忽略
				}
			}()
			select {
			case c <- data:
			default:
				// channel 满了，丢弃（避免阻塞）
				log.Printf("[SSEHub] topic '%s' client channel full, dropping message", topic)
			}
		}(ch)
	}
}

// PublishJSON 发布 JSON 数据到某 topic
func (h *SSEHub) PublishJSON(topic string, event string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", event, string(payload))
	h.Publish(topic, []byte(msg))
	return nil
}

// Handler 返回 Gin SSE Handler，支持多 topic 订阅
// 请求格式:
//   - 完整: GET /sse?topics=task:abc:detail,task:abc:progress
//   - 简写: GET /sse?task_id=abc&topics=detail,progress,logs,table_status,step_status
func (h *SSEHub) Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		topicsParam := c.Query("topics")
		if topicsParam == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "topics parameter required"})
			return
		}

		// 支持简写格式: task_id=xxx&topics=logs,detail,...
		taskID := c.Query("task_id")
		var topics []string
		if taskID != "" {
			for _, t := range splitTopics(topicsParam) {
				topics = append(topics, "task:"+taskID+":"+t)
			}
		} else {
			topics = splitTopics(topicsParam)
		}
		if len(topics) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no valid topics"})
			return
		}

		// 设置 SSE headers
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Header().Set("X-Accel-Buffering", "no")
		c.Writer.Flush()

		// 订阅所有 topic
		channels := make(map[string]chan []byte)
		for _, topic := range topics {
			channels[topic] = h.Subscribe(topic)
		}

		// 等待客户端断开连
		clientGone := c.Request.Context().Done()

		defer func() {
			for topic, ch := range channels {
				h.Unsubscribe(topic, ch)
			}
		}()

		// 合并所有 channel 到一个 select
		for {
			select {
			case <-clientGone:
				return
			default:
				// 非阻塞检查各 channel
				for _, ch := range channels {
					select {
					case data := <-ch:
						c.Writer.Write(data)
						c.Writer.Flush()
					default:
					}
				}
				// 短暂等待，避免 CPU 空转
				// 使用 gin 的 context 来响应客户端断开
				select {
				case <-clientGone:
					return
				default:
				}
			}
		}
	}
}

// splitTopics 解析逗号分隔的 topic 列表
func splitTopics(param string) []string {
	var topics []string
	current := ""
	for _, ch := range param {
		if ch == ',' {
			t := trimSpace(current)
			if t != "" {
				topics = append(topics, t)
			}
			current = ""
		} else {
			current += string(ch)
		}
	}
	t := trimSpace(current)
	if t != "" {
		topics = append(topics, t)
	}
	return topics
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && s[start] == ' ' {
		start++
	}
	for end > start && s[end-1] == ' ' {
		end--
	}
	return s[start:end]
}
