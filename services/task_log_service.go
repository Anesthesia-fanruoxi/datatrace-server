package services

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// TaskLogEntry 单条任务日志
type TaskLogEntry struct {
	Time     string `json:"time"`
	Level    string `json:"level"`
	Category string `json:"category"`
	Message  string `json:"message"`
}

// TaskLogService 任务日志文件服务（按 task_id 分文件存储）
type TaskLogService struct {
	baseDir string
	mu      sync.Mutex
	files   map[string]*os.File // taskID → 文件句柄
}

// NewTaskLogService 创建任务日志服务
func NewTaskLogService(baseDir string) *TaskLogService {
	if baseDir == "" {
		baseDir = "logs/tasks"
	}
	os.MkdirAll(baseDir, 0755)
	return &TaskLogService{
		baseDir: baseDir,
		files:   make(map[string]*os.File),
	}
}

// AppendLog 追加一条日志到任务文件
func (s *TaskLogService) AppendLog(taskID, category, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.openFile(taskID)
	if err != nil {
		return
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	level := "INFO"
	if strings.Contains(strings.ToLower(category), "error") || strings.Contains(strings.ToLower(message), "❌") {
		level = "ERROR"
	} else if strings.Contains(strings.ToLower(category), "warn") || strings.Contains(strings.ToLower(message), "⚠") {
		level = "WARN"
	}

	line := fmt.Sprintf("%s\t%s\t%s\t%s\n", now, level, category, message)
	f.WriteString(line)
}

// GetLogs 读取任务的所有日志
func (s *TaskLogService) GetLogs(taskID string) []TaskLogEntry {
	s.mu.Lock()
	// 先 flush 确保最新
	if f, ok := s.files[taskID]; ok {
		f.Sync()
	}
	s.mu.Unlock()

	path := s.logPath(taskID)
	f, err := os.Open(path)
	if err != nil {
		return []TaskLogEntry{}
	}
	defer f.Close()

	var entries []TaskLogEntry
	scanner := bufio.NewScanner(f)
	// 增大 buffer 防止长行截断
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 4)
		if len(parts) == 4 {
			entries = append(entries, TaskLogEntry{
				Time:     parts[0],
				Level:    parts[1],
				Category: parts[2],
				Message:  parts[3],
			})
		}
	}
	return entries
}

// GetLogsSince 读取指定时间之后的日志
func (s *TaskLogService) GetLogsSince(taskID string, since time.Time) []TaskLogEntry {
	all := s.GetLogs(taskID)
	var result []TaskLogEntry
	sinceStr := since.Format("2006-01-02 15:04:05")
	for _, e := range all {
		if e.Time >= sinceStr {
			result = append(result, e)
		}
	}
	return result
}

// ClearLogs 清空任务日志（任务重启时调用）
func (s *TaskLogService) ClearLogs(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if f, ok := s.files[taskID]; ok {
		f.Close()
		delete(s.files, taskID)
	}

	path := s.logPath(taskID)
	os.Remove(path)
}

// Close 关闭所有文件句柄
func (s *TaskLogService) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, f := range s.files {
		f.Close()
		delete(s.files, id)
	}
}

func (s *TaskLogService) logPath(taskID string) string {
	return filepath.Join(s.baseDir, taskID+".log")
}

func (s *TaskLogService) openFile(taskID string) (*os.File, error) {
	if f, ok := s.files[taskID]; ok {
		return f, nil
	}

	path := s.logPath(taskID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	s.files[taskID] = f
	return f, nil
}
