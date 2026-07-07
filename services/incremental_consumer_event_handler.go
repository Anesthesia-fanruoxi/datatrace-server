package services

import (
	"database/sql"
	"fmt"
	"strings"
)

// ConflictError 冲突错误（目标行不存在或已被修改）
type ConflictError struct {
	Event  string
	Table  string
	Detail string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("冲突检测: %s.%s - %s", e.Event, e.Table, e.Detail)
}

// EventSQLHandler 事件处理器（生成 SQL）
type EventSQLHandler struct {
	db *sql.DB
}

// NewEventSQLHandler 创建事件处理器
func NewEventSQLHandler(db *sql.DB) *EventSQLHandler {
	return &EventSQLHandler{db: db}
}

// HandleInsert 处理 INSERT 事件 → INSERT ... ON DUPLICATE KEY UPDATE
func (h *EventSQLHandler) HandleInsert(targetDB, targetTable string, data map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	columns := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))
	placeholders := make([]string, 0, len(data))
	updateClauses := make([]string, 0, len(data))

	for col, val := range data {
		columns = append(columns, fmt.Sprintf("`%s`", col))
		values = append(values, val)
		placeholders = append(placeholders, "?")
		updateClauses = append(updateClauses, fmt.Sprintf("`%s` = VALUES(`%s`)", col, col))
	}

	query := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		targetDB, targetTable,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","),
		strings.Join(updateClauses, ","))

	_, err := h.db.Exec(query, values...)
	return err
}

// HandleUpdate 处理 UPDATE 事件 → UPDATE ... WHERE old_data
func (h *EventSQLHandler) HandleUpdate(targetDB, targetTable string, data, oldData map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	setClauses := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data)+len(oldData))

	for col, val := range data {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col))
		values = append(values, val)
	}

	whereClauses := make([]string, 0, len(oldData))
	for col, val := range oldData {
		whereClauses = append(whereClauses, fmt.Sprintf("`%s` = ?", col))
		values = append(values, val)
	}

	query := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s",
		targetDB, targetTable,
		strings.Join(setClauses, " AND "),
		strings.Join(whereClauses, " AND "))

	result, err := h.db.Exec(query, values...)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return &ConflictError{Event: "update", Table: fmt.Sprintf("%s.%s", targetDB, targetTable), Detail: "目标行不存在"}
	}
	return nil
}

// HandleDelete 处理 DELETE 事件 → DELETE ... WHERE data
func (h *EventSQLHandler) HandleDelete(targetDB, targetTable string, data map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	whereClauses := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	for col, val := range data {
		whereClauses = append(whereClauses, fmt.Sprintf("`%s` = ?", col))
		values = append(values, val)
	}

	query := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s",
		targetDB, targetTable,
		strings.Join(whereClauses, " AND "))

	result, err := h.db.Exec(query, values...)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return &ConflictError{Event: "delete", Table: fmt.Sprintf("%s.%s", targetDB, targetTable), Detail: "目标行不存在"}
	}
	return nil
}

// ApplyEvent 根据事件类型分发处理
func (h *EventSQLHandler) ApplyEvent(event *BinlogEvent, targetDB, targetTable string) error {
	switch event.Type {
	case "insert":
		return h.HandleInsert(targetDB, targetTable, event.Data)
	case "update":
		return h.HandleUpdate(targetDB, targetTable, event.Data, event.OldData)
	case "delete":
		return h.HandleDelete(targetDB, targetTable, event.Data)
	default:
		return fmt.Errorf("未知事件类型: %s", event.Type)
	}
}
