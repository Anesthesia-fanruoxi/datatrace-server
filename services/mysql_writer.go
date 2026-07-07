package services

import (
	"context"
	"database/sql"
	"datatrace/common"
	"fmt"
	"strings"
)

// MySQLWriter 批量写入器
type MySQLWriter struct {
	db *sql.DB
}

// NewMySQLWriter 创建写入器
func NewMySQLWriter(db *sql.DB) *MySQLWriter {
	return &MySQLWriter{db: db}
}

// CreateTable 在目标库创建表（使用源表 DDL）
func (w *MySQLWriter) CreateTable(ctx context.Context, targetDB, targetTable, ddl string) error {
	// 替换源库名为目标库名
	ddl = strings.ReplaceAll(ddl, "`", "")
	exec := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` LIKE (%s)", targetDB, targetTable, ddl)
	// 简化：直接用原始 DDL 但替换库名
	exec = fmt.Sprintf("USE `%s`; %s", targetDB, ddl)
	_, err := w.db.ExecContext(ctx, exec)
	return err
}

// DropTable 删除目标表
func (w *MySQLWriter) DropTable(ctx context.Context, database, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", database, table))
	return err
}

// TruncateTable 清空目标表
func (w *MySQLWriter) TruncateTable(ctx context.Context, database, table string) error {
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", database, table))
	return err
}

// CreateDatabase 创建目标数据库
func (w *MySQLWriter) CreateDatabase(ctx context.Context, database string) error {
	common.LogInfo("【CreateDatabase】执行: CREATE DATABASE IF NOT EXISTS `%s`", database)
	_, err := w.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database))
	if err != nil {
		common.LogError("【CreateDatabase】❌ 失败: %v", err)
	} else {
		common.LogInfo("【CreateDatabase】✅ 成功: %s", database)
	}
	return err
}

// BatchInsert 批量插入数据
func (w *MySQLWriter) BatchInsert(ctx context.Context, database, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	cols := make([]string, len(columns))
	for i, c := range columns {
		cols[i] = fmt.Sprintf("`%s`", c)
	}

	// 单行占位符
	placeholders := "(" + strings.Repeat("?,", len(columns)-1) + "?)"

	// 分批拼接多值 INSERT（MySQL 限制 65535 占位符）
	maxRowsPerStmt := 65535 / len(columns)
	if maxRowsPerStmt < 1 {
		maxRowsPerStmt = 1
	}

	for start := 0; start < len(rows); start += maxRowsPerStmt {
		end := start + maxRowsPerStmt
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]

		var allValues []interface{}
		var allPlaceholders []string
		for _, row := range batch {
			allPlaceholders = append(allPlaceholders, placeholders)
			for _, v := range row {
				allValues = append(allValues, v)
			}
		}

		query := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s",
			database, table,
			strings.Join(cols, ","),
			strings.Join(allPlaceholders, ","))

		if _, err := w.db.ExecContext(ctx, query, allValues...); err != nil {
			return fmt.Errorf("批量插入失败 (offset=%d): %w", start, err)
		}
	}
	return nil
}

// ExecDDL 执行任意 DDL
func (w *MySQLWriter) ExecDDL(ctx context.Context, ddl string) error {
	_, err := w.db.ExecContext(ctx, ddl)
	return err
}

// TableExists 检查表是否存在
func (w *MySQLWriter) TableExists(ctx context.Context, database, table string) (bool, error) {
	query := "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	var count int
	err := w.db.QueryRowContext(ctx, query, database, table).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
