package services

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// MySQLReader 分批读取器
type MySQLReader struct {
	db *sql.DB
}

// NewMySQLReader 创建读取器
func NewMySQLReader(db *sql.DB) *MySQLReader {
	return &MySQLReader{db: db}
}

// ColumnInfo 列信息
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Key      string
}

// GetColumns 获取表列信息
func (r *MySQLReader) GetColumns(ctx context.Context, database, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")
	rows, err := r.db.QueryContext(ctx, query, database, table)
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable, key string
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &key); err != nil {
			return nil, err
		}
		col.Nullable = nullable == "YES"
		col.Key = key
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// GetRowCount 获取表行数
func (r *MySQLReader) GetRowCount(ctx context.Context, database, table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", database, table)
	var count int64
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// ReadBatch 分批读取数据
func (r *MySQLReader) ReadBatch(ctx context.Context, database, table string, columns []string, offset, limit int) ([][]interface{}, error) {
	cols := make([]string, len(columns))
	for i, c := range columns {
		cols[i] = fmt.Sprintf("`%s`", c)
	}

	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY 1 LIMIT ? OFFSET ?",
		strings.Join(cols, ","), database, table)

	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("读取数据失败: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var result [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(colTypes))
		scanArgs := make([]interface{}, len(colTypes))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		result = append(result, values)
	}
	return result, rows.Err()
}

// GetCreateTableDDL 获取建表 DDL
func (r *MySQLReader) GetCreateTableDDL(ctx context.Context, database, table string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table)
	var name, ddl string
	err := r.db.QueryRowContext(ctx, query).Scan(&name, &ddl)
	if err != nil {
		return "", fmt.Errorf("获取DDL失败: %w", err)
	}
	return ddl, nil
}

// GetForeignKeys 获取表的外键关系
func (r *MySQLReader) GetForeignKeys(ctx context.Context, database string, tables []string) ([]ForeignKeyInfo, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(tables))
	args := make([]interface{}, 0, len(tables)+1)
	args = append(args, database)
	for i, t := range tables {
		placeholders[i] = "?"
		args = append(args, t)
	}

	query := fmt.Sprintf(`SELECT TABLE_NAME, REFERENCED_TABLE_NAME 
		FROM information_schema.KEY_COLUMN_USAGE 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN (%s) 
		AND REFERENCED_TABLE_NAME IS NOT NULL`,
		strings.Join(placeholders, ","))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("获取外键失败: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKeyInfo
	for rows.Next() {
		var fk ForeignKeyInfo
		if err := rows.Scan(&fk.ChildTable, &fk.ParentTable); err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// GetDatabases 获取数据库列表
func (r *MySQLReader) GetDatabases(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dbs []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	return dbs, rows.Err()
}

// EstimateBatchSize 根据表平均行大小动态计算批次行数
// targetBytes: 每批目标内存（默认16MB），maxBatch: 用户配置的上限
func (r *MySQLReader) EstimateBatchSize(ctx context.Context, database, table string, maxBatch int) int {
	if maxBatch <= 0 {
		maxBatch = 50000
	}

	var avgRowLen sql.NullInt64
	query := "SELECT AVG_ROW_LENGTH FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	err := r.db.QueryRowContext(ctx, query, database, table).Scan(&avgRowLen)
	if err != nil || !avgRowLen.Valid || avgRowLen.Int64 <= 0 {
		return maxBatch // 查不到则用上限
	}

	const targetBytes int64 = 16 * 1024 * 1024 // 16MB
	batch := int(targetBytes / avgRowLen.Int64)

	// 限制范围
	if batch < 500 {
		batch = 500
	}
	if batch > maxBatch {
		batch = maxBatch
	}
	return batch
}

// GetTables 获取指定数据库的表列表
func (r *MySQLReader) GetTables(ctx context.Context, database string) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf("SHOW TABLES FROM `%s`", database))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}
