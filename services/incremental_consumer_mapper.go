package services

// ConsumerMapper 源→目标映射器
type ConsumerMapper struct {
	dbMapping    map[string]string // source_db → target_db
	tableMapping map[string]string // "source_db.source_table" → "target_table"
}

// NewConsumerMapper 创建映射器
func NewConsumerMapper(dbMapping, tableMapping map[string]string) *ConsumerMapper {
	return &ConsumerMapper{
		dbMapping:    dbMapping,
		tableMapping: tableMapping,
	}
}

// MapDatabase 映射数据库名
func (m *ConsumerMapper) MapDatabase(sourceDB string) string {
	if target, ok := m.dbMapping[sourceDB]; ok {
		return target
	}
	return sourceDB
}

// MapTable 映射表名
func (m *ConsumerMapper) MapTable(sourceDB, sourceTable string) (targetDB, targetTable string) {
	targetDB = m.MapDatabase(sourceDB)
	key := sourceDB + "." + sourceTable
	if target, ok := m.tableMapping[key]; ok {
		targetTable = target
	} else {
		targetTable = sourceTable
	}
	return
}

// BuildMapperFromConfig 从任务配置构建映射器
func BuildMapperFromConfig(config *TaskConfig) *ConsumerMapper {
	dbMapping := make(map[string]string)
	tableMapping := make(map[string]string)

	for _, db := range config.DatabaseMappings {
		dbMapping[db.SourceDB] = db.TargetDB
		for _, tbl := range db.Tables {
			key := db.SourceDB + "." + tbl.SourceTable
			tableMapping[key] = tbl.TargetTable
		}
	}

	return NewConsumerMapper(dbMapping, tableMapping)
}
