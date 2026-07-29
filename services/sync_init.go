package services

import (
	"context"
	"database/sql"
	"datatrace/common"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SyncInitializer 目标库初始化器
type SyncInitializer struct {
	writer    *MySQLWriter
	reader    *MySQLReader
	publishFn func(taskID, category, message string) // 日志回调
}

// NewSyncInitializer 创建初始化器
func NewSyncInitializer(reader *MySQLReader, writer *MySQLWriter) *SyncInitializer {
	return &SyncInitializer{reader: reader, writer: writer}
}

// SetPublishFn 设置日志发布回调
func (s *SyncInitializer) SetPublishFn(fn func(taskID, category, message string)) {
	s.publishFn = fn
}

func (s *SyncInitializer) publishLog(taskID, category, message string) {
	if s.publishFn != nil {
		s.publishFn(taskID, category, message)
	}
}

// InitTargetDatabase 初始化目标数据库
func (s *SyncInitializer) InitTargetDatabase(ctx context.Context, targetDB string) error {
	return s.writer.CreateDatabase(ctx, targetDB)
}

// InitTargetTable 根据策略初始化目标表
func (s *SyncInitializer) InitTargetTable(ctx context.Context, sourceDB, sourceTable, targetDB, targetTable string, strategy string, columns []string) error {
	common.LogDebug("【InitTable】开始初始化表: %s.%s -> %s.%s, 策略: %s, 字段数: %d", sourceDB, sourceTable, targetDB, targetTable, strategy, len(columns))

	// 确保连接在目标库上下文
	if err := s.writer.ExecDDL(ctx, fmt.Sprintf("USE `%s`", targetDB)); err != nil {
		common.LogError("【InitTable】❌ 切换目标库失败: %v", err)
		return fmt.Errorf("切换目标库失败: %w", err)
	}

	// 获取源表 DDL
	ddl, err := s.reader.GetCreateTableDDL(ctx, sourceDB, sourceTable)
	if err != nil {
		common.LogError("【InitTable】❌ 获取源表 DDL 失败: %v", err)
		return fmt.Errorf("获取源表DDL失败: %w", err)
	}

	// 替换库名和表名
	ddl = replaceTableRef(ddl, sourceDB, targetDB, sourceTable, targetTable)

	// 字段过滤：如果指定了字段列表，过滤 DDL
	if len(columns) > 0 {
		ddl = filterDDLColumns(ddl, columns)
		common.LogDebug("【InitTable】字段过滤: 保留 %d 个字段", len(columns))
	}

	// 检查目标表是否存在
	common.LogDebug("【InitTable】正在检查目标表是否存在: %s.%s", targetDB, targetTable)
	exists, err := s.writer.TableExists(ctx, targetDB, targetTable)
	if err != nil {
		common.LogError("【InitTable】❌ 检查目标表存在失败: %v", err)
		return fmt.Errorf("检查目标表存在失败: %w", err)
	}
	common.LogDebug("【InitTable】目标表 %s.%s 存在状态: %v", targetDB, targetTable, exists)

	switch strategy {
	case "drop":
		common.LogDebug("【InitTable】策略: drop, 表存在=%v", exists)
		if exists {
			common.LogDebug("【InitTable】正在删除目标表: %s.%s", targetDB, targetTable)
			if err := s.writer.DropTable(ctx, targetDB, targetTable); err != nil {
				common.LogError("【InitTable】❌ 删除目标表失败: %v", err)
				return fmt.Errorf("删除目标表失败: %w", err)
			}
			common.LogDebug("【InitTable】✅ 目标表已删除")
		}
		common.LogDebug("【InitTable】正在执行 DDL 创建表")
		if err := s.writer.ExecDDL(ctx, ddl); err != nil {
			common.LogError("【InitTable】❌ 创建表失败: %v", err)
			return err
		}
		common.LogDebug("【InitTable】✅ 表创建成功")

	case "truncate":
		common.LogDebug("【InitTable】策略: truncate, 表存在=%v", exists)
		if exists {
			common.LogDebug("【InitTable】正在清空目标表: %s.%s", targetDB, targetTable)
			if err := s.writer.TruncateTable(ctx, targetDB, targetTable); err != nil {
				common.LogError("【InitTable】❌ 清空目标表失败: %v", err)
				return err
			}
			common.LogDebug("【InitTable】✅ 目标表已清空")
		} else {
			common.LogDebug("【InitTable】正在执行 DDL 创建表")
			if err := s.writer.ExecDDL(ctx, ddl); err != nil {
				common.LogError("【InitTable】❌ 创建表失败: %v", err)
				return err
			}
			common.LogDebug("【InitTable】✅ 表创建成功")
		}

	case "append":
		common.LogDebug("【InitTable】策略: append, 表存在=%v", exists)
		if !exists {
			common.LogDebug("【InitTable】正在执行 DDL 创建表")
			if err := s.writer.ExecDDL(ctx, ddl); err != nil {
				common.LogError("【InitTable】❌ 创建表失败: %v", err)
				return err
			}
			common.LogDebug("【InitTable】✅ 表创建成功")
		} else {
			common.LogDebug("【InitTable】⚠️ 表已存在，追加模式跳过创建")
		}

	case "structure_only":
		common.LogDebug("【InitTable】策略: structure_only, 表存在=%v", exists)
		if exists {
			common.LogDebug("【InitTable】正在删除目标表: %s.%s", targetDB, targetTable)
			if err := s.writer.DropTable(ctx, targetDB, targetTable); err != nil {
				common.LogError("【InitTable】❌ 删除目标表失败: %v", err)
				return fmt.Errorf("删除目标表失败: %w", err)
			}
			common.LogDebug("【InitTable】✅ 目标表已删除")
		}
		common.LogDebug("【InitTable】正在执行 DDL 创建表（仅结构）")
		if err := s.writer.ExecDDL(ctx, ddl); err != nil {
			common.LogError("【InitTable】❌ 创建表失败: %v", err)
			return err
		}
		common.LogDebug("【InitTable】✅ 表结构创建成功")

	default:
		common.LogError("【InitTable】❌ 未知的表策略: %s", strategy)
		return fmt.Errorf("未知的表策略: %s", strategy)
	}

	common.LogDebug("【InitTable】✅ 表 %s.%s 初始化完成", targetDB, targetTable)
	return nil
}

// InitAllTargets 初始化目标数据库和表（内部自动解析 DDL 外键依赖并拓扑排序建表）
// mappings 为当前目标的库表映射（多目标时已由调用方结合 TaskConfig.GetEffectiveMappings 解析），config 仅用于读取 SyncConfig 策略
func (s *SyncInitializer) InitAllTargets(ctx context.Context, config *TaskConfig, mappings []DatabaseMapping, targetDSN string, taskID string) error {
	common.LogInfo("【SyncInit】开始初始化目标库表（%d 个库映射）", len(mappings))
	common.LogDebug("【SyncInit】目标 DSN: %s", targetDSN)

	// 保护性检查：mappings 为空意味着任务配置与运行时解析不一致（旧格式字段空、多目标取错 targetID 等），
	// 若直接空转会导致“没建库没建表但日志报完成”的假成功，必须直接报错
	if len(mappings) == 0 {
		common.LogError("【SyncInit】❌ 当前目标没有任何库表映射，可能是多目标配置中 target_id 不匹配或配置为空")
		s.publishLog(taskID, "initialize", "❌ 当前目标没有任何库表映射，任务中止")
		return fmt.Errorf("当前目标库表映射为空，无法初始化")
	}

	targetDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		common.LogError("【SyncInit】❌ 连接目标库失败: %v", err)
		return fmt.Errorf("连接目标库失败: %w", err)
	}
	defer targetDB.Close()

	// 真正建立连接（sql.Open 是惰性的，必须 Ping 才能验证连通性）
	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pingCancel()
	common.LogDebug("【SyncInit】正在 Ping 目标库...")
	if err := targetDB.PingContext(pingCtx); err != nil {
		common.LogError("【SyncInit】❌ 目标库 Ping 失败: %v", err)
		return fmt.Errorf("目标库连接失败（Ping 超时或错误）: %w", err)
	}
	common.LogDebug("【SyncInit】✅ 目标库 Ping 成功，连接正常")

	targetWriter := NewMySQLWriter(targetDB)

	// ─── 第一步：创建库 ───
	s.publishLog(taskID, "initialize", "━━ 第一步：创建目标库 ━━")
	for _, db := range mappings {
		// 带超时保护
		if _, diagErr := targetDB.ExecContext(ctx, "SELECT 1"); diagErr != nil {
			common.LogError("【SyncInit】❌ 目标库连接诊断失败（SELECT 1）: %v", diagErr)
			return fmt.Errorf("目标库连接不可用: %w", diagErr)
		}
		createCtx, createCancel := context.WithTimeout(ctx, 30*time.Second)
		createErr := targetWriter.CreateDatabase(createCtx, db.TargetDB)
		createCancel()
		if createErr != nil {
			common.LogError("【SyncInit】❌ 创建目标库 %s 失败: %v", db.TargetDB, createErr)
			s.publishLog(taskID, "initialize", fmt.Sprintf("创建目标库 %s 失败: %v", db.TargetDB, createErr))
			return fmt.Errorf("创建目标库 %s 失败: %w", db.TargetDB, createErr)
		}
		if db.TargetDB == db.SourceDB {
			s.publishLog(taskID, "initialize", fmt.Sprintf("创建目标库 %s 成功", db.TargetDB))
		} else {
			s.publishLog(taskID, "initialize", fmt.Sprintf("创建目标库 %s（源库名：%s）成功", db.TargetDB, db.SourceDB))
		}
	}

	// ─── 第二步：创建表（按拓扑序，父表先建）───
	s.publishLog(taskID, "initialize", "━━ 第二步：创建目标表 ━━")
	strategy := config.SyncConfig.TableExistsStrategy
	if strategy == "" {
		strategy = "truncate"
	}

	// 构建 tableKey(sourceDB.sourceTable) -> tblInfo 的查找表
	// 必须包含源库名，否则多个源库同名表（如 undo_log）会互相覆盖导致只建一份
	type tblInfo struct {
		SourceDB    string
		TargetDB    string
		SourceTable string
		TargetTable string
		Columns     []string
	}
	tblMap := make(map[string]tblInfo)
	var allTbls []string
	for _, db := range mappings {
		for _, tbl := range db.Tables {
			key := TableKey(db.SourceDB, tbl.SourceTable)
			tblMap[key] = tblInfo{db.SourceDB, db.TargetDB, tbl.SourceTable, tbl.TargetTable, tbl.Columns}
			allTbls = append(allTbls, key)
		}
	}

	// ─── 预获取所有 DDL 并解析外键依赖（并发：SHOW CREATE TABLE 无副作用）───
	ddlMap := make(map[string]string) // tableKey -> processed DDL
	var ddlDeps []ddlDep              // 外键依赖关系（tableKey 级别）
	{
		type ddlResult struct {
			key       string
			processed string
			refs      []string
		}
		const ddlWorkers = 10
		sem := make(chan struct{}, ddlWorkers)
		resultCh := make(chan ddlResult, len(allTbls))
		errCh := make(chan error, len(allTbls))
		var wg sync.WaitGroup
		for _, k := range allTbls {
			info := tblMap[k]
			wg.Add(1)
			sem <- struct{}{}
			go func(key string, info tblInfo) {
				defer wg.Done()
				defer func() { <-sem }()
				rawDDL, err := s.reader.GetCreateTableDDL(ctx, info.SourceDB, info.SourceTable)
				if err != nil {
					errCh <- fmt.Errorf("获取表 %s DDL 失败: %w", key, err)
					return
				}
				processedDDL := replaceTableRef(rawDDL, info.SourceDB, info.TargetDB, info.SourceTable, info.TargetTable)
				if len(info.Columns) > 0 {
					processedDDL = filterDDLColumns(processedDDL, info.Columns)
				}
				resultCh <- ddlResult{key: key, processed: processedDDL, refs: extractDDLReferences(processedDDL)}
			}(k, info)
		}
		wg.Wait()
		close(resultCh)
		close(errCh)
		if err, ok := <-errCh; ok {
			common.LogError("【SyncInit】❌ %v", err)
			s.publishLog(taskID, "initialize", fmt.Sprintf("❌ 预获取表结构失败: %v", err))
			return err
		}
		for r := range resultCh {
			ddlMap[r.key] = r.processed
			info := tblMap[r.key]
			for _, ref := range r.refs {
				parentKey := TableKey(info.SourceDB, ref)
				ddlDeps = append(ddlDeps, ddlDep{child: r.key, parent: parentKey})
			}
		}
	}

	// ─── 拓扑排序建表顺序 ───
	buildOrder := topoSortDDL(allTbls, ddlDeps)
	if len(buildOrder) < len(allTbls) {
		common.LogWarn("【SyncInit】⚠️ 拓扑排序检测到循环依赖，部分表将按原始顺序建表")
	}

	// DROP/structure_only 策略：按逆拓扑序删表（子表先删，父表后删）
	if strategy == "drop" || strategy == "structure_only" {
		for i := len(buildOrder) - 1; i >= 0; i-- {
			key := buildOrder[i]
			info, ok := tblMap[key]
			if !ok {
				continue
			}
			if err := s.writer.DropTable(ctx, info.TargetDB, info.TargetTable); err != nil {
				common.LogError("【SyncInit】❌ 删除表 %s.%s 失败: %v", info.TargetDB, info.TargetTable, err)
				return fmt.Errorf("删除表 %s 失败: %w", info.TargetTable, err)
			}
		}
	}

	for _, key := range buildOrder {
		info, ok := tblMap[key]
		if !ok {
			continue
		}
		ddl := ddlMap[key]
		common.LogDebug("【SyncInit】  处理表: %s.%s -> %s.%s", info.SourceDB, info.SourceTable, info.TargetDB, info.TargetTable)

		if err := s.execCreateTable(ctx, info.TargetDB, info.TargetTable, strategy, ddl); err != nil {
			common.LogError("【SyncInit】❌ 初始化表 %s.%s 失败: %v", info.TargetDB, info.TargetTable, err)
			s.publishLog(taskID, "initialize", fmt.Sprintf("创建表 %s 失败: %v", info.TargetTable, err))
			return err
		}
		if info.TargetTable == info.SourceTable {
			s.publishLog(taskID, "initialize", fmt.Sprintf("创建表 %s.%s 成功", info.TargetDB, info.TargetTable))
		} else {
			s.publishLog(taskID, "initialize", fmt.Sprintf("创建表 %s.%s（源表名：%s）成功", info.TargetDB, info.TargetTable, info.SourceTable))
		}
	}

	s.publishLog(taskID, "initialize", "库表结构初始化完成")
	common.LogInfo("【SyncInit】✅✅✅ 所有目标数据库和表初始化完成！")
	return nil
}

// replaceTableRef 替换 DDL 中的库名和表名引用
func replaceTableRef(ddl, sourceDB, targetDB, sourceTable, targetTable string) string {
	result := ddl
	// 替换反引号包裹的库名
	result = replaceIdentifier(result, sourceDB, targetDB)
	// 如果表名不同则替换
	if sourceTable != targetTable {
		result = replaceIdentifier(result, sourceTable, targetTable)
	}
	return result
}

// replaceIdentifier 替换 SQL 中的标识符（反引号包裹的）
func replaceIdentifier(sql, old, new string) string {
	oldQuoted := fmt.Sprintf("`%s`", old)
	newQuoted := fmt.Sprintf("`%s`", new)
	result := ""
	for i := 0; i < len(sql); i++ {
		if i+len(oldQuoted) <= len(sql) && sql[i:i+len(oldQuoted)] == oldQuoted {
			result += newQuoted
			i += len(oldQuoted) - 1
		} else {
			result += string(sql[i])
		}
	}
	return result
}

// filterDDLColumns 过滤 CREATE TABLE DDL，只保留指定字段
func filterDDLColumns(ddl string, columns []string) string {
	// 查找括号范围
	openIdx := strings.Index(ddl, "(")
	if openIdx < 0 {
		return ddl
	}
	closeIdx := strings.LastIndex(ddl, ")")
	if closeIdx <= openIdx {
		return ddl
	}

	// 构建保留字段集合
	keepCols := make(map[string]bool)
	for _, col := range columns {
		keepCols[col] = true
	}

	// 解析括号内的行
	body := ddl[openIdx+1 : closeIdx]
	lines := strings.Split(body, "\n")

	var kept []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimRight(trimmed, ",")
		if trimmed == "" {
			continue
		}

		// 索引/约束行：检查引用的字段是否都在保留列表中
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.HasPrefix(upperTrimmed, "PRIMARY KEY") ||
			strings.HasPrefix(upperTrimmed, "UNIQUE KEY") ||
			strings.HasPrefix(upperTrimmed, "UNIQUE INDEX") ||
			strings.HasPrefix(upperTrimmed, "KEY ") ||
			strings.HasPrefix(upperTrimmed, "INDEX ") ||
			strings.HasPrefix(upperTrimmed, "CONSTRAINT ") ||
			strings.HasPrefix(upperTrimmed, "CHECK ") {
			// 提取索引引用的所有字段名，检查是否都在保留列表中
			if indexColumnsExist(trimmed, keepCols) {
				kept = append(kept, line)
			} else {
				common.LogDebug("【filterDDL】跳过索引（引用了未选字段）: %s", strings.TrimSpace(line))
			}
			continue
		}

		// 字段行：提取反引号内的字段名
		colName := extractColumnName(trimmed)
		if colName != "" && keepCols[colName] {
			kept = append(kept, line)
		}
	}

	if len(kept) == 0 {
		return ddl
	}

	// 重建 DDL
	prefix := ddl[:openIdx+1]
	suffix := ddl[closeIdx:]

	// 修正逗号：移除最后一行的尾逗号
	for i := len(kept) - 1; i >= 0; i-- {
		kept[i] = strings.TrimRight(kept[i], ",")
		if i < len(kept)-1 {
			kept[i] += ","
		}
	}

	return prefix + "\n" + strings.Join(kept, "\n") + "\n" + suffix
}

// extractColumnName 从 DDL 字段定义行提取字段名
func extractColumnName(line string) string {
	if len(line) == 0 || line[0] != '`' {
		return ""
	}
	endIdx := strings.Index(line[1:], "`")
	if endIdx < 0 {
		return ""
	}
	return line[1 : endIdx+1]
}

// indexColumnsExist 检查索引行中引用的所有字段是否都在 keepCols 中
func indexColumnsExist(indexLine string, keepCols map[string]bool) bool {
	// 只检查括号内的字段名（括号外的是索引名，跳过）
	parenStart := strings.Index(indexLine, "(")
	if parenStart < 0 {
		return true // 无括号，保守保留
	}
	parenPart := indexLine[parenStart:]

	inBacktick := false
	var current strings.Builder
	for _, ch := range parenPart {
		if ch == '`' {
			if inBacktick {
				colName := current.String()
				if !keepCols[colName] {
					return false
				}
				current.Reset()
			}
			inBacktick = !inBacktick
		} else if inBacktick {
			current.WriteRune(ch)
		}
	}
	return true
}

// execCreateTable 使用已处理的 DDL 按策略建表
func (s *SyncInitializer) execCreateTable(ctx context.Context, targetDB, targetTable, strategy, ddl string) error {
	if err := s.writer.ExecDDL(ctx, fmt.Sprintf("USE `%s`", targetDB)); err != nil {
		return fmt.Errorf("切换目标库失败: %w", err)
	}
	exists, err := s.writer.TableExists(ctx, targetDB, targetTable)
	if err != nil {
		return fmt.Errorf("检查表存在失败: %w", err)
	}
	switch strategy {
	case "drop":
		if exists {
			if err := s.writer.DropTable(ctx, targetDB, targetTable); err != nil {
				return fmt.Errorf("删除表失败: %w", err)
			}
		}
		return s.writer.ExecDDL(ctx, ddl)
	case "truncate":
		if exists {
			return s.writer.TruncateTable(ctx, targetDB, targetTable)
		}
		return s.writer.ExecDDL(ctx, ddl)
	case "append":
		if !exists {
			return s.writer.ExecDDL(ctx, ddl)
		}
		return nil
	case "structure_only":
		if exists {
			if err := s.writer.DropTable(ctx, targetDB, targetTable); err != nil {
				return fmt.Errorf("删除表失败: %w", err)
			}
		}
		return s.writer.ExecDDL(ctx, ddl)
	default:
		return fmt.Errorf("未知策略: %s", strategy)
	}
}

// ddlDep DDL 外键依赖关系（child 引用了 parent）
type ddlDep struct {
	child  string
	parent string
}

// extractDDLReferences 从 DDL 中提取 REFERENCES 引用的表名
func extractDDLReferences(ddl string) []string {
	var refs []string
	upper := strings.ToUpper(ddl)
	searchFrom := 0
	for {
		idx := strings.Index(upper[searchFrom:], "REFERENCES")
		if idx < 0 {
			break
		}
		pos := searchFrom + idx + len("REFERENCES")
		// 跳过空白，提取反引号内的表名
		rest := ddl[pos:]
		rest = strings.TrimLeft(rest, " \t\n\r")
		if len(rest) > 0 && rest[0] == '`' {
			endIdx := strings.Index(rest[1:], "`")
			if endIdx >= 0 {
				refTable := rest[1 : endIdx+1]
				refs = append(refs, refTable)
			}
		}
		searchFrom = pos
	}
	return refs
}

// topoSortDDL 拓扑排序建表顺序（Kahn 算法）
func topoSortDDL(tables []string, deps []ddlDep) []string {
	tableSet := make(map[string]bool)
	inDegree := make(map[string]int)
	adj := make(map[string][]string) // parent → children

	for _, t := range tables {
		tableSet[t] = true
		inDegree[t] = 0
	}

	for _, d := range deps {
		if !tableSet[d.child] || !tableSet[d.parent] {
			continue
		}
		if d.child == d.parent {
			continue
		}
		adj[d.parent] = append(adj[d.parent], d.child)
		inDegree[d.child]++
	}

	var queue []string
	for _, t := range tables {
		if inDegree[t] == 0 {
			queue = append(queue, t)
		}
	}

	var ordered []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		ordered = append(ordered, node)
		for _, child := range adj[node] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	// 环中的表补充到末尾
	orderedSet := make(map[string]bool)
	for _, t := range ordered {
		orderedSet[t] = true
	}
	for _, t := range tables {
		if !orderedSet[t] {
			ordered = append(ordered, t)
		}
	}
	return ordered
}
