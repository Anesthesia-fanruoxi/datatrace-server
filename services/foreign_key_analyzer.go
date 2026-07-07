package services

// ForeignKeyInfo 外键关系
type ForeignKeyInfo struct {
	ChildTable  string // 子表（有外键的表）
	ParentTable string // 父表（被引用的表）
}

// TopologyResult 拓扑排序结果
type TopologyResult struct {
	OrderedTables     []string   // 排序后的表（父表在前）
	IndependentTables []string   // 无外键的独立表
	Cycles            [][]string // 检测到的环
}

// AnalyzeForeignKeys 对表进行拓扑排序（Kahn 算法）
func AnalyzeForeignKeys(tables []string, foreignKeys []ForeignKeyInfo) *TopologyResult {
	// 构建邻接表和入度
	inDegree := make(map[string]int)
	adj := make(map[string][]string) // parent → children
	tableSet := make(map[string]bool)

	for _, t := range tables {
		tableSet[t] = true
		inDegree[t] = 0
	}

	// 只保留涉及的表之间的外键
	for _, fk := range foreignKeys {
		if !tableSet[fk.ChildTable] || !tableSet[fk.ParentTable] {
			continue
		}
		if fk.ChildTable == fk.ParentTable {
			continue // 自引用跳过
		}
		adj[fk.ParentTable] = append(adj[fk.ParentTable], fk.ChildTable)
		inDegree[fk.ChildTable]++
	}

	// Kahn 算法
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

	result := &TopologyResult{}

	// 检测环：未进入 ordered 的表存在环
	orderedSet := make(map[string]bool)
	for _, t := range ordered {
		orderedSet[t] = true
	}

	var cycleTables []string
	for _, t := range tables {
		if !orderedSet[t] {
			cycleTables = append(cycleTables, t)
		}
	}

	if len(cycleTables) > 0 {
		result.Cycles = append(result.Cycles, cycleTables)
	}

	// 分离独立表（入度为 0 且无出边）
	hasRelation := make(map[string]bool)
	for _, fk := range foreignKeys {
		if tableSet[fk.ChildTable] && tableSet[fk.ParentTable] {
			hasRelation[fk.ChildTable] = true
			hasRelation[fk.ParentTable] = true
		}
	}

	for _, t := range ordered {
		if hasRelation[t] {
			result.OrderedTables = append(result.OrderedTables, t)
		} else {
			result.IndependentTables = append(result.IndependentTables, t)
		}
	}

	return result
}
