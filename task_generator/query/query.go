package query

import (
	"github.com/bigrainnn/db_ops_common/data_struct"
	"github.com/bigrainnn/db_ops_common/task_generator/uni_func"
	"strconv"
)

type TaskGenerator struct {
	Limit       uint32
	Pos         uint32
	QueryField  []string
	Sort        []data_struct.ColumnSort
	WhereStruct data_struct.WhereStruct
	TableName   string
}

func (h *TaskGenerator) CreateQuery() string {
	// 不支持
	if len(h.QueryField) == 0 {
		return "*"
	}

	var res string
	for _, value := range h.QueryField {
		res += value + ","
	}

	// 自动过滤enabled == 0的记录
	res += "enabled,"
	runeRes := []rune(res)
	runeRes[len(runeRes)-1] = ' '
	return string(runeRes)
}

func (h *TaskGenerator) CreateOrderBy() string {
	if len(h.Sort) == 0 {
		return ""
	}

	orderBy := " order by "
	for _, value := range h.Sort {
		orderBy += value.Name
		if value.Desc {
			orderBy += " desc"
		}
		orderBy += ","
	}
	return uni_func.TruncateText(orderBy, ",")
}

func (h *TaskGenerator) CreateSqlRaw() (string, []interface{}) {
	queryColumn := h.CreateQuery()
	orderBy := h.CreateOrderBy()
	whereSql := h.CreateWhereSql()

	sql := "select " + queryColumn + " from " + h.TableName + whereSql + orderBy + " " + h.CreateLimit() + h.CreateOffset()
	return sql, h.WhereStruct.Immediate
}

func (h *TaskGenerator) CreateCountSqlRaw() (string, []interface{}) {
	whereSql := h.CreateWhereSql()

	sql := "select count(*) from " + h.TableName + whereSql
	return sql, h.WhereStruct.Immediate
}

func (h *TaskGenerator) CreateLimit() string {
	if h.Limit == 0 {
		return ""
	}
	return "limit " + strconv.Itoa(int(h.Limit))
}

func (h *TaskGenerator) CreateOffset() string {
	if h.Pos == 0 {
		return ""
	}
	return " offset " + strconv.Itoa(int(h.Pos))
}

func (h *TaskGenerator) CreateWhereSql() string {
	if h.WhereStruct.Expression == "" {
		return ""
	}

	return " where " + h.WhereStruct.Expression
}

func (h *TaskGenerator) CreateBatchSql() []data_struct.SqlWithImmediate {
	var res []data_struct.SqlWithImmediate
	sql, immediate := h.CreateSqlRaw()
	sqlWithImmediate := data_struct.SqlWithImmediate{
		Sql:       sql,
		Immediate: immediate,
	}
	res = append(res, sqlWithImmediate)

	return res
}
