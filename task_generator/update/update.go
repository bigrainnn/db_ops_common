package update

import (
	"github.com/bigrainnn/db_ops_common/data_struct"
	"github.com/bigrainnn/db_ops_common/task_generator/uni_func"
)

type Handler struct {
	UpdateFields []data_struct.UpdatedRecord
	TableName    string
}

func (h *Handler) CreateSqlRaw(updateFields data_struct.Record, uniqueFields data_struct.Record) (string, []interface{}) {
	sql := "update " + h.TableName + " set"
	var valueSlice []interface{}
	updateSql := " "
	whereSql := " "
	for _, v := range updateFields {
		key := v[0]
		value := v[1]
		updateSql += key + "=?,"
		valueSlice = append(valueSlice, value)
	}
	for _, v := range uniqueFields {
		key := v[0]
		value := v[1]
		whereSql += key + "=? AND "
		valueSlice = append(valueSlice, value)
	}

	sql += uni_func.TruncateText(updateSql, ",") + " where" + uni_func.TruncateText(whereSql, "AND")
	return sql, valueSlice
}

func (h *Handler) CreateBatchSql() []data_struct.SqlWithImmediate {
	var res []data_struct.SqlWithImmediate
	for _, value := range h.UpdateFields {
		sql, immediate := h.CreateSqlRaw(value.UpdateField, value.WhereField)
		sqlWithImmediate := data_struct.SqlWithImmediate{
			Sql:       sql,
			Immediate: immediate,
		}
		res = append(res, sqlWithImmediate)
	}
	return res
}
