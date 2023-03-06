package insert

import (
	"github.com/bigrainnn/db_ops_common/data_struct"
	"github.com/bigrainnn/db_ops_common/task_generator/uni_func"
)

type Mode int32

const (
	ModeCommon               Mode = 0
	ModeOmDuplicateKeyUpdate Mode = 1
	ModeReplace              Mode = 2
)

type TaskGenerator struct {
	Records   []data_struct.RecordForInsert
	TableName string
	Mode      Mode
	UniKeys   [][]string
}

func (h *TaskGenerator) CreateOnDuplicateKeySql(record data_struct.RecordForInsert) (string, []interface{}) {
	sql := " ON DUPLICATE KEY UPDATE "
	r := record.Record
	var valueSlice []interface{}
	for _, field := range r {
		sql += field[0] + "=?,"
		valueSlice = append(valueSlice, field[1])
	}

	return sql, valueSlice
}

func (h *TaskGenerator) CreateSqlRaw(r data_struct.RecordForInsert) (string, []interface{}) {
	var sql string
	if r.NeedReplace || h.Mode == ModeReplace {
		sql = "replace into " + h.TableName
	} else {
		sql = "insert into " + h.TableName
	}

	var valueSlice []interface{}
	keys := " ("
	values := "("
	for _, v := range r.Record {
		key := v[0]
		value := v[1]
		keys += key + ","
		values += "?,"
		valueSlice = append(valueSlice, value)
	}

	runeKeys := []rune(keys)
	runeValues := []rune(values)
	runeKeys[len(runeKeys)-1] = ')'
	runeValues[len(runeValues)-1] = ')'
	sql += string(runeKeys) + " values " + string(runeValues)

	if h.Mode == ModeOmDuplicateKeyUpdate {
		updateSql, updateSlice := h.CreateOnDuplicateKeySql(r)
		sql += updateSql
		valueSlice = append(valueSlice, updateSlice...)
		sql = uni_func.TruncateText(sql, ",")
	}

	return sql, valueSlice
}

func (h *TaskGenerator) CreateBatchSql() []data_struct.SqlWithImmediate {
	var res []data_struct.SqlWithImmediate
	for _, record := range h.Records {
		sql, immediate := h.CreateSqlRaw(record)
		sqlWithImmediate := data_struct.SqlWithImmediate{
			Sql:       sql,
			Immediate: immediate,
		}
		res = append(res, sqlWithImmediate)
	}
	return res
}

func (h *TaskGenerator) PrepareBatchInsertSql() []data_struct.SqlWithImmediate {
	var res []data_struct.SqlWithImmediate
	for i := 0; i < len(h.Records); i++ {
		for _, uniKey := range h.UniKeys {
			insertSql := h.PrepareInsertSql(uniKey, h.Records[i].Record)
			res = append(res, insertSql)
		}
	}
	return res
}

// PrepareInsertSql 插入预处理，针对enabled适配
func (h *TaskGenerator) PrepareInsertSql(uniKey []string, record data_struct.Record) data_struct.SqlWithImmediate {
	var res data_struct.SqlWithImmediate
	m := h.GetUniKeyMap(uniKey, record)
	sql := "select enabled from " + h.TableName + " where "
	var immediate []interface{}
	for k, v := range m {
		sql += k + "= ? and "
		immediate = append(immediate, v)
	}
	sql = uni_func.TruncateText(sql, "and ")

	res = data_struct.SqlWithImmediate{
		Sql:       sql,
		Immediate: immediate,
	}

	return res
}

func (h *TaskGenerator) GetUniKeyMap(uniKey []string, record data_struct.Record) map[string][]string {
	m := make(map[string][]string)
	for _, key := range uniKey {
		for _, field := range record {
			if field[0] == key {
				m[key] = append(m[key], field[1])
				break
			}
		}
	}
	return m
}
