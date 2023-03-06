package task_processor

import (
	"context"
	"database/sql"
	"errors"
	"github.com/bigrainnn/db_ops_common/data_struct"
	"github.com/bigrainnn/db_ops_common/task_generator/insert"
	"github.com/bigrainnn/db_ops_common/task_generator/query"
	"gorm.io/gorm"
	"strconv"
)

type TaskGenerator interface {
	CreateBatchSql() []data_struct.SqlWithImmediate
}

type Processor struct {
	Task       TaskGenerator
	C          context.Context
	DB         *gorm.DB
	BusinessId string
}

func (h *Processor) InsertPrepareProcess(c *insert.TaskGenerator) error {
	insertQuerySql := c.PrepareBatchInsertSql()
	for index, insertQuerySql := range insertQuerySql {
		rows, err := h.DB.Raw(insertQuerySql.Sql, insertQuerySql.Immediate...).Rows()
		if err != nil {
			return err
		}

		queryRecord, err := h.CollectRes(rows)
		if err != nil {
			return err
		}

		// 插入预处理，有记录enable==1，报错
		records := queryRecord.Records
		for _, record := range records {
			if record[0].String == "1" {
				return errors.New("DUPLICATE KEY ERROR, sql: " + insertQuerySql.Sql)
			} else if record[0].String == "0" {
				c.Records[index].NeedReplace = true
			}
		}
	}
	return nil
}

// ProcessTx 事务处理封装
func (h *Processor) ProcessTx() error {
	c, ok := h.Task.(*insert.TaskGenerator)
	// 适配，存在记录且enabled==1的时候报错
	if ok && c.Mode != insert.ModeReplace && c.Mode != insert.ModeOmDuplicateKeyUpdate {
		err := h.InsertPrepareProcess(c)
		if err != nil {
			return err
		}
	}

	tx := h.DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	for _, record := range h.Task.CreateBatchSql() {
		result := tx.Exec(record.Sql, record.Immediate...)
		if result.Error != nil {
			tx.Rollback()
			return result.Error
		}
		if result.RowsAffected == 0 {
			tx.Rollback()
			return errors.New("affected rows is zero")
		}
	}
	result := tx.Commit()
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// ProcessQuery 查询处理，支持分页，无事务，仅支持单条查询
func (h *Processor) ProcessQuery() (*data_struct.QueryRecord, int, error) {
	c, _ := h.Task.(*query.TaskGenerator)
	countSql, immediate := c.CreateCountSqlRaw()
	rows, err := h.DB.Raw(countSql, immediate...).Rows()
	if err != nil {
		return nil, 0, err
	}

	records, err := h.CollectRes(rows)
	if err != nil {
		return nil, 0, err
	}
	numsString := records.Records[0][0].String
	nums, _ := strconv.Atoi(numsString)

	batchSql := c.CreateBatchSql()
	if len(batchSql) != 1 {
		return nil, 0, errors.New("illegal length: " + string(rune(len(batchSql))))
	}

	rows, err = h.DB.Raw(batchSql[0].Sql, batchSql[0].Immediate...).Rows()
	if err != nil {
		return nil, 0, err
	}

	records, err = h.CollectRes(rows)
	if err != nil {
		return nil, 0, err
	}

	return records, nums, nil
}

func (h *Processor) CollectRes(rows *sql.Rows) (*data_struct.QueryRecord, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	record := data_struct.NewQueryRecord(columns)
	for rows.Next() {
		err = rows.Scan(record.Pointers...)
		if err != nil {
			return nil, err
		}
		record.AddRecord()
	}

	return record, nil
}
