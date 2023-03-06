package data_struct

import "database/sql"

type Field [2]string

type RecordForInsert struct {
	Record      Record
	NeedReplace bool
}

type Record []Field

type UpdatedRecord struct {
	UpdateField Record
	WhereField  Record
}

type DBConnectParam struct {
	DBName      string
	DBTableName string
	User        string
	Password    string
	Address     string
}

type ColumnSort struct {
	Name string
	Desc bool
}

type WhereStruct struct {
	Expression string
	Immediate  []interface{}
}

type SqlWithImmediate struct {
	Sql       string
	Immediate []interface{}
}

type QueryRecord struct {
	Pointers  []interface{}
	Container []sql.NullString
	Columns   []string
	Records   [][]sql.NullString
}

func (q *QueryRecord) AddRecord() {
	newRecord := make([]sql.NullString, len(q.Container))
	copy(newRecord, q.Container)
	q.Records = append(q.Records, newRecord)
}

func NewQueryRecord(col []string) *QueryRecord {
	p := make([]interface{}, len(col))
	c := make([]sql.NullString, len(col))
	for i := range p {
		p[i] = &c[i]
	}
	return &QueryRecord{
		Pointers:  p,
		Container: c,
		Columns:   col,
	}
}
