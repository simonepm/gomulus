package main

import (
	"database/sql"
	"fmt"
	"math"
	"gomulus"
	_ "github.com/kshvakov/clickhouse"
)

// ClickhouseSource ...
var ClickhouseSource clickhouseSource

type clickhouseSource struct {
	Config gomulus.DriverConfig
	DB     *sql.DB
	Limit  int
	Count  int
	Table  string
}

// New ...
func (s *clickhouseSource) New(config gomulus.DriverConfig) error {

	var err error
	var db *sql.DB
	var count = 0
	var limit = config.Options["limit"].(float64)
	var table = config.Options["table"].(string)
	var endpoint, _ = config.Options["endpoint"].(string) // tcp://%s:%d?username=%s&password=%s&database=%s&read_timeout=%d&write_timeout=%d
	var rowLimit = int(math.Max(1, limit))

	if db, err = sql.Open("clickhouse", endpoint); err != nil {
		return err
	}

	s.DB = db

	var tables = make([]string, 0)
	var rows *sql.Rows

	if rows, err = db.Query("SHOW TABLES"); err != nil {
		return err
	}

	for rows.Next() {
		t := ""
		err := rows.Scan(&t)
		if err != nil {
			return err
		}
		tables = append(tables, t)
	}

	if !inSlice(table, tables) {
		return fmt.Errorf("table not found `%s`", table)
	}

	s.Table = table

	if err = db.QueryRow(fmt.Sprintf("SELECT COUNT(0) FROM %s", table)).Scan(&count); err != nil {
		return err
	}

	s.Count = count
	s.Limit = rowLimit

	return nil

}

// GetTasks ...
func (s *clickhouseSource) GetTasks() ([]gomulus.SelectionTask, error) {

	offset := 0
	tasks := make([]gomulus.SelectionTask, 0)

	for true {

		if offset > s.Count-1 {
			break
		}

		query := fmt.Sprintf("SELECT * FROM %s LIMIT %d, %d", s.Table, offset, s.Limit)

		offset += s.Limit

		tasks = append(tasks, gomulus.SelectionTask{
			Meta: map[string]interface{}{
				"query": query,
			},
		})

	}

	return tasks, nil

}

// ProcessTask ...
func (s *clickhouseSource) ProcessTask(SelectionTask gomulus.SelectionTask) ([][]interface{}, error) {

	var db = s.DB
	var query, _ = SelectionTask.Meta["query"].(string)

	return Select(db, query)

}

// Select ...
func Select(db *sql.DB, query string) ([][]interface{}, error) {

	slices := make([][]interface{}, 0)

	rows, err := db.Query(query)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, _ := rows.Columns()

	for rows.Next() {

		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))

		for i := range columns {
			pointers[i] = &values[i]
		}

		rows.Scan(pointers...)

		err = rows.Err()

		if err != nil {
			return nil, err
		}

		slices = append(slices, values)

	}

	return slices, nil

}

func inSlice(a string, list []string) bool {

	for _, b := range list {
		if b == a {
			return true
		}
	}

	return false

}
