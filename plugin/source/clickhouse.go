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
	Limit   int
	Count   int
	Offset  int
	Table   string
	Columns string
}

// New ...
func (s *clickhouseSource) New(config gomulus.DriverConfig) error {

	var err error
	var db *sql.DB
	var count, _ = config.Options["count"].(float64)
	var offset, _ = config.Options["offset"].(float64)
	var endpoint, _ = config.Options["endpoint"].(string)
	var table, _ = config.Options["table"].(string)
	var limit, _ = config.Options["limit"].(float64)
	var columns, _ = config.Options["columns"].(string)
	var tables = make([]string, 0)
	var rows *sql.Rows

	if columns == "" {
		columns = "*"
	}

	if db, err = sql.Open("clickhouse", endpoint); err != nil {
		return err
	}

	s.DB = db

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

	if count == 0 {
		if err = db.QueryRow(fmt.Sprintf("SELECT COUNT(0) FROM %s", table)).Scan(&count); err != nil {
			return err
		}
	}

	s.Count = int(math.Max(1, count))
	s.Limit = int(math.Max(1, limit))
	s.Offset = int(math.Max(0, offset))
	s.Columns = columns

	return nil

}

// GetTasks ...
func (s *clickhouseSource) GetTasks() ([]gomulus.SelectionTask, error) {

	offset := s.Offset
	tasks := make([]gomulus.SelectionTask, 0)

	for true {

		if offset > s.Count-1 {
			break
		}

		query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d, %d", s.Columns, s.Table, offset, s.Limit)

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
