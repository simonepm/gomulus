package gomulus

import (
	"database/sql"
	"fmt"
	"os"
	"gomulus"
	"strings"
)

// DefaultMysqlDestination ...
type DefaultMysqlDestination struct {
	Config gomulus.DriverConfig
	DB     *sql.DB
	Table  string
}

// New ...
func (d *DefaultMysqlDestination) New(config gomulus.DriverConfig) error {

	var err error
	var db *sql.DB
	var truncate, _ = config.Options["truncate"].(bool)
	var endpoint, _ = config.Options["endpoint"].(string)
	var table, _ = config.Options["table"].(string)

	if db, err = sql.Open("mysql", endpoint); err != nil {
		return err
	}

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

	if !InSliceString(table, tables) {
		return fmt.Errorf("table not found `%s`", table)
	}

	d.Table = table

	if truncate {
		fmt.Fprintln(os.Stdout, "truncating table", table, "...")
		if _, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
			return err
		}
	}

	d.DB = db

	return nil

}

// GetTask ...
func (d *DefaultMysqlDestination) GetTask(data [][]interface{}) (gomulus.InsertionTask, error) {

	query := fmt.Sprintf("INSERT INTO %s VALUES ", d.Table)

	for _, row := range data {
		query += "("
		for range row {
			query += "?,"
		}
		query = strings.TrimRight(query, ",")
		query += "),"
	}
	query = strings.TrimRight(query, ",")

	return gomulus.InsertionTask{
		Meta: map[string]interface{}{
			"query": query,
		},
		Data: data,
	}, nil

}

// ProcessTask ...
func (d *DefaultMysqlDestination) ProcessTask(InsertionTask gomulus.InsertionTask) (int, error) {

	db := d.DB

	query, _ := InsertionTask.Meta["query"].(string)

	vals := []interface{}{}

	stmt, err := db.Prepare(query)

	if err != nil {
		return len(InsertionTask.Data), err
	}

	defer stmt.Close()

	for _, row := range InsertionTask.Data {
		vals = append(vals, row...)
	}

	_, err = stmt.Exec(vals...)

	if err != nil {
		return len(InsertionTask.Data), err
	}

	return len(InsertionTask.Data), err

}

// InSliceString ...
func InSliceString(a string, list []string) bool {

	for _, b := range list {
		if b == a {
			return true
		}
	}

	return false

}
