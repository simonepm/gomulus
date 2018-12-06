package gomulus

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gomulus"
	"strings"
)

// DefaultMysqlDestination ...
type DefaultMysqlDestination struct {
	Config   gomulus.DriverConfig
	DB       *sql.DB
	Database string
	Table    string
}

// New ...
func (d *DefaultMysqlDestination) New(config gomulus.DriverConfig) error {

	var err error
	var db *sql.DB
	var truncate, _ = config.Options["truncate"].(bool)
	var database, _ = config.Options["database"].(string)
	var endpoint, _ = config.Options["endpoint"].(string)
	var table, _ = config.Options["table"].(string)
	var tables = make([]string, 0)
	var rows *sql.Rows

	if db, err = sql.Open("mysql", endpoint); err != nil {
		return err
	}

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
		return fmt.Errorf("table not found `%s`.`%s`", database, table)
	}

	d.Table = table

	if truncate {
		if _, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", database, table)); err != nil {
			return err
		}
	}

	d.Database = database
	d.Table = table
	d.DB = db

	return nil

}

// GetTask ...
func (d *DefaultMysqlDestination) GetTask(data [][]interface{}) (gomulus.InsertionTask, error) {

	return gomulus.InsertionTask{
		Meta: map[string]interface{}{},
		Data: data,
	}, nil

}

// ProcessTask ...
func (d *DefaultMysqlDestination) ProcessTask(InsertionTask gomulus.InsertionTask) (int, error) {

	db := d.DB

	marks := ""
	for _, row := range InsertionTask.Data {
		for range row {
			marks += "?,"
		}
		break
	}

	marks = strings.TrimRight(marks, ",")

	query := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%s)", d.Database, d.Table, marks)

	tx, _ := db.Begin()

	stmt, err := tx.Prepare(query)

	if err != nil {
		return len(InsertionTask.Data), err
	}

	defer stmt.Close()

	for _, row := range InsertionTask.Data {

		if _, err = stmt.Exec(row...); err != nil {
			return len(InsertionTask.Data), err
		}

	}

	if err := tx.Commit(); err != nil {
		return len(InsertionTask.Data), err
	}

	return len(InsertionTask.Data), err

}

func inSlice(a string, list []string) bool {

	for _, b := range list {
		if b == a {
			return true
		}
	}

	return false

}
