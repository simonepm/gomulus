package gomulus

import (
	"database/sql"
	"errors"
	"fmt"
	"gomulus"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type DefaultMysqlDestination struct {
	Config   gomulus.DriverConfig
	DB       *sql.DB
	Database string
	Table    string
}

func (d *DefaultMysqlDestination) New(config map[string]interface{}) error {

	var err error
	var db *sql.DB
	var truncate, _ = config["truncate"].(bool)
	var database, _ = config["database"].(string)
	var endpoint, _ = config["host"].(string)
	var table, _ = config["table"].(string)
	var tables = make([]string, 0)
	var rows *sql.Rows

	if ok, _ := regexp.MatchString(`^[\p{L}_][\p{L}\p{N}@$#_]{0,127}$`, database); !ok {
		return errors.New(fmt.Sprintf("invalid database name `%s`", database))
	}

	if ok, _ := regexp.MatchString(`^[\p{L}_][\p{L}\p{N}@$#_]{0,127}$`, table); !ok {
		return errors.New(fmt.Sprintf("invalid table name `%s`", table))
	}

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

	if !InSliceString(table, tables) {
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

func (d *DefaultMysqlDestination) PreProcessData(data [][]interface{}) ([][]interface{}, error) {

	return data, nil

}

func (d *DefaultMysqlDestination) PersistData(data [][]interface{}) (int, error) {

	db := d.DB

	marks := ""
	for _, row := range data {
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
		return len(data), err
	}

	defer stmt.Close()

	for _, row := range data {

		if _, err = stmt.Exec(row...); err != nil {
			return len(data), err
		}

	}

	if err := tx.Commit(); err != nil {
		return len(data), err
	}

	return len(data), err

}

func InSliceString(a string, list []string) bool {

	for _, b := range list {
		if b == a {
			return true
		}
	}

	return false

}
