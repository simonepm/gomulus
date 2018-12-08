package main

import (
	"database/sql"
	"errors"
	"fmt"
	"gomulus"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/kshvakov/clickhouse"
)

var ClickhouseDestination clickhouseDestination

type clickhouseDestination struct {
	Config   gomulus.DriverConfig
	DB       *sql.DB
	Database string
	Table    string
	Columns  []interface{}
}

func (d *clickhouseDestination) New(config map[string]interface{}) error {

	var err error
	var con *sql.DB
	var truncate, _ = config["truncate"].(bool)
	var database, _ = config["database"].(string)
	var endpoint, _ = config["endpoint"].(string)
	var table, _ = config["table"].(string)
	var create, _ = config["create"].(map[string]interface{})
	var columns, _ = ddl["columns"].([]interface{}) // []map[string]string
	var engine, _ = ddl["engine"].(string)
	var tables = make([]string, 0)
	var rows *sql.Rows

	if ok, _ := regexp.MatchString(`^[\p{L}_][\p{L}\p{N}@$#_]{0,127}$`, database); !ok {
		return errors.New(fmt.Sprintf("invalid database name `%s`", database))
	}

	if ok, _ := regexp.MatchString(`^[\p{L}_][\p{L}\p{N}@$#_]{0,127}$`, table); !ok {
		return errors.New(fmt.Sprintf("invalid table name `%s`", table))
	}

	if con, err = sql.Open("clickhouse", endpoint); err != nil {
		return err
	}

	if create != nil {
		if err = createTable(con, database, table, columns, engine); err != nil {
			return err
		}
	}

	if rows, err = con.Query("SHOW TABLES"); err != nil {
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

	if truncate {

		if err = truncateTable(con, database, table); err != nil {
			return err
		}

	}

	d.Columns = columns
	d.Database = database
	d.Table = table
	d.DB = con

	return nil

}

func (d *clickhouseDestination) PreProcessData(data [][]interface{}) ([][]interface{}, error) {

	return data, nil

}

func (d *clickhouseDestination) PersistData(data [][]interface{}) (int, error) {

	con := d.DB

	marks := ""
	for _, row := range data {
		for range row {
			marks += "?,"
		}
		break
	}

	marks = strings.TrimRight(marks, ",")

	query := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%s)", d.Database, d.Table, marks)

	tx, _ := con.Begin()

	stmt, err := tx.Prepare(query)

	if err != nil {
		return len(data), err
	}

	defer stmt.Close()

	for _, row := range data {

		parsedRow := make([]interface{}, 0, len(row))

		for i, column := range d.Columns {

			col, _ := column.(map[string]interface{})
			keys := make([]interface{}, 0, len(col))
			for k := range col {
				keys = append(keys, k)
			}

			columnName, _ := keys[0].(string)
			columnType := col[columnName]
			columnBytes, _ := row[i].([]byte)
			columnString := string(columnBytes)

			switch columnType {
			case "UInt8":
				if v, err := strconv.ParseInt(columnString, 10, 8); err == nil {
					parsedRow = append(parsedRow, uint8(v))
				} else {
					parsedRow = append(parsedRow, uint8(0))
				}
			case "Boolean":
				switch columnString {
				case "1":
					parsedRow = append(parsedRow, uint8(1))
				case "true":
					parsedRow = append(parsedRow, uint8(1))
				case "on":
					parsedRow = append(parsedRow, uint8(1))
				default:
					parsedRow = append(parsedRow, 0)
				}
			case "UInt16":
				if v, err := strconv.ParseInt(columnString, 10, 16); err == nil {
					parsedRow = append(parsedRow, uint16(v))
				} else {
					parsedRow = append(parsedRow, uint16(0))
				}
			case "UInt32":
				if v, err := strconv.ParseInt(columnString, 10, 32); err == nil {
					parsedRow = append(parsedRow, uint32(v))
				} else {
					parsedRow = append(parsedRow, uint32(0))
				}
			case "UInt64":
				if v, err := strconv.ParseInt(columnString, 10, 64); err == nil {
					parsedRow = append(parsedRow, uint64(v))
				} else {
					parsedRow = append(parsedRow, uint64(0))
				}
			case "Int8":
				if v, err := strconv.ParseInt(columnString, 10, 8); err == nil {
					parsedRow = append(parsedRow, int8(v))
				} else {
					parsedRow = append(parsedRow, int8(0))
				}
			case "Int16":
				if v, err := strconv.ParseInt(columnString, 10, 16); err == nil {
					parsedRow = append(parsedRow, int16(v))
				} else {
					parsedRow = append(parsedRow, int16(0))
				}
			case "Int32":
				if v, err := strconv.ParseInt(columnString, 10, 32); err == nil {
					parsedRow = append(parsedRow, int32(v))
				} else {
					parsedRow = append(parsedRow, int32(0))
				}
			case "Int64":
				if v, err := strconv.ParseInt(columnString, 10, 64); err == nil {
					parsedRow = append(parsedRow, int64(v))
				} else {
					parsedRow = append(parsedRow, int64(0))
				}
			case "Float32":
				if v, err := strconv.ParseFloat(columnString, 32); err == nil {
					parsedRow = append(parsedRow, float32(v))
				} else {
					parsedRow = append(parsedRow, float32(0))
				}
			case "Float64":
				if v, err := strconv.ParseFloat(columnString, 64); err == nil {
					parsedRow = append(parsedRow, float64(v))
				} else {
					parsedRow = append(parsedRow, float64(0))
				}
			case "Date":
				if _, err := time.Parse("2006-01-02", columnString); err == nil {
					parsedRow = append(parsedRow, columnString)
				} else {
					parsedRow = append(parsedRow, "1970-01-01")
				}
			case "Datetime":
				if _, err := time.Parse("2006-01-02 15:04:05", columnString); err == nil {
					parsedRow = append(parsedRow, columnString)
				} else {
					parsedRow = append(parsedRow, "1970-01-01 00:00:00")
				}
			default:
				parsedRow = append(parsedRow, columnString)
			}

		}

		if _, err = stmt.Exec(parsedRow...); err != nil {
			return len(data), err
		}

	}

	if err := tx.Commit(); err != nil {
		return len(data), err
	}

	return len(data), err

}

func createTable(con *sql.DB, database string, table string, columns []interface{}, engine string) error {

	var err error

	if _, err = con.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", database)); err != nil {
		return err
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (", database, table)

	for _, column := range columns {
		col, _ := column.(map[string]interface{})
		keys := make([]interface{}, 0, len(col))
		for k := range col {
			keys = append(keys, k)
		}
		columnName, _ := keys[0].(string)
		columnType := col[columnName]
		query += fmt.Sprintf("%s %s, ", columnName, columnType)
	}

	query = strings.TrimRight(query, ", ")

	query += ") " + engine

	query = strings.TrimRight(query, ";") + ";"

	if _, err = con.Exec(query); err != nil {
		return fmt.Errorf("error while executing '%s':\n%s", query, err.Error())
	}

	return nil

}

func truncateTable(con *sql.DB, database string, table string) error {

	var create string

	row := con.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table))

	err := row.Scan(&create)

	if err != nil {
		return err
	}

	if _, err := con.Exec(fmt.Sprintf("DROP TABLE `%s`.`%s`", database, table)); err != nil {
		return err
	}

	if _, err := con.Exec(create); err != nil {
		return err
	}

	return nil

}

func inSlice(a string, list []string) bool {

	for _, b := range list {
		if b == a {
			return true
		}
	}

	return false

}
