package gomulus

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gomulus"
	"math"
	"regexp"
)

type DefaultMysqlSource struct {
	Config   gomulus.DriverConfig
	DB       *sql.DB
	Limit    int
	Count    int
	Offset   int
	Table    string
	Columns  string
	Database string
}

func (s *DefaultMysqlSource) New(config map[string]interface{}) error {

	var err error
	var db *sql.DB
	var rows *sql.Rows
	var count, _ = config["count"].(float64)
	var offset, _ = config["offset"].(float64)
	var endpoint, _ = config["endpoint"].(string)
	var database, _ = config["database"].(string)
	var table, _ = config["table"].(string)
	var limit, _ = config["limit"].(float64)
	var columns, _ = config["columns"].(string)
	var tables = make([]string, 0)

	if columns == "" {
		columns = "*"
	}

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

	if !inSlice(table, tables) {
		return fmt.Errorf("table not found `%s`.`%s`", database, table)
	}

	if count == 0 {
		if err = db.QueryRow(fmt.Sprintf("SELECT COUNT(0) `%s`.`%s`", database, table)).Scan(&count); err != nil {
			return err
		}
	}

	s.DB = db
	s.Table = table
	s.Database = database
	s.Count = int(math.Max(1, count))
	s.Limit = int(math.Max(1, limit))
	s.Offset = int(math.Max(0, offset))
	s.Columns = columns

	return nil

}

func (s *DefaultMysqlSource) GetJobs() ([]map[string]interface{}, error) {

	offset := s.Offset
	jobs := make([]map[string]interface{}, 0)

	for true {

		if offset > s.Count-1 {
			break
		}

		query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` LIMIT %d, %d", s.Columns, s.Database, s.Table, offset, s.Limit)

		offset += s.Limit

		jobs = append(jobs, map[string]interface{}{
			"query": query,
		})

	}

	return jobs, nil

}

func (s *DefaultMysqlSource) FetchData(meta map[string]interface{}) ([][]interface{}, error) {

	var db = s.DB
	var query, _ = meta["query"].(string)

	return Select(db, query)

}

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

		if err = rows.Scan(pointers...); err != nil {
			return nil, err
		}

		if err = rows.Err(); err != nil {
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
