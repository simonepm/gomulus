package gomulus

import (
	"bufio"
	"encoding/csv"
	"gomulus"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

type DefaultCSVSource struct {
	Config  gomulus.DriverConfig
	File    *os.File
	Path    string
	Limit   int
	Offset  int
	EOL     string
	Comma   string
	Columns []int
}

func (s *DefaultCSVSource) New(config map[string]interface{}) error {

	var err error
	var file *os.File
	var comma, _ = config["column_separator"].(string)
	var eol, _ = config["line_separator"].(string)
	var limit, _ = config["limit"].(float64)
	var offset, _ = config["offset"].(float64)
	var path, _ = config["path"].(string)
	columns, ok := config["columns"].([]interface{})

	if eol == "" {
		eol = "\n"
	}

	if comma == "" {
		comma = ","
	}

	if path, err = filepath.Abs(path); err != nil {
		return err
	}

	if file, err = os.Open(path); err != nil {
		return err
	}

	s.EOL = eol
	s.Comma = comma
	s.File = file
	s.Path = path
	s.Limit = int(math.Max(1, limit))
	s.Offset = int(math.Max(0, offset))
	s.Comma = comma

	if ok {
		for _, c := range columns {
			cc, _ := c.(float64)
			s.Columns = append(s.Columns, int(cc))
		}
	}

	return nil

}

func (s *DefaultCSVSource) GetJobs() ([]map[string]interface{}, error) {

	var jobs = make([]map[string]interface{}, 0)
	var lines = make(map[int]int, 0)

	offset := s.Offset
	count := 0
	total := 0
	scanner := bufio.NewScanner(s.File)

	lines[0] = 0
	for scanner.Scan() {
		count++
		length := total + len([]byte(scanner.Text())) + len([]byte(s.EOL))
		lines[count] = length
		total = length
		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
	}

	for true {

		if offset >= count {
			break
		}

		from := lines[offset]
		to := lines[offset+s.Limit]

		if to-from < 1 {
			break
		}

		jobs = append(jobs, map[string]interface{}{
			"from": from,
			"to":   to,
		})

		offset = offset + s.Limit

	}

	return jobs, nil

}

func (s *DefaultCSVSource) FetchData(job map[string]interface{}) ([][]interface{}, error) {

	var err error
	var data [][]interface{}
	var file *os.File
	var from, _ = job["from"].(int)
	var to, _ = job["to"].(int)

	if false {

		if file, err = os.Open(s.Path); err != nil {
			return nil, err
		}

		defer file.Close()

	} else {

		file = s.File

	}

	buffer := make([]byte, to-from)

	if _, err = file.ReadAt(buffer, int64(from)); err != nil && err != io.EOF {
		return nil, err
	}

	reader := csv.NewReader(strings.NewReader(string(buffer)))
	reader.Comma = []rune(s.Comma)[0]

	for {

		slice := make([]interface{}, 0)

		columns, err := reader.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		for i, c := range columns {
			if len(s.Columns) > 0 && InSliceInt(i, s.Columns) || len(s.Columns) == 0 {
				slice = append(slice, []byte(c))
			}
		}

		data = append(data, slice)

	}

	return data, nil

}

func InSliceInt(a int, list []int) bool {

	for _, b := range list {
		if b == a {
			return true
		}
	}

	return false

}
