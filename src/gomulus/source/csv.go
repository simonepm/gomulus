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
	Config gomulus.DriverConfig
	File   *os.File
	Path   string
	Limit  int
	EOF    string
	Sep    string
	Offset int
}

func (s *DefaultCSVSource) New(config map[string]interface{}) error {

	var err error
	var file *os.File
	var eof, _ = config["line_sep"].(string)
	var sep, _ = config["column_sep"].(string)
	var limit, _ = config["limit"].(float64)
	var path, _ = config["path"].(string)
	var offset, _ = config["offset"].(float64)

	if eof == "" {
		eof = "\n"
	}

	if sep == "" {
		sep = ","
	}

	if path, err = filepath.Abs(path); err != nil {
		return err
	}

	if file, err = os.Open(path); err != nil {
		return err
	}

	s.EOF = eof
	s.Sep = sep
	s.File = file
	s.Path = path
	s.Limit = int(math.Max(1, limit))
	s.Offset = int(math.Max(0, limit))

	return nil

}

func (s *DefaultCSVSource) GetJobs() ([]map[string]interface{}, error) {

	var jobs = make([]map[string]interface{}, 0)
	var lines = make(map[int]int, 0)

	offset := s.Offset
	count := 0
	scanner := bufio.NewScanner(s.File)

	lines[0] = 0

	for scanner.Scan() {

		count++
		lines[count] += len(scanner.Bytes()) + len([]byte(s.EOF))

	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
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

		offset = offset + s.Limit

		jobs = append(jobs, map[string]interface{}{
			"from": from,
			"to":   to,
		})

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
	reader.Comma = s.Sep

	for {

		slice := make([]interface{}, 0)

		line, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			continue
		}

		for _, l := range line {
			slice = append(slice, []byte(l))
		}

		data = append(data, slice)

	}

	return data, nil

}
