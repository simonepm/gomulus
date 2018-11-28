package gomulus

import (
	"bufio"
	"encoding/csv"
	"io"
	"math"
	"os"
	"gomulus"
	"strings"
)

// DefaultCSVSource ...
type DefaultCSVSource struct {
	Config gomulus.DriverConfig
	File   *os.File
	Path   string
	Limit  int
	EOF    string
}

// New ...
func (s *DefaultCSVSource) New(config gomulus.DriverConfig) error {

	var err error
	var file *os.File
	var limit, _ = config.Options["limit"].(float64)
	var endpoint, _ = config.Options["endpoint"].(string)
	var eof, _ = config.Options["line_separator"].(string)
	var rowLimit = int(math.Max(1, limit))

	if eof == "" {
		eof = "\n"
	}

	if file, err = os.Open(endpoint); err != nil {
		return err
	}

	s.File = file
	s.Path = endpoint
	s.Limit = rowLimit
	s.EOF = eof

	return nil

}

// GetTasks ...
func (s *DefaultCSVSource) GetTasks() ([]gomulus.SelectionTask, error) {

	count := 0
	offset := 0
	bytes := 0
	tasks := make([]gomulus.SelectionTask, 0)
	lines := make(map[int]int, 0)
	scanner := bufio.NewScanner(s.File)

	for scanner.Scan() {

		count++
		bytes += len(scanner.Bytes()) + len([]byte(s.EOF))
		lines[count] = bytes

	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}

	for true {

		if offset > count {
			break
		}

		from := lines[offset]
		to := lines[offset+s.Limit]

		if to-from < 1 {
			break
		}

		offset = offset + s.Limit

		tasks = append(tasks, gomulus.SelectionTask{
			Meta: map[string]interface{}{
				"from": from,
				"to":   to,
			},
		})

	}

	return tasks, nil

}

// ProcessTask ...
func (s *DefaultCSVSource) ProcessTask(SelectionTask gomulus.SelectionTask) ([][]interface{}, error) {

	var err error
	var data [][]interface{}
	var file *os.File
	var from, _ = SelectionTask.Meta["from"].(int)
	var to, _ = SelectionTask.Meta["to"].(int)

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