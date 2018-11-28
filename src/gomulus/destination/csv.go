package gomulus

import (
	"encoding/csv"
	"fmt"
	"github.com/gofrs/flock"
	"os"
	"path/filepath"
	"gomulus"
)

// DefaultCSVDestination ...
type DefaultCSVDestination struct {
	Config gomulus.DriverConfig
	Flock  *flock.Flock
	File   *os.File
}

// New ...
func (d *DefaultCSVDestination) New(config gomulus.DriverConfig) error {

	var err error
	var file *os.File
	var endpoint, _ = config.Options["endpoint"].(string)
	var truncate, _ = config.Options["truncate"].(bool)

	if endpoint, err = filepath.Abs(endpoint); err != nil {
		return err
	}

	fileLock := flock.New(endpoint)

	if truncate {
		fmt.Fprintln(os.Stdout, "truncating path", endpoint, "...")
		if file, err = os.OpenFile(endpoint, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666); err != nil {
			return err
		}
	} else {
		if file, err = os.OpenFile(endpoint, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666); err != nil {
			return err
		}
	}

	d.Flock = fileLock

	d.File = file

	return nil

}

// GetTask ...
func (d *DefaultCSVDestination) GetTask(data [][]interface{}) (gomulus.InsertionTask, error) {

	return gomulus.InsertionTask{
		Meta: map[string]interface{}{},
		Data: data,
	}, nil

}

// ProcessTask ...
func (d *DefaultCSVDestination) ProcessTask(InsertionTask gomulus.InsertionTask) (int, error) {

	locked, _ := d.Flock.TryLock()

	file := d.File

	wr := csv.NewWriter(file)

	for _, row := range InsertionTask.Data {

		values := make([]string, 0)

		for _, column := range row {
			col, _ := column.([]byte)
			values = append(values, string(col))
		}

		err := wr.Write(values)

		if err != nil {
			return len(InsertionTask.Data), err
		}

	}

	wr.Flush()

	if locked {
		d.Flock.Unlock()
	}

	if wr.Error() != nil {
		return len(InsertionTask.Data), wr.Error()
	}

	return len(InsertionTask.Data), nil

}
