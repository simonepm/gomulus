package gomulus

import (
	"encoding/csv"
	"github.com/gofrs/flock"
	"gomulus"
	"os"
	"path/filepath"
)

type DefaultCSVDestination struct {
	Config gomulus.DriverConfig
	Flock  *flock.Flock
	File   *os.File
}

func (d *DefaultCSVDestination) New(config map[string]interface{}) error {

	var err error
	var file *os.File
	var path, _ = config["path"].(string)
	var truncate, _ = config["truncate"].(bool)

	if path, err = filepath.Abs(path); err != nil {
		return err
	}

	if truncate {
		if file, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666); err != nil {
			return err
		}
	} else {
		if file, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666); err != nil {
			return err
		}
	}

	d.Flock = flock.New(path)
	d.File = file

	return nil

}

func (d *DefaultCSVDestination) PreProcessData(data [][]interface{}) ([][]interface{}, error) {

	return data, nil

}

func (d *DefaultCSVDestination) PersistData(data [][]interface{}) (int, error) {

	locked, _ := d.Flock.TryLock()

	file := d.File

	wr := csv.NewWriter(file)

	for _, row := range data {

		values := make([]string, 0)

		for _, column := range row {
			col, _ := column.([]byte)
			values = append(values, string(col))
		}

		err := wr.Write(values)

		if err != nil {
			return len(data), err
		}

	}

	wr.Flush()

	if locked {
		_ = d.Flock.Unlock()
	}

	if wr.Error() != nil {
		return len(data), wr.Error()
	}

	return len(data), nil

}
