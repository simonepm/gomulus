# GOmulus

Fast, modular and extensible data-forklift pool manager written in GO.

## Introduction

GOmulus is a tool for moving data-sets from any source to any destination.

By default you can move data from/to MySQL tables and CSV files, but GOmulus is easily exensible by building .so plugins that follows a lean GO interface.

GOmulus is also easy to configure. Pass a JSON configuration file telling the script the designated source, destination and how many concurrent operations of selection and insertion is allowed to perform (specifing a pool dimension as integer greater or equal to 1) and you are ready to... GO.

## Dependences

By default GOmulus depends on two GO packages:

    # go get github.com/go-sql-driver/mysql
    # go get github.com/gofrs/flock

## Build

    # go build -o ./GOmulus ./main.go

## Run

    # ./GOmulus --config "./config/path/name.json"

## Configuration

As depicted above, you should pass a JSON configuration file that declares a source and a destination.
Every source and destination has its own driver and every driver can perform concurrent operations by increasing the pool parameter (from 1 to N).

A driver needs some kind of configuration to run on your data endpoint, so the option parameter is here just to address this need.

### Simple example

    {
      "timeout": 10000,
      "source": {
        "driver": "mysql",
        "pool": 4,
        "options": {
          "limit":    1000,
          "endpoint": "user:pass@tcp(host:port)/db",
          "table":    "tb"
        }
      },
      "destination": {
        "driver": "csv",
        "pool": 1,
        "options": {
          "limit":    1000,
          "endpoint": "./data/csv/path/name.csv",
          "truncate": true
        }
      }
    }

In this basic example GOmulus will select 1000 rows per concurrent routine (4 in total) and will persist the selected data on a CSV file, truncated beforehand.

## Custom sources and destinations

"mysql" and "csv" are only the default drivers provided to get your hands dirt on a first run.
But you can extend GOmulus by adding your custom data sources and destinations as follows.

###TL;DR

In the ./plugin/ directory of this repository you can find ready-made examples of a source and a destination custom driver. Build them by using the `go build -buildmode=plugin` command and import following the "configuration example with a custom destination driver" section.

### Extend the default source interface

Develop your custom source driver by extending the default source GO interface:

    type SourceInterface interface {
        New(DriverConfig) error
        GetTasks() ([]SelectionTask, error)
        ProcessTask(SelectionTask) ([][]interface{}, error)
    }

The `New(DriverConfig) error` method of your driver should expect a DriverConfig GO structure:

    type DriverConfig struct {
      Driver  string                 `json:"driver"`
      Pool    int                    `json:"pool,omitempty"`
      Options map[string]interface{} `json:"options,omitempty"`
    }
    
So you can easily access the Options parameter, parsed from the JSON configuration file option parameter of your driver.

The `GetTasks() ([]SelectionTask, error)` method should return a slice of selection operations in the form of `[]SelectionTask`

    type SelectionTask struct {
      Meta map[string]interface{}
    }

Is the `ProcessTask(SelectionTask) ([][]interface{}, error)` method that should effectively perform the selection operation by following the info contained in the `SelectionTask.Meta` parameter passed to it.

It should return the selected data in the form of `[][]interface{}` that will be automatically passed to the `GetTask([][]interface{}) (InsertionTask, error)` method of the destination driver and persited by the `ProcessTask(InsertionTask) (int, error)` method of the latter.

#### Build your custom source driver
    
    # go build -buildmode=plugin -o ./path/name.so ./path/name.go
    
### Extend the default destination interface

Develop your custom destination driver by extending the destination GO interface provided:

    type DestinationInterface interface {
        New(DriverConfig) error
        GetTask([][]interface{}) (InsertionTask, error)
        ProcessTask(InsertionTask) (int, error)
    }
    
The `New(DriverConfig) error` method of your driver should expect a DriverConfig GO structure:

    type DriverConfig struct {
      Driver  string                 `json:"driver"`
      Pool    int                    `json:"pool,omitempty"`
      Options map[string]interface{} `json:"options,omitempty"`
    }
    
So you can easily access the Options parameter parsed from the JSON configuration file.

The `GetTask([][]interface{}) (InsertionTask, error)` method should return an insertion operation in the form of an `InsertionTask` in which `Meta` is a `map[string]interface{}` containing optional custom info to help the `ProcessTask(InsertionTask) (int, error)` method to perform the insertion operation.

    type InsertionTask struct {
      Meta map[string]interface{}
      Data [][]interface{}
    }

`GetTask([][]interface{}) (InsertionTask, error)` gets the selected Data from the source in the form of `[][]interface{}` as argument, this argument should be passed AS IS to the returned `InsertionTask.Data` parameter.

The purpose of getting it in this early step is only if you need it to populate the `InsertionTask.Meta` parameter accordingly.

Is the `ProcessTask(InsertionTask) (int, error)` method that should effectively perform the insertion operation of the `InsertionTask.Data` by optionally using the `InsertionTask.Meta` parameter as helper.

#### Build your custom destination driver
    
    # go build -buildmode=plugin -o ./path/name.so ./path/name.go
    
### Configuration example with a custom destination driver

    {
      "plugins": {
        "sources": [],
        "destinations": [
          {
            "name": "ExportedPluginDriverVariable",
            "path": "./go/plugin/path/name.so"
          }
        ]
      },
      "source": {
        "pool": 4,
        "driver": "mysql",
        "options": {
          "limit":    1000,
          "endpoint": "user:pass@tcp(host:port)/db",
          "table":    "tb"
        }
      },
      "destination": {
        "pool": 1,
        "driver": "ExportedPluginDriverVariable",
        "options": {
          "custom_option_a": "a",
          "custom_option_b": "b"
        }
      }
    }