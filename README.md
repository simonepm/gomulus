# GOmulus

Fast, modular and extensible data-forklift pool manager written in GO.

## Introduction

GOmulus is a tool for moving data-sets from any source to any destination.

By default you can move data from MySQL tables to CSV files and viceversa, but GOmulus is easily exensible with any data source by building .so plugins that follows a lean GO interface.

GOmulus is also easy to configure. Pass just one JSON configuration file telling the script the designated source, destination and how many concurrent operations of selection and insertion is allowed to perform (specifing a pool dimension as integer greater or equal to 1) and you are ready to... GO.

## Installation

By default GOmulus depends only on two packages:

    # go get github.com/go-sql-driver/mysql
    # go get github.com/gofrs/flock

## Build

    # go build -o ./gomulus ./main.go

## Run

    # ./gomulus --config "./config/path/name.json"

## Configuration

As depicted above, you should pass a JSON configuration file that declares a source and a destination.
Every source and destination has its own driver and every driver can perform concurrent operations by increasing the pool parameter value (from 1 to N; suggested 1 per CPU).

A driver requires some kind of configuration to run on your data endpoint; the `option` parameter is here right to address this need.

### Simple example

    {
      "source": {
        "pool": 4,
        "driver": "mysql",
        "options": {
          "offset":   0,
          "limit":    10000,
          "endpoint": "user:pass@tcp(host:port)",
          "database": "database",
          "table":    "table",
          "columns":  "*"
        }
      },
      "destination": {
        "pool": 1,
        "driver": "csv",
        "options": {
          "path": "./data/csv/path/name.csv",
          "truncate": true
        }
      }
    }

In the basic example above, GOmulus will select 10000 rows per routine (4 in total) and will persist the selected data on a CSV file, truncated beforehand, or created if it doesn't exists already.

## Custom sources and destinations

"mysql" and "csv" are the default drivers provided to get your hands dirt on a first run.

But you can extend GOmulus by adding any custom data source or destination as follows.

### TL;DR

In the `./plugin` directory in the root of this repository you can find ready-made examples of a source and a destination custom drivers.

Build and import them by using the `go build -buildmode=plugin` command and following the __"Configuration example with a custom destination driver"__ section.

### Extend the default source interface

Develop your custom source driver by extending the default source GO interface:

    type SourceInterface interface {
        New(map[string]interface{}) error
        GetJobs() ([]map[string]interface{}, error)
        FetchData(map[string]interface{}) ([][]interface{}, error)
    }
    
On GOmulus startup the source option parameter of the JSON config file is passed as `map[string]interface{}` to `New(map[string]interface{})` of your custom source driver.

`GetJobs()` of your custom source driver should return a slice of jobs.

Each job should be in the form of `map[string]interface{}` containing the meta info needed by `FetchData(map[string]interface{})` method of your custom source driver to actually perform the fetch operation.

`FetchData(map[string]interface{})` of your custom source driver should return the fetched data as `[][]interface{}`.
Data will be passed by GOmuus to the `PreProcessData([][]interface{})` method of the designated destination driver instance for further processing.

#### Build your custom source driver
    
    # go build -buildmode=plugin -o ./path/name.so ./path/name.go
    
### Extend the default destination interface

Develop your custom destination driver by extending the destination GO interface:

    type DestinationInterface interface {
        New(map[string]interface{}) error
        PreProcessData([][]interface{}) ([][]interface{}, error)
        PersistData([][]interface{}) (int, error)
    }
    
On GOmulus startup the destination option parameter of the JSON config file is passed as `map[string]interface{}` to `New(map[string]interface{})` of your custom destination driver.

`PreProcessData([][]interface{})` of your custom destination driver optionally preprocess data passed as argument from the source driver instance and then should return it in the same format (`[][]interface{}`).

`PersistData([][]interface{})` of your custom destination driver should actually persist data passed to it as argument by GOmulus and then return the number of persisted rows as integer.
    
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
          "offset":   0,
          "limit":    10000,
          "endpoint": "user:pass@tcp(host:port)",
          "database": "database",
          "table":    "table",
          "columns":  "*"
        }
      },
      "destination": {
        "pool": 4,
        "driver": "ExportedPluginDriverVariable",
        "options": {
          "custom_option_a": "a",
          "custom_option_b": "b"
        }
      }
    }
