# GOmulus

Fast, modular and extensible data-forklift pool manager written in GO.

## Introduction

GOmulus is a tool for moving data-set from any source to any destination.

By default is packed with two drivers - "mysql" and "csv" - so you can move data from/to MySQL tables and CSV files.
Anyway, GOmulus is easily extensible with any data source by building custom .so plugins.
Pass a JSON configuration and you are ready to... GO.

## Installation

By default GOmulus depends only on two packages:

    # go get github.com/go-sql-driver/mysql
    # go get github.com/gofrs/flock

## Build

    # go build -o ./gomulus ./main.go

## Run

    # ./gomulus --config "./config.json"

## Configuration

In your JSON configuration file you should declare a `source` and a `destination` as follows:

    "source": {
        "pool": 4,
        "driver": "DriverName",
        "options": { [...] }
    },
    "destination": {
        "pool": 4,
        "driver": "DriverName",
        "options": { [...] }
    }

`driver` is the chosen driver name.
`options` is a custom object containing all necessary information for your driver to run on your data-set (e.g. MySQL connection settings).
`pool` should be an integer greater or equal to 1 (suggested equals to the number of CPU on your machine, default 1) corresponding to the number of concurrent operations that your driver is allowed to perform.

### Configuration example - from MySQL table to CSV file

    {
      "source": {
        "pool":         4,
        "driver":       "mysql",
        "options": {
          "host":       "<user>:<pass>@tcp(<host>:<port>)",
          "database":   "<database>",
          "table":      "<table>",
          "offset":     0,
          "limit":      1000
        }
      },
      "destination": {
        "pool":         1,
        "driver":       "csv",
        "options": {
          "path":       "<filepath>",
          "truncate":   true
        }
      }
    }
    
In the example above, GOmulus will select 1000 rows per routine (4000 in total) from a MySQL table, starting from the first row and will persist the selected data on a CSV file, truncated beforehand, or created if it doesn't exists.
    
### Configuration example - from CSV file to MySQL table

    {
      "source": {
        "pool":     1,
        "driver":   "csv",
        "options": {
          "path":       "<filepath>",
          "column_sep": ",",
          "line_sep":   "\n",
          "lines":      1000
        }
      },
      "destination": {
        "pool":     4,
        "driver":   "mysql",
        "options": {
          "host":       "<user>:<pass>@tcp(<host>:<port>)",
          "database":   "<database>",
          "table":      "<table>",
          "truncate":   true
        }
      }
    }

In the example above, GOmulus will select 1000 lines per batch from a CSV file and will persist the selected data on a MySQL table, truncated beforehand, or created if it doesn't exists.

## Custom source and destination drivers

"mysql" and "csv" are the default drivers provided, but you can extend GOmulus by adding any custom data source or destination as follows.

### TL;DR

In the `plugin` directory of this repository you can find ready-made examples of a source and a destination custom drivers.

### Extend the default driver SourceInterface

Develop your custom source driver by extending the default SourceInterface:

```go
    type SourceInterface interface {
        New(map[string]interface{}) error
        GetJobs() ([]map[string]interface{}, error)
        GetData(map[string]interface{}) ([][]interface{}, error)
    }
```

`New` method of your driver should expect a `map[string]interface{}` as argument, corresponding to the source `options` object in your JSON configuration file.
Here you can initialize your driver and return an error in case something goes wrong with the configuration options provided.

`GetJobs` method should return a list of __jobs__ in the form of `[]map[string]interface{}`.
Every job will be passed to `GetData` method next.

`GetData` is the method that should effectively perform the selection operation by following the info contained in the __job__  (`map[string]interface{}`) passed as argument.
`GetData` method should return __data__ as `[][]interface{}`: a slice of rows containing a slice of columns.
    
### Extend the default driver DestinationInterface

Develop your custom destination driver by extending the default DestinationInterface:

```go
    type DestinationInterface interface {
        New(map[string]interface{}) error
        PreProcessData([][]interface{}) ([][]interface{}, error)
        PersistData([][]interface{}) (int, error)
    }
```

`New` method of your driver should expect a `map[string]interface{}` as argument, corresponding to the destination `options` object in your JSON configuration file. Here you can initialize your driver and return an error in case something goes wrong with the configuration options provided.

`PreProcessData` receives the __data__ (`[][]interface{}`) returned from the source driver `GetData` method as argument, allowing you to optionally modify its content before actually persisting it with the `PersistData` method.

`PersistData` is the method that should effectively perform the insertion operation of __data__ (`[][]interface{}`) passed as argument. It should return the number of rows persisted in case of success alongside eventual errors occurred.

#### Build custom drivers
    
    # go build -buildmode=plugin -o ./plugin.so ./plugin.go
    
### Usage of custom drivers

Pass a `plugin` parameter inside your driver declaration object containing the path to your custom driver plugin.
`driver` name should reflect the exported variable name of type SourceInterface or DriverInterface of your plugin.

    {
      "source": {
        "pool":     4,
        "plugin":   "./plugin/source/clickhouse.so"
        "driver":   "ClickhouseSource",
        "options": {
          [...]
        }
      },
      "destination": {
        "pool":     4,
        "plugin":   "./plugin/destination/clickhouse.so"
        "driver":   "ClickhouseDestination",
        "options": {
          [...]
        }
      }
    }
    
To know more on how GO plugins works I suggest to read the following resources:

- https://golang.org/pkg/plugin/
- https://medium.com/learning-the-go-programming-language/writing-modular-go-programs-with-plugins-ec46381ee1a9

To know more on how GO interfaces works I suggest to read the following resources:

- https://gobyexample.com/interfaces
- https://medium.com/golangspec/interfaces-in-go-part-i-4ae53a97479c
