# gomulus

Fast, modular and extensible "data-forklift" written in GO.

## Install dependences

    # go get github.com/go-sql-driver/mysql
    # go get github.com/gofrs/flock

## Build

    # go build -o ./gomulus.bin ./main.go

## Run

    # ./gomulus.bin --config "./config/path/name.json"

## Configuration template

    {
      "timeout": 10000,
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
        "driver": "csv",
        "options": {
          "endpoint": "./data/csv/path/name.csv",
          "truncate": true
        }
      }
    }
    
## Default drivers

    csv
    mysql

## Building a custom source driver

Extend the default source interface

    type SourceInterface interface {
        New(DriverConfig) error
        GetTasks() ([]SelectionTask, error)
        ProcessTask(SelectionTask) ([][]interface{}, error)
    }
    
and build your custom driver
    
    # go build -buildmode=plugin -o ./path/name.so ./path/name.go
    
## Building a custom destination driver

Extend the default destination interface

    type DestinationInterface interface {
        New(DriverConfig) error
        GetTask([][]interface{}) (InsertionTask, error)
        ProcessTask(InsertionTask) (int, error)
    }
    
and build your custom driver
    
    # go build -buildmode=plugin -o ./path/name.so ./path/name.go
    
### Configuration template example with a custom driver

    {
      "timeout": 10000,
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
        "driver": "ExportedPluginDriverVarName",
        "options": {
          // custom options
          // accessible from DriverConfig.Options as map[string]interface{}
          // they are passed as argument to your driver in New(DriverConfig) method on startup;
          // parsed from config json file
        }
      },
      "plugins": {
        "sources": [],
        "destinations": [
          {
            "name": "ExportedPluginDriverVarName",
            "path": "./go/plugin/path/name.so"
          }
        ]
      }
    }