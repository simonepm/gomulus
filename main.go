package main

import (
	"flag"
	"fmt"
	"gomulus"
	destinations "gomulus/destination"
	sources "gomulus/source"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"plugin"
	"sync/atomic"
	"syscall"
	"time"
)

// FlagConfig ...
var FlagConfig = flag.String("config", "./gomulus.json", "JSON config file path")

// SelectionTaskPool ...
var SelectionTaskPool map[int]chan gomulus.SelectionTask

// InsertionTaskPool ...
var InsertionTaskPool map[int]chan gomulus.InsertionTask

// SourceInstance ...
var SourceInstance gomulus.SourceInterface

// DestinationInstance ...
var DestinationInstance gomulus.DestinationInterface

// PendingTasksCount ...
var PendingTasksCount int64

// SelectionChannelLength
var SelectionChannelLength = 1000

// InsertionChannelLength
var InsertionChannelLength = 1000

func main() {

	var err error
	var started = time.Now()

	// Config

	flag.Parse()

	var config = gomulus.Config{}
	var configPath string
	var configFile *os.File

	if configPath, err = filepath.Abs(*FlagConfig); err != nil {
		log.Fatal(err.Error())
	}

	if configFile, err = os.Open(configPath); err != nil {
		log.Fatal(err.Error())
	}

	configJSON, _ := ioutil.ReadAll(configFile)

	_ = configFile.Close()

	if err = config.Unmarshal(configJSON); err != nil {
		log.Fatal(err.Error())
	}

	log.Print("initializing...")

	Source := config.Source
	Destination := config.Destination

	SelectionTaskPool = make(map[int]chan gomulus.SelectionTask, 0)

	for i := 1; i <= int(math.Max(1, float64(Source.Pool))); i++ {

		SelectionTaskPool[i] = make(chan gomulus.SelectionTask, SelectionChannelLength)

	}

	InsertionTaskPool = make(map[int]chan gomulus.InsertionTask, 0)

	for i := 1; i <= int(math.Max(1, float64(Destination.Pool))); i++ {

		InsertionTaskPool[i] = make(chan gomulus.InsertionTask, InsertionChannelLength)

	}

	log.Print("starting...")

	if SourceInstance, DestinationInstance, err = Start(Source, Destination, config.Plugins); err != nil {
		log.Fatal(err.Error())
	}

	log.Print("waiting for selection tasks...")

	go func() {

		for q, concurrentSelect := range SelectionTaskPool {

			go func(SelectionChannel chan gomulus.SelectionTask, q int) {

				for SelectionTask := range SelectionChannel {

					if data, err := SourceInstance.ProcessTask(SelectionTask); err != nil {

						atomic.AddInt64(&PendingTasksCount, -1)

						log.Print("failed task on selection queue ", q, "; an error occurred: ", err.Error())

					} else {

						InsertionTask, err := DestinationInstance.GetTask(data)

						if err != nil {

							atomic.AddInt64(&PendingTasksCount, -1)

							log.Print("failed insertion task generation on selection queue ", q, "; an error occurred: ", err.Error())

						} else {

							minLengthQueue := 0

							for true {

								queuesLengths := make(map[int]int, 0)

								for id, queue := range InsertionTaskPool {
									queuesLengths[id] = len(queue)
								}

								minLengthQueue = GetShortestQueue(queuesLengths)

								if len(InsertionTaskPool[minLengthQueue]) <= 0 || len(InsertionTaskPool[minLengthQueue]) < InsertionChannelLength {
									break
								}

								time.Sleep(time.Millisecond * 500)

							}

							InsertionTaskPool[minLengthQueue] <- InsertionTask

							log.Print("selected ", len(data), " rows by selection task on queue ", q)

						}

					}

				}

			}(concurrentSelect, q)

		}

	}()

	log.Print("waiting for insertion tasks...")

	go func() {

		for q, concurrentInsert := range InsertionTaskPool {

			go func(InsertionChannel chan gomulus.InsertionTask, q int) {

				for InsertionTask := range InsertionChannel {

					atomic.AddInt64(&PendingTasksCount, -1)

					if n, err := DestinationInstance.ProcessTask(InsertionTask); err != nil {

						log.Print("failed task on insertion queue ", q, "; lost ", n, " rows due to an error: ", err.Error())

					} else {

						log.Print("stored ", n, " rows by insertion task on queue ", q)

					}

				}

			}(concurrentInsert, q)

		}

	}()

	log.Print("running...")

	sigterm := make(chan os.Signal, 2)

	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	timeElapsed := 0
	timeTimer := time.NewTimer(time.Second)
	timeOut := int(config.Timeout/1000)

	go func() {
		for {
			select {
			case <-timeTimer.C:
				timeElapsed++
				if timeOut > 0 && timeElapsed > timeOut {
					log.Fatal(fmt.Sprintf("timed out after %d seconds", timeOut))
				}
				if atomic.LoadInt64(&PendingTasksCount) == 0 {
					sigterm <- syscall.SIGINT
				}
				timeTimer.Reset(time.Second)
				break
			}
		}
	}()

	<-sigterm

	log.Print("DONE, took ", time.Now().Unix()-started.Unix(), " seconds")

	os.Exit(0)

}

// Start ...
func Start(Source gomulus.DriverConfig, Destination gomulus.DriverConfig, Plugins gomulus.PluginsConfig) (gomulus.SourceInterface, gomulus.DestinationInterface, error) {

	var err error
	var found bool
	var source gomulus.SourceInterface
	var destination gomulus.DestinationInterface
	var sourcePlugins []gomulus.PluginConfig
	var destinationPlugins []gomulus.PluginConfig

	sourcePlugins = Plugins.Sources
	destinationPlugins = Plugins.Destinations

	log.Print("selecting source driver...")

	found = false

	switch Source.Driver {

	case "csv":

		found = true
		source = &sources.DefaultCSVSource{}

	case "mysql":

		found = true
		source = &sources.DefaultMysqlSource{}

	default:

		for _, pc := range sourcePlugins {
			if Source.Driver == pc.Name {
				found = true
				pc.Path, err = filepath.Abs(pc.Path)
				if err != nil {
					log.Fatal(err.Error())
				}
				plug, err := plugin.Open(pc.Path)
				if err != nil {
					log.Fatal(err.Error())
				}
				symbol, err := plug.Lookup(pc.Name)
				source, _ = symbol.(gomulus.SourceInterface)
				break
			}
		}

	}

	if !found {

		return nil, nil, fmt.Errorf("no source driver found under the name `%s`", Source.Driver)

	}

	log.Print("selecting destination driver...")

	found = false

	switch Destination.Driver {

	case "csv":

		found = true
		destination = &destinations.DefaultCSVDestination{}

	case "mysql":

		found = true
		destination = &destinations.DefaultMysqlDestination{}

	default:

		for _, pc := range destinationPlugins {
			if Destination.Driver == pc.Name {
				found = true
				pc.Path, err = filepath.Abs(pc.Path)
				if err != nil {
					log.Fatal(err.Error())
				}
				plug, err := plugin.Open(pc.Path)
				if err != nil {
					log.Fatal(err.Error())
				}
				symbol, err := plug.Lookup(pc.Name)
				if err != nil {
					log.Fatal(err.Error())
				}
				destination, _ = symbol.(gomulus.DestinationInterface)
				break
			}
		}

	}

	if !found {

		return nil, nil, fmt.Errorf("no destination driver found under the name `%s`", Destination.Driver)

	}

	log.Print("starting a new destination driver instance...")

	err = destination.New(Destination)

	if err != nil {
		return nil, nil, err
	}

	log.Print("starting a new source driver instance...")

	if err = source.New(Source); err != nil {
		return nil, nil, err
	}

	log.Print("getting selection tasks...")

	SelectionTasks, err := source.GetTasks()

	queuesLengths := make(map[int]int, 0)

	for id, queue := range SelectionTaskPool {

		queuesLengths[id] = len(queue)

	}

	log.Print("spinning selection tasks...")

	go func() {

		for _, SelectionTask := range SelectionTasks {

			atomic.AddInt64(&PendingTasksCount, 1)

			minLengthQueue := 0

			queuesLengths := make(map[int]int, 0)

			for id, queue := range SelectionTaskPool {
				queuesLengths[id] = len(queue)
			}

			minLengthQueue = GetShortestQueue(queuesLengths)

			SelectionTaskPool[minLengthQueue] <- SelectionTask

		}

	}()

	return source, destination, nil

}

// GetShortestQueue ...
func GetShortestQueue(lengths map[int]int) int {

	var minLength = math.MaxInt64
	var minLengthQueue int

	if minLengthQueue == 0 {
		for id, length := range lengths {
			if length < minLength {
				minLengthQueue = id
				minLength = length
			}
		}
	}

	return minLengthQueue

}