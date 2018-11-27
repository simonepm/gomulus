package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"plugin"
	"gomulus"
	sources "gomulus/source"
	destinations "gomulus/destination"
	"strings"
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

func init() {
	rand.Seed(time.Now().Unix())
}

func main() {

	var err error
	var started = time.Now()

	// Config

	flag.Parse()

	var config = gomulus.Config{}
	var configPath string
	var configFile *os.File

	if configPath, err = filepath.Abs(*FlagConfig); err != nil {
		panic(err)
	}

	if configFile, err = os.Open(configPath); err != nil {
		panic(err)
	}

	configJSON, _ := ioutil.ReadAll(configFile)

	_ = configFile.Close()

	if err = config.Unmarshal(configJSON); err != nil {
		panic(err)
	}

	// Init

	Source := config.Source
	Destination := config.Destination

	SelectionChannelLength := 1000

	SelectionTaskPool = make(map[int]chan gomulus.SelectionTask, 0)

	for i := 1; i <= int(math.Max(1, float64(Source.Pool))); i++ {

		SelectionTaskPool[i] = make(chan gomulus.SelectionTask, SelectionChannelLength)

	}

	// TODO limit selection->insertion rate

	InsertionChannelLength := 1000

	InsertionTaskPool = make(map[int]chan gomulus.InsertionTask, 0)

	for i := 1; i <= int(math.Max(1, float64(Destination.Pool))); i++ {

		InsertionTaskPool[i] = make(chan gomulus.InsertionTask, InsertionChannelLength)

	}

	go func() {

		for q, concurrentSelect := range SelectionTaskPool {

			go func(SelectionChannel chan gomulus.SelectionTask, q int) {

				for SelectionTask := range SelectionChannel {

					if data, err := SourceInstance.ProcessTask(SelectionTask); err != nil {

						atomic.AddInt64(&PendingTasksCount, -1)

						fmt.Fprintln(os.Stderr, "failed task on selection queue", q, "; an error occurred:", err.Error())

					} else {

						InsertionTask, err := DestinationInstance.GetTask(data)

						if err != nil {

							atomic.AddInt64(&PendingTasksCount, -1)

							fmt.Fprintln(os.Stderr, "failed insertion task generation on selection queue", q, "; an error occurred:", err.Error())

						} else {

							queuesLengths := make(map[int]int, 0)

							for id, queue := range InsertionTaskPool {
								queuesLengths[id] = len(queue)
							}

							minLengthQueue := GetShortestQueue(queuesLengths)

							InsertionTaskPool[minLengthQueue] <- InsertionTask

							fmt.Fprintln(os.Stdout, "selected", len(data), "rows by selection task on queue", q)

						}

					}

				}

			}(concurrentSelect, q)

		}

	}()

	go func() {

		for q, concurrentInsert := range InsertionTaskPool {

			go func(InsertionChannel chan gomulus.InsertionTask, q int) {

				for InsertionTask := range InsertionChannel {

					atomic.AddInt64(&PendingTasksCount, -1)

					if n, err := DestinationInstance.ProcessTask(InsertionTask); err != nil {

						fmt.Fprintln(os.Stderr, "failed task on insertion queue", q, "; lost", n, "rows due to an error: ", err.Error())

					} else {

						fmt.Fprintln(os.Stdout, "stored", n, "rows by insertion task on queue", q)

					}

				}

			}(concurrentInsert, q)

		}

	}()

	// Run

	if SourceInstance, DestinationInstance, err = Run(Source, Destination, config.Plugins); err != nil {
		panic(err)
	}

	SelectionTasks, err := SourceInstance.GetTasks()

	queuesLengths := make(map[int]int, 0)

	for id, queue := range SelectionTaskPool {

		queuesLengths[id] = len(queue)

	}

	for _, SelectionTask := range SelectionTasks {

		atomic.AddInt64(&PendingTasksCount, 1)

		minLengthQueue := GetShortestQueue(queuesLengths)

		SelectionTaskPool[minLengthQueue] <- SelectionTask

	}

	// Exit

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
					panic(fmt.Errorf("timed out after %d seconds", timeOut))
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

	fmt.Fprintln(os.Stdout, "DONE, took", time.Now().Unix()-started.Unix(), "seconds")

	os.Exit(0)

}

// Run ...
func Run(Source gomulus.DriverConfig, Destination gomulus.DriverConfig, Plugins gomulus.PluginsConfig) (gomulus.SourceInterface, gomulus.DestinationInterface, error) {

	var err error
	var found bool
	var source gomulus.SourceInterface
	var destination gomulus.DestinationInterface
	var sourcePlugins []gomulus.PluginConfig
	var destinationPlugins []gomulus.PluginConfig

	sourcePlugins = Plugins.Sources
	destinationPlugins = Plugins.Destinations

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
					panic(err)
				}
				plug, err := plugin.Open(pc.Path)
				if err != nil {
					panic(err)
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

	if err = source.New(Source); err != nil {
		return nil, nil, err
	}

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
					panic(err)
				}
				plug, err := plugin.Open(pc.Path)
				if err != nil {
					panic(err)
				}
				if pc.Symbol == "" {
					pc.Symbol = fmt.Sprintf("%s%s", strings.Title(pc.Name), "Destination")
				}
				symbol, err := plug.Lookup(pc.Symbol)
				if err != nil {
					panic(err)
				}
				destination, _ = symbol.(gomulus.DestinationInterface)
				break
			}
		}

	}

	if !found {

		return nil, nil, fmt.Errorf("no destination driver found under the name `%s`", Destination.Driver)

	}

	if err = destination.New(Destination); err != nil {
		return nil, nil, err
	}

	return source, destination, nil

}

// GetShortestQueue ...
func GetShortestQueue(lengths map[int]int) int {

	var minLength = math.MaxInt64
	var minLengthQueue = 0 // RandInt(1, len(lengths))

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

// RandInt ...
func RandInt(min, max int) int {
	return rand.Intn(max - min) + min
}
