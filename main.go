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

var FlagConfig = flag.String("config", "./config/config.json", "JSON config file path")

var SourceInstance gomulus.SourceInterface

var DestinationInstance gomulus.DestinationInterface

var FetchPool map[int]chan map[string]interface{}

var PersistPool map[int]chan [][]interface{}

var FetchChannelLength = 1000

var PersistChannelLength = 1000

var PendingJobsCount int64

func main() {

	var err error
	var started = time.Now()

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

	Source := config.Source
	Destination := config.Destination

	FetchPool = make(map[int]chan map[string]interface{}, 0)

	for i := 1; i <= int(math.Max(1, float64(Source.Pool))); i++ {

		FetchPool[i] = make(chan map[string]interface{}, FetchChannelLength)

	}

	PersistPool = make(map[int]chan [][]interface{}, 0)

	for i := 1; i <= int(math.Max(1, float64(Destination.Pool))); i++ {

		PersistPool[i] = make(chan [][]interface{}, PersistChannelLength)

	}

	log.Print("starting...")

	if SourceInstance, DestinationInstance, err = Start(Source, Destination, config.Plugins); err != nil {
		log.Fatal(err.Error())
	}

	go func() {

		for q, Selection := range FetchPool {

			go func(FetchChannel chan map[string]interface{}, q int) {

				for job := range FetchChannel {

					if data, err := SourceInstance.FetchData(job); err != nil {

						atomic.AddInt64(&PendingJobsCount, -1)

						log.Print("failed data fetching on queue ", q, "; an error occurred: ", err.Error())

					} else {

						data, err := DestinationInstance.PreProcessData(data)

						if err != nil {

							atomic.AddInt64(&PendingJobsCount, -1)

							log.Print("failed data pre-processing on queue ", q, "; an error occurred: ", err.Error())

						} else {

							queue := 0

							for true {

								lengths := make(map[int]int, 0)

								for id, queue := range PersistPool {
									lengths[id] = len(queue)
								}

								queue = GetShortestQueue(lengths)

								if len(PersistPool[queue]) <= 0 || len(PersistPool[queue]) < PersistChannelLength {
									break
								}

								time.Sleep(time.Millisecond * 500)

							}

							PersistPool[queue] <- data

							log.Print("fetching ", len(data), " rows on queue ", q, "...")

						}

					}

				}

			}(Selection, q)

		}

	}()

	go func() {

		for q, concurrentInsert := range PersistPool {

			go func(PersistChannel chan [][]interface{}, q int) {

				for data := range PersistChannel {

					log.Print("fetched ", len(data), " rows on queue ", q, "...")

					atomic.AddInt64(&PendingJobsCount, -1)

					if n, err := DestinationInstance.PersistData(data); err != nil {

						log.Print("failed data persist on queue ", q, "; lost ", n, ", an error occurred: ", err.Error())

					} else {

						log.Print("persisted ", n, " rows on queue ", q)

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
	timeOut := int(config.Timeout / 1000)

	go func() {
		for {
			select {
			case <-timeTimer.C:
				timeElapsed++
				if timeOut > 0 && timeElapsed > timeOut {
					log.Fatal(fmt.Sprintf("timed out after %d seconds", timeOut))
				}
				if atomic.LoadInt64(&PendingJobsCount) == 0 {
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

func Start(Source gomulus.DriverConfig, Destination gomulus.DriverConfig) (gomulus.SourceInterface, gomulus.DestinationInterface, error) {

	var err error
	var found bool
	var source gomulus.SourceInterface
	var destination gomulus.DestinationInterface

	err = nil
	found = false

	switch Source.Driver {

	case "csv":

		source = &sources.DefaultCSVSource{}
		found = true

	case "mysql":

		source = &sources.DefaultMysqlSource{}
		found = true

	default:

		pluginPath, err = filepath.Abs(Source.Plugin)
		if err != nil {
			break
		}
		plug, err := plugin.Open(plugin.Path)
		if err != nil {
			break
		}
		symbol, err := plug.Lookup(pc.Name)
		if err != nil {
			break
		}
		source, err = symbol.(gomulus.SourceInterface)
		if err != nil {
			break
		}

		found = true

	}

	if !found {

		return nil, nil, fmt.Errorf("no source driver found under the name `%s`", Source.Driver, err.Error())

	}

	err = nil
	found = false

	switch Destination.Driver {

	case "csv":

		destination = &destinations.DefaultCSVDestination{}
		found = true

	case "mysql":

		destination = &destinations.DefaultMysqlDestination{}
		found = true

	default:

		pluginPath, err = filepath.Abs(Destination.Plugin)
		if err != nil {
			break
		}
		plug, err := plugin.Open(plugin.Path)
		if err != nil {
			break
		}
		symbol, err := plug.Lookup(pc.Name)
		if err != nil {
			break
		}
		destination, err = symbol.(gomulus.DestinationInterface)
		if err != nil {
			break
		}

		found = true

	}

	if !found {

		return nil, nil, fmt.Errorf("no destination driver found under the name `%s`", Destination.Driver, err.Error())

	}

	log.Print(fmt.Sprintf("starting a new `%s` source driver instance...", Source.Driver))

	if err = source.New(Source.Options); err != nil {
		return nil, nil, err
	}

	log.Print(fmt.Sprintf("starting a new `%s` destination driver instance...", Destination.Driver))

	if err = destination.New(Destination.Options); err != nil {
		return nil, nil, err
	}

	log.Print(fmt.Sprintf("getting source driver jobs..."))

	jobs, err := source.GetJobs()

	lengths := make(map[int]int, 0)

	for id, queue := range FetchPool {
		lengths[id] = len(queue)
	}

	go func() {

		for _, job := range jobs {

			atomic.AddInt64(&PendingJobsCount, 1)

			queue := 0

			lengths := make(map[int]int, 0)

			for id, queue := range FetchPool {
				lengths[id] = len(queue)
			}

			queue = GetShortestQueue(lengths)

			FetchPool[queue] <- job

		}

	}()

	return source, destination, nil

}

func GetShortestQueue(lengths map[int]int) int {

	var minLength = math.MaxInt64
	var queue int

	if queue == 0 {
		for id, length := range lengths {
			if length < minLength {
				queue = id
				minLength = length
			}
		}
	}

	return queue

}
