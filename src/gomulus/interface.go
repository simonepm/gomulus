package gomulus

type SourceInterface interface {
	New(map[string]interface{}) error
	GetJobs() ([]map[string]interface{}, error)
	FetchData(map[string]interface{}) ([][]interface{}, error)
}

type DestinationInterface interface {
	New(map[string]interface{}) error
	PreProcessData([][]interface{}) ([][]interface{}, error)
	PersistData([][]interface{}) (int, error)
}