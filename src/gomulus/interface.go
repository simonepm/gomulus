package gomulus

// SourceInterface ...
type SourceInterface interface {
	New(DriverConfig) error
	GetTasks() ([]SelectionTask, error)
	ProcessTask(SelectionTask) ([][]interface{}, error)
}

// DestinationInterface ...
type DestinationInterface interface {
	New(DriverConfig) error
	GetTask([][]interface{}) (InsertionTask, error)
	ProcessTask(InsertionTask) (int, error)
}

// SelectionTask ...
type SelectionTask struct {
	Meta map[string]interface{}
}

// InsertionTask ...
type InsertionTask struct {
	Meta map[string]interface{}
	Data [][]interface{}
}
