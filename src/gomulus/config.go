package gomulus

type Config struct {
	Timeout     int          `json:"timeout,omitempty"`
	Source      DriverConfig `json:"source"`
	Destination DriverConfig `json:"destination"`
}

type DriverConfig struct {
	Driver  string                 `json:"driver"`
	Plugin  string                 `json:"plugin,omitempty"`
	Options map[string]interface{} `json:"options,omitempty"`
	Pool    int                    `json:"pool,omitempty"`
}
