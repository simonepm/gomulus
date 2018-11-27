package gomulus

import "encoding/json"

// Config ...
type Config struct {
	Timeout     int           `json:"timeout,omitempty"`
	Source      DriverConfig  `json:"source"`
	Destination DriverConfig  `json:"destination"`
	Plugins     PluginsConfig `json:"plugins,omitempty"`
}

// Unmarshal Config ...
func (c *Config) Unmarshal(data []byte) error {
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	return nil
}

// PluginsConfig ...
type PluginsConfig struct {
	Sources      []PluginConfig `json:"sources"`
	Destinations []PluginConfig `json:"destinations"`
}

// PluginConfig ...
type PluginConfig struct {
	Name   string `json:"name"`
	Symbol string `json:"symbol,omitempty"`
	Path   string `json:"path"`
}

// DriverConfig ...
type DriverConfig struct {
	Driver  string                 `json:"driver"`
	Options map[string]interface{} `json:"options,omitempty"`
	Pool    int                    `json:"pool,omitempty"`
}
