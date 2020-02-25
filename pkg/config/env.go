package config

import (
	"github.com/kelseyhightower/envconfig"
)

type EnvConfig struct {
	WorkPath string `default:"./"`
	ScheduleFilename string `default:"schedule.yaml"`
	JobRunLogFilename string `default:"runlog.yaml"`
	LogLevel          string `default:"debug"`
}


func (c *EnvConfig) LoadConfig(prefix string) error {
	return envconfig.Process(prefix, c)
}

