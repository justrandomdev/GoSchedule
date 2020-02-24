package config

import (
	"github.com/kelseyhightower/envconfig"
)

type EnvConfig struct {
	CoreDbHost string `required:"true"`  
	CoreDbPort uint16 `required:"true"`
	CoreDbName string `required:"true"`
	CoreDbUser string `required:"true"`
	CoreDbPassword string `required:"true"`

	DwDbHost string `required:"true"`
	DwDbPort uint16 `required:"true"`
	DwDbName string `required:"true"`
	DwDbUser string `required:"true"`
	DwDbPassword string `required:"true"`

	WorkPath string `default:"./"`
	ScheduleFilename string `default:"schedule.yaml"`
	JobRunLogFilename string `default:"runlog.yaml"`
	LogLevel          string `default:"debug"`
}


func (c *EnvConfig) LoadConfig(prefix string) error {
	return envconfig.Process(prefix, c)
}

