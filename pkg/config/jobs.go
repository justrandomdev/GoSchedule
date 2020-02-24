package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

const timeFormat string = "15:04"

type JobFrequency struct {
	Minute byte `yaml:"minute"`
	Hour   byte `yaml:"hour"`
	Day    byte `yaml:"day"`
	Week   byte `yaml:"week"`
	Month  byte `yaml:"month"`
	Year   byte `yaml:"year"`
}

type Schedule struct {
	Time      string        `yaml:"time"`
	Frequency *JobFrequency `yaml:"frequency"`
}

type JobSpec struct {
	Name    string    `yaml:"name"`
	When    *Schedule `yaml:"when"`
	Query   string    `yaml:"query"`
	LastRun time.Time
	NextRun time.Time
}

type JobsConfig struct {
	Jobs []JobSpec `yaml:"sqljobs"`
}

func (c *JobsConfig) LoadConfig(workPath string, filename string) (*JobsConfig, error) {
	osFile := filepath.ToSlash(workPath + filename)
	scheduleYaml, err := ioutil.ReadFile(osFile)

	if err != nil {
		return nil, err
	}

	config := &JobsConfig{}
	err = yaml.Unmarshal(scheduleYaml, config)

	if err != nil {
		return nil, err
	}

	err = c.validate(config)

	return config, err
}

func (c JobsConfig) validate(config *JobsConfig) error {

	for idx, job := range config.Jobs {
		if job.Name == "" {
			return errors.New(fmt.Sprintf("Name value in job number %d is required", idx))
		}
		if job.Query == "" {
			return errors.New(fmt.Sprintf("Query value in job name %s is required", job.Name))
		}
		if job.When == nil {
			return errors.New(fmt.Sprintf("When value in job name %s is required", job.Name))
		}
		if job.When.Time == "" && job.When.Frequency == nil {
			return errors.New(fmt.Sprintf("Either time or frequency in job name %s is required", job.Name))
		}
		if job.When.Frequency != nil && job.When.Time != "" {
			if job.When.Frequency.Minute != 0 && job.When.Frequency.Hour != 0 {
				return errors.New(fmt.Sprintf("When using time and frequency neither minute nor hour can be used in job name %s", job.Name))
			}
			if job.When.Frequency.Day == 0 && job.When.Frequency.Week == 0 && job.When.Frequency.Month == 0 && job.When.Frequency.Year == 0 {
				return errors.New(fmt.Sprintf("When using time and frequency, a frequency value needs to be supplied in job name %s", job.Name))
			}
			if _, err := time.Parse(timeFormat, job.When.Time); err != nil {
				return errors.New(fmt.Sprintf("Invalid time format in job name %s. Format expected is 15:04", job.Name))
			}
		}
	}

	return nil
}
