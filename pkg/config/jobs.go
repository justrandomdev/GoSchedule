package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

const timeFormat string = "15:04"

type RunTimes struct {
	LastRun time.Time
	NextRun time.Time
}

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

type SqlJobSpec struct {
	Name       string    `yaml:"name"`
	Connection string    `yaml:"connection"`
	When       *Schedule `yaml:"when"`
	Query      string    `yaml:"query"`
	RunTimes
}

type DataField struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

type ImportJobSpec struct {
	Name        string      `yaml:"name"`
	Connection  string      `yaml:"connection"`
	When        *Schedule   `yaml:"when"`
	ImportQuery string      `yaml:"importQuery"`
	ColumnMap   []DataField `yaml:"columnMap"`
	ExportQuery string      `yaml:"exportQuery"`
	RunTimes
}

type DbConnectionDefs struct {
	Type     string `yaml:"type"`
	Host     string `yaml:"host"`
	Port     uint16 `yaml:"port"`
	DbName   string `yaml:"dbName"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type JobsConfig struct {
	SqlJobs     []SqlJobSpec                `yaml:"sqlJobs"`
	ImportJobs  []ImportJobSpec             `yaml:"sqlImports"`
	Connections map[string]DbConnectionDefs `yaml:"dbConnections"`
}

func LoadConfig(workPath string, filename string) (*JobsConfig, error) {
	osFile := filepath.ToSlash(workPath + filename)
	data, err := ioutil.ReadFile(osFile)

	if err != nil {
		return nil, err
	}

	scheduleYaml := []byte(os.ExpandEnv(string(data)))

	config := &JobsConfig{}
	err = yaml.Unmarshal(scheduleYaml, config)
	if err != nil {
		return nil, err
	}

	dupNames := make(map[string]bool)
	if err = validateSqlJobs(config.SqlJobs, dupNames); err != nil {
		return nil, err
	}
	if err = validateImportJobs(config.ImportJobs, dupNames); err != nil {
		return nil, err
	}

	return config, err
}

func validateSqlJobs(config []SqlJobSpec, names map[string]bool) error {

	for idx, job := range config {
		if ok, err := validateDuplicateName(names, job.Name); !ok {
			return err
		}

		if job.Query == "" {
			return fmt.Errorf("Query value in sql job %s is required", job.Name)
		}

		if job.Name == "" {
			return fmt.Errorf("Name value in sql job number %d is required", idx+1)
		}

		if job.Connection == "" {
			return fmt.Errorf("Query value in sql job %s is required", job.Name)
		}

		if job.When == nil {
			return fmt.Errorf("When value in import job %s is required", job.Name)
		}

		if ok, err := validateWhen(job.When, job.Name); !ok {
			return err
		}
	}

	return nil
}

func validateImportJobs(config []ImportJobSpec, names map[string]bool) error {

	for idx, job := range config {
		if ok, err := validateDuplicateName(names, job.Name); !ok {
			return err
		}

		if job.Name == "" {
			return fmt.Errorf("Name value in import job number %d is required", idx+1)
		}
		if job.Connection == "" {
			return fmt.Errorf("Query value in import job %s is required", job.Name)
		}

		if job.When == nil {
			return fmt.Errorf("When value in import job %s is required", job.Name)
		}

		if job.ImportQuery == "" {
			return fmt.Errorf("ImportQuery value in import job %s is required", job.Name)
		}

		if job.ColumnMap == nil {
			return fmt.Errorf("ColumnMap value in import job %s is required", job.Name)
		}

		if job.ExportQuery == "" {
			return fmt.Errorf("ExportQuery value in import job %s is required", job.Name)
		}

		if ok, err := validateWhen(job.When, job.Name); !ok {
			return err
		}

	}

	return nil
}

func validateDuplicateName(names map[string]bool, jobName string) (bool, error) {
	if _, found := names[jobName]; found {
		return false, fmt.Errorf("Job name %s is duplicated", jobName)
	}
	names[jobName] = true

	return true, nil
}

func validateWhen(when *Schedule, jobName string) (bool, error) {
	if when.Time == "" && when.Frequency == nil {
		return false, fmt.Errorf("Either time or frequency in import job %s is required", jobName)
	}

	if when.Frequency != nil && when.Time != "" {
		if when.Frequency.Minute != 0 && when.Frequency.Hour != 0 {
			return false, fmt.Errorf("When using time and frequency neither minute nor hour can be used in import job %s", jobName)
		}
		if when.Frequency.Day == 0 && when.Frequency.Week == 0 && when.Frequency.Month == 0 && when.Frequency.Year == 0 {
			return false, fmt.Errorf("When using time and frequency, a frequency value needs to be supplied in import job %s", jobName)
		}
		if _, err := time.Parse(timeFormat, when.Time); err != nil {
			return false, fmt.Errorf("Invalid time format in import job %s. Format expected is 15:04", jobName)
		}
	}

	return true, nil
}
