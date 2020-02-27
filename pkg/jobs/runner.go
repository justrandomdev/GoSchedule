package jobs

import (
	"context"
	"io/ioutil"
	"jobrunner/pkg/config"
	"jobrunner/pkg/db"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const (
	timeFormat           string = "15:04"
	runChannelBufferSize int    = 5
)

type JobRunLogItem struct {
	//Name    string    `yaml:"name"`
	LastRun time.Time `yaml:"lastRun"`
	NextRun time.Time `yaml:"nextRun"`
	Result  string    `yaml:"result"`
}

type JobRunLogMap struct {
	RunLog map[string]JobRunLogItem `yaml:"logItems"`
}

type JobsRunner struct {
	JobRunLogMap
	sqlJobs           map[string]config.SqlJobSpec
	importJobs        map[string]config.ImportJobSpec
	connections       map[string]db.DbConnection
	runChan           chan interface{}
	batchCompleteChan chan bool
	isRunning         bool
	logPath           string
	location          *time.Location
	//ctx               context.Context
	//ctxCancel         context.CancelFunc
	wg                sync.WaitGroup
}

func NewRunner(runLogPath string) (*JobsRunner, error) {
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		return nil, err
	}

	//thisCtx, cancel := context.WithCancel(parentCtx)	

	return &JobsRunner{
		isRunning: false,
		logPath:   filepath.ToSlash(runLogPath),
		location:  loc,
		//ctx:       thisCtx,
		//ctxCancel: cancel,
	}, nil
}

func (j *JobsRunner) LoadJobs(items *config.JobsConfig) error {
	//Convert config to maps used internally
	j.sqlJobs = make(map[string]config.SqlJobSpec, len(items.SqlJobs))
	j.importJobs = make(map[string]config.ImportJobSpec, len(items.ImportJobs))
	j.connections = make(map[string]db.DbConnection, len(items.Connections))
	j.RunLog = make(map[string]JobRunLogItem)

	for _, item := range items.SqlJobs {
		if _, ok := j.sqlJobs[item.Name]; !ok {
			j.sqlJobs[item.Name] = item
		}
	}

	for _, item := range items.ImportJobs {
		if _, ok := j.importJobs[item.Name]; !ok {
			j.importJobs[item.Name] = item
		}
	}

	for k, v := range items.Connections {
		dbConn := db.DbConnection{}
		if err := dbConn.Connect(v.Type, v.Host, v.DbName, v.User, v.Password, v.Port); err != nil {
			return err
		}
		j.connections[k] = dbConn
	}

	//Update runtimes from log
	existingRunLog, err := j.loadRunLog()
	if err != nil {
		return err
	}

	for k, v := range existingRunLog {
		if _, hit := j.sqlJobs[k]; hit {
			js := config.SqlJobSpec{
				Name:       j.sqlJobs[k].Name,
				When:       j.sqlJobs[k].When,
				Query:      j.sqlJobs[k].Query,
				Connection: j.sqlJobs[k].Connection,
				RunTimes: config.RunTimes{
					LastRun: v.LastRun,
					NextRun: v.NextRun,
				},
			}
			j.sqlJobs[k] = js
		}

		if _, hit := j.importJobs[k]; hit {
			js := config.ImportJobSpec{
				Name:        j.importJobs[k].Name,
				When:        j.importJobs[k].When,
				Connection:  j.importJobs[k].Connection,
				ImportQuery: j.importJobs[k].ImportQuery,
				ColumnMap:   j.importJobs[k].ColumnMap,
				ExportQuery: j.importJobs[k].ExportQuery,
				RunTimes: config.RunTimes{
					LastRun: v.LastRun,
					NextRun: v.NextRun,
				},
			}
			j.importJobs[k] = js
		}
	}

	return nil
}

func (j *JobsRunner) disconnectDb() error {
	for _, v := range j.connections {
		if err := v.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (j *JobsRunner) Start(parentCtx context.Context) error {
	thisCtx, cancel := context.WithCancel(parentCtx)		
	defer cancel()

	j.runChan = make(chan interface{}, runChannelBufferSize)
	j.batchCompleteChan = make(chan bool)

	go j.executeJobs()

	//Write to runlog after each 'batch' has been processed
	go func(receiver *JobsRunner) {
		for range j.batchCompleteChan {
			if err := receiver.writeJobRunLog(); err != nil {
				log.Errorf("Error writing run log file: %v\n", err)
			}
		}
	}(j)

	j.isRunning = true

	//Keepalive loop. Will wake up every minute to check if a job needs to be run.
	//This will only stop on context cancellation
	for {
		for _, job := range j.sqlJobs {
			j.processJobs(job)
			log.Debugf("Job: %+v", job)
		}

		for _, job := range j.importJobs {
			j.processJobs(job)
			log.Debugf("Job: %+v", job)
		}

		j.batchCompleteChan <- true

		//Wait for current execution to complete or timeout on context done
		select {
		case <-thisCtx.Done():
			return thisCtx.Err()
		default:
			time.Sleep(30 * time.Second)
		}
	}
}

func (j *JobsRunner) Stop() {
	j.isRunning = false

	log.Tracef("Jobrunner.Stop waiting...")
	j.wg.Wait()
	log.Tracef("Jobrunner.Stop waiting done")

	j.disconnectDb()

	close(j.runChan)
	close(j.batchCompleteChan)
	log.Tracef("Jobrunner.Stop channels closed")
}

func (j *JobsRunner) processJobs(job interface{}) {

	log.Debugf("Executing processJobs")
	if !j.isRunning {
		return
	}

	var lastRun, nextRun time.Time
	var when *config.Schedule

	if val, ok := job.(config.SqlJobSpec); ok {
		lastRun = val.LastRun
		nextRun = val.NextRun
		when = val.When
	}

	if val, ok := job.(config.ImportJobSpec); ok {
		lastRun = val.LastRun
		nextRun = val.NextRun
		when = val.When
	}

	if lastRun.IsZero() && when.Time == "" {
		//New entry, not time based, run now
		j.wg.Add(1)
		j.runChan <- job
	} else if lastRun.IsZero() && when.Time != "" {
		//New entry, time based, run at the correct time
		y, m, d := time.Now().Date()
		timeWithDate, _ := time.Parse(timeFormat, when.Time)
		target := timeWithDate.AddDate(y, int(m), d)

		if j.isDateEqual(target, time.Now()) {
			j.wg.Add(1)
			j.runChan <- job
		}
	} else if !lastRun.IsZero() && !nextRun.IsZero() {
		//Is it time to run?
		t := time.Now()
		if j.isDateEqual(nextRun, t) || t.After(nextRun) {
			j.wg.Add(1)
			j.runChan <- job
		}
	}
}

func (j *JobsRunner) executeJobs() {
	for genJob := range j.runChan {

		if val, ok := genJob.(config.SqlJobSpec); ok {
			err := j.handleSqlJob(&val)
			if err != nil {
				j.logJobExecution(val.Name, &val.RunTimes, "failure")
				log.Errorf("Error executing SQL job %s. %v", val.Name, err)
			} else {
				j.logJobExecution(val.Name, &val.RunTimes, "success")
			}
		}

		if val, ok := genJob.(config.ImportJobSpec); ok {
			j.handleImportJob(val)
			j.logJobExecution(val.Name, &val.RunTimes, "success")
		}

		//log.Debugf("Starting long running job")
		//time.Sleep(30 * time.Second)
		j.wg.Done()
	}
}

func (j *JobsRunner) handleSqlJob(job *config.SqlJobSpec) error {
	log.Debugf("executeJobs got job from channel: " + job.Name)
	t := time.Now().UTC()

	//Update run times
	job.LastRun = t
	job.NextRun = j.getNextRun(job.Name, job.LastRun, job.When)

	//Execute sql query
	db := j.connections[job.Connection]
	result, err := db.Conn.Exec(job.Query)
	if err != nil {
		return err
	}

	rowCount, err := result.RowsAffected()
	if err != nil {
		log.Debugf("%v", err)
	}

	log.Debugf("%s query executed. %d rows affected.", job.Name, rowCount)
	return nil
}

func (j *JobsRunner) handleImportJob(job config.ImportJobSpec) error {
	log.Debugf("executeJobs got job from channel: " + job.Name)
	t := time.Now().UTC()

	//Update run times
	job.LastRun = t
	job.NextRun = j.getNextRun(job.Name, job.LastRun, job.When)

	pipeline := NewSqlImportPipeline(j.connections, job)

	//Log import errors
	go func(){
		for err := range pipeline.ErrorChan {
			log.Errorf("Sql import error for job %s: %+v", job.Name, err)
		}		
	}()

	err := pipeline.StartImport()
	if err != nil {
		return err
	}

	/*
		for i, r := range resultSet {
			fld := reflect.Indirect(reflect.ValueOf(r))
			fmt.Printf("==============================> Row number: %d\n\n", i)
			for j := 0; j < fld.NumField(); j++ {
				name := fld.Type().Field(j).Name
				val := fld.Field(j).Interface()
				fmt.Printf("Field name: %s - value: %+v\n\n", name, val)
			}

		}

		fmt.Println(len(resultSet))
	*/
	//Generate struct that needs to store data
	//Execute ImportQuery

	//Read records
	//Parse export query(if needed SQLx should handle it)
	//Execute export query

	//Execute sql query
	/*
		db := j.connections[job.Connection]
		result, err := db.Conn.Exec(job.Query)
		if err != nil {
			log.Debugf("Error executing sql query for job %s. %v", job.Name, err)
		}

		rowCount, err := result.RowsAffected()
		if err != nil {
			log.Debugf("%v", err)
		}

		log.Debugf("%s query executed. %d rows affected.", job.Name, rowCount)
	*/
	return nil
}


func (j *JobsRunner) logJobExecution(name string, times *config.RunTimes, result string) {
	j.RunLog[name] = JobRunLogItem{
		Result:  result,
		LastRun: times.LastRun,
		NextRun: times.NextRun,
	}
}

func (j *JobsRunner) writeJobRunLog() error {
	//Read existing runlog
	missing := make(map[string]JobRunLogItem)

	if _, err := os.Stat(j.logPath); !os.IsNotExist(err) {
		existingRunLog, err := j.loadRunLog()
		if err != nil {
			return err
		}

		//Add any missing items back to runlog so they are not overwritten
		for key, item := range existingRunLog {
			if _, hit := j.RunLog[key]; !hit {
				missing[key] = item
			}
		}

		os.Remove(j.logPath)
	}

	for key, item := range missing {
		j.RunLog[key] = item
	}

	fh, err := os.OpenFile(j.logPath, os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	defer fh.Close()

	payload, err := yaml.Marshal(j.RunLog)

	if err != nil {
		return err
	}

	fh.Write(payload)

	j.RunLog = make(map[string]JobRunLogItem)

	return nil
}

func (j *JobsRunner) loadRunLog() (map[string]JobRunLogItem, error) {
	existingRunLog := make(map[string]JobRunLogItem)

	yamlLog, err := ioutil.ReadFile(j.logPath)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(yamlLog, existingRunLog)
	return existingRunLog, err
}

func (j *JobsRunner) getNextRun(name string, lastRun time.Time, when *config.Schedule) time.Time {
	log.Debugf("Executing getNextRun on job %s", name)
	var lastTime, nextTime time.Time

	if when.Time != "" {
		y, m, d := time.Now().Date()
		lastTime, _ = time.Parse(timeFormat, when.Time)
		lastTime = time.Date(y, m, d, lastTime.Hour(), lastTime.Minute(), lastTime.Second(), 0, j.location)
	} else {
		lastTime = lastRun
	}

	switch {
	case when.Frequency.Year > 0:
		nextTime = lastTime.AddDate(int(when.Frequency.Year), 0, 0)
	case when.Frequency.Month > 0:
		nextTime = lastTime.AddDate(0, int(when.Frequency.Month), 0)
	case when.Frequency.Day > 0:
		nextTime = lastTime.AddDate(0, 0, int(when.Frequency.Day))
	case when.Frequency.Hour > 0:
		nextTime = lastTime.Add(time.Duration(when.Frequency.Hour) * time.Hour)
	case when.Frequency.Minute > 0:
		nextTime = lastTime.Add(time.Duration(when.Frequency.Minute) * time.Minute)
	}

	return nextTime
}

func (j *JobsRunner) isDateEqual(a time.Time, b time.Time) bool {
	return a.Hour() == b.Hour() && a.Minute() == b.Minute()
}
