package jobs

import (
	"context"
	"io/ioutil"
	"jobrunner/pkg/config"
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
	jobs              map[string]config.JobSpec
	runChan           chan config.JobSpec
	batchCompleteChan chan bool
	isRunning         bool
	logPath           string
	location          *time.Location
	ctx               context.Context
	wg                sync.WaitGroup
}

func NewRunner(runLogPath string, context context.Context) (*JobsRunner, error) {
	loc, err := time.LoadLocation("UTC")

	if err != nil {
		return nil, err
	}

	return &JobsRunner{
		isRunning: false,
		logPath:   filepath.ToSlash(runLogPath),
		location:  loc,
		ctx:       context,
	}, nil
}

func (j *JobsRunner) LoadJobs(items []config.JobSpec) error {
	j.jobs = make(map[string]config.JobSpec, len(items))
	j.RunLog = make(map[string]JobRunLogItem)

	for _, item := range items {
		if _, ok := j.jobs[item.Name]; !ok {
			j.jobs[item.Name] = item
		}
	}

	//Update runtimes from log
	existingRunLog, err := j.loadRunLog()
	if err != nil {
		return err
	}

	for k, v := range existingRunLog {
		if _, hit := j.jobs[k]; hit {

			js := config.JobSpec{
				Name:    j.jobs[k].Name,
				When:    j.jobs[k].When,
				Query:   j.jobs[k].Query,
				LastRun: v.LastRun,
				NextRun: v.NextRun,
			}

			j.jobs[k] = js
		}
	}

	return nil

}

func (j *JobsRunner) Start() error {
	j.runChan = make(chan config.JobSpec)
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
		for _, job := range j.jobs {
			j.processJobs(job)
			log.Debugf("Job: %+v", job)
		}
		j.batchCompleteChan <- true

		//Wait for current execution to complete or timeout on context done
		select {
		case <-j.ctx.Done():
			return j.ctx.Err()
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
	close(j.runChan)
	close(j.batchCompleteChan)
	log.Tracef("Jobrunner.Stop channels closed")

}

func (j *JobsRunner) processJobs(job config.JobSpec) {

	log.Debugf("Executing processJobs")
	if !j.isRunning {
		return
	}

	if job.LastRun.IsZero() && job.When.Time == "" {
		//New entry, not time based, run now
		j.wg.Add(1)
		j.runChan <- job
	} else if job.LastRun.IsZero() && job.When.Time != "" {
		//New entry, time based, run at the correct time
		y, m, d := time.Now().Date()
		timeWithDate, _ := time.Parse(timeFormat, job.When.Time)
		target := timeWithDate.AddDate(y, int(m), d)

		if j.isDateEqual(target, time.Now()) {
			j.wg.Add(1)
			j.runChan <- job
		}
	} else if !job.LastRun.IsZero() && !job.NextRun.IsZero() {
		//Is it time to run?
		t := time.Now()
		if j.isDateEqual(job.NextRun, t) || t.After(job.NextRun) {
			j.wg.Add(1)
			j.runChan <- job
		}
	}
}

func (j *JobsRunner) executeJobs() {
	for job := range j.runChan {
		log.Debugf("executeJobs got job from channel: " + job.Name)
		t := time.Now().UTC()

		//Update run times
		job.LastRun = t
		job.NextRun = j.getNextRun(job)
		j.logJobExecution(job, "success")

		log.Debugf("Starting long running job")
		time.Sleep(30 * time.Second)
		j.wg.Done()
	}
}

func (j *JobsRunner) logJobExecution(job config.JobSpec, result string) {
	j.RunLog[job.Name] = JobRunLogItem{
		//Name:    job.Name,
		Result:  result,
		LastRun: job.LastRun,
		NextRun: job.NextRun,
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

func (j *JobsRunner) getNextRun(job config.JobSpec) time.Time {
	log.Debugf("Executing getNextRun on job %s", job.Name)
	var lastTime, nextTime time.Time

	if job.When.Time != "" {
		y, m, d := time.Now().Date()
		lastTime, _ = time.Parse(timeFormat, job.When.Time)
		lastTime = time.Date(y, m, d, lastTime.Hour(), lastTime.Minute(), lastTime.Second(), 0, j.location)
	} else {
		lastTime = job.LastRun
	}

	switch {
	case job.When.Frequency.Year > 0:
		nextTime = lastTime.AddDate(int(job.When.Frequency.Year), 0, 0)
	case job.When.Frequency.Month > 0:
		nextTime = lastTime.AddDate(0, int(job.When.Frequency.Month), 0)
	case job.When.Frequency.Day > 0:
		nextTime = lastTime.AddDate(0, 0, int(job.When.Frequency.Day))
	case job.When.Frequency.Hour > 0:
		nextTime = lastTime.Add(time.Duration(job.When.Frequency.Hour) * time.Hour)
	case job.When.Frequency.Minute > 0:
		nextTime = lastTime.Add(time.Duration(job.When.Frequency.Minute) * time.Minute)
	}

	return nextTime
}

func (j *JobsRunner) isDateEqual(a time.Time, b time.Time) bool {
	return a.Hour() == b.Hour() && a.Minute() == b.Minute()
}
