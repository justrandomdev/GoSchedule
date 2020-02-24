package main

import (
	"context"
	"io"
	"flag"
	"jobrunner/pkg/config"
	"jobrunner/pkg/db"
	"jobrunner/pkg/jobs"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	stdLog "log"
	log "github.com/sirupsen/logrus"
)

var (
	cfg                      config.EnvConfig
	jobConfig                config.JobsConfig
	jobRunner                *jobs.JobsRunner
	listenAddr, fileLocation string
	coreDb, warehouseDb      db.PostgresConnection
	logger                   *log.Logger
)

func main() {
	parseCmdFlags()
	getEnvConfig()
	initLogger()

	cancel, err := initApp()
	if err != nil {
		log.Fatalf("Error during application initialization: %v\n", err)
	}

	logWriter := logger.Writer()
	defer logWriter.Close()

	server := initHttpApi(logWriter)

	done, exit := setupShutdownChannels()
	go gracefulShutdown(cancel, exit, done, server)
	go jobRunner.Start()

	//log.Printf("Service listening on %s\n", listenAddr)
	//if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
	//	log.Fatalf("Could not listen on %s: %v\n", listenAddr, err)
	//}

	<-done
	log.Println("Service shutdown.")
}

func parseCmdFlags() {
	flag.StringVar(&listenAddr, "h", "0.0.0.0:8080", "HTTP Service bind address e.g. 0.0.0.0:8080 (Required)")
	flag.Parse()

	if listenAddr == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func getEnvConfig() {
	if err := cfg.LoadConfig("jobsched"); err != nil {
		log.Fatalf("Error reading environment variables. Reason - %v\n", err)
		os.Exit(1)
	}
}

func initLogger() {
	logger = log.New()
	logger.Out = os.Stdout

	level, err := log.ParseLevel(cfg.LogLevel)
	if err != nil {
		log.Fatalf("JOBSCHED_LOGLEVEL is set to an unknown value. Reason - %v\n", err)
		os.Exit(1)
	}

	log.SetLevel(level)
	log.Printf("Log level set to %s\n", level.String())
}

func initApp() (context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	runnerCtx, _ := context.WithCancel(ctx)

	jobsConfig, err := jobConfig.LoadConfig(cfg.WorkPath, cfg.ScheduleFilename)
	if err != nil {
		return cancel, err
	}

	jobRunner, err = jobs.NewRunner(cfg.WorkPath + cfg.JobRunLogFilename, runnerCtx)
	if err != nil {
		return cancel, err
	}

	jobRunner.LoadJobs(jobsConfig.Jobs)

	return cancel, connectDb()
}

func initHttpApi(logWriter *io.PipeWriter) *http.Server {
	router := mux.NewRouter()

	return &http.Server{
		Addr:         listenAddr,
		Handler:      router,
		ErrorLog:     stdLog.New(logWriter, "", 0),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}
}

func connectDb() error {
	if err := coreDb.Connect(cfg.CoreDbHost, cfg.CoreDbName, cfg.CoreDbUser, cfg.CoreDbPassword, cfg.CoreDbPort); err != nil {
		return err
	}

	return warehouseDb.Connect(cfg.DwDbHost, cfg.DwDbName, cfg.DwDbUser, cfg.DwDbPassword, cfg.DwDbPort)
}

func disconnectDb() error {
	if err := coreDb.Close(); err != nil {
		return err
	}

	return warehouseDb.Close()
}

func setupShutdownChannels() (chan bool, chan os.Signal) {
	done := make(chan bool, 1)
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	return done, exit
}

func gracefulShutdown(cancel context.CancelFunc, exitChannel chan os.Signal, doneChannel chan bool, server *http.Server) {
	<-exitChannel
	log.Println("Service is shutting down...")
	cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobRunner.Stop()

	server.SetKeepAlivesEnabled(false)
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Could not gracefully shutdown the service: %v\n", err)
	}

	if err := disconnectDb(); err != nil {
		log.Fatalf("Could not gracefully close the Db connection's: %v\n", err)
	}

	close(doneChannel)
}


