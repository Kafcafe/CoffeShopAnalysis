package main

import (
	logger "common/logger"
	middleware "common/middleware"
	filters "filters/lib"
	"fmt"
	"os"
	"strings"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

const (
	SUCCESS_EXIT_CODE                 = 0
	STARTUP_ERROR_EXIT_CODE           = 1
	ERROR_DURING_PROCESSING_EXIT_CODE = 2
)

// InitConfig initializes the application configuration using Viper.
// It reads from config.yaml and environment variables with CLI_ prefix.
// Returns the configured Viper instance or an error.
func InitConfig() (*viper.Viper, error) {

	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	//v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// PrintConfig logs the current server configuration details.
// Parameters:
//
//	v: the configuration instance
func PrintConfig(v *viper.Viper, logger *logging.Logger) {
	logger.Infof("Config for query 1: fromYear: %d | toYear: %d | fromHour: %d | toHour: %d",
		v.GetInt("filter.query1.fromYear"),
		v.GetInt("filter.query1.toYear"),
		v.GetInt("filter.query1.fromHour"),
		v.GetInt("filter.query1.toHour"),
	)

	logger.Infof("Config for query 2: fromYear: %d | toYear: %d",
		v.GetInt("filter.query2.fromYear"),
		v.GetInt("filter.query2.toYear"),
	)

	logger.Infof("Config for query 3: fromYear: %d | toYear: %d | fromHour: %d | toHour: %d",
		v.GetInt("filter.query3.fromYear"),
		v.GetInt("filter.query3.toYear"),
		v.GetInt("filter.query3.fromHour"),
		v.GetInt("filter.query3.toHour"),
	)

	logger.Infof("Config for query 4: fromYear: %d | toYear: %d",
		v.GetInt("filter.query4.fromYear"),
		v.GetInt("filter.query4.toYear"),
	)

	logger.Infof("Detected RabbitMQ configuration: host: %s | port: %d | username: %s | password: %s",
		v.GetString("rabbitmq.host"),
		v.GetInt("rabbitmq.port"),
		v.GetString("rabbitmq.user"),
		v.GetString("rabbitmq.pass"),
	)
}

func main() {
	config, err := InitConfig()
	if err != nil {
		fmt.Printf("Error initializing configuration: %v\n", err)
		return
	}

	err = logger.InitGlobalLogger(config.GetString("log.level"))
	if err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		return
	}

	logger := logger.GetLoggerWithPrefix("[MAIN]")

	PrintConfig(config, logger)

	rabbitConf := middleware.NewRabbitConfig(
		config.GetString("rabbitmq.user"),
		config.GetString("rabbitmq.pass"),
		config.GetString("rabbitmq.host"),
		config.GetInt("rabbitmq.port"),
	)

	query1 := filters.DatetimeFilterConfig{
		FromYear: config.GetInt("filter.query1.fromYear"),
		ToYear:   config.GetInt("filter.query1.toYear"),
		FromHour: config.GetInt("filter.query1.fromHour"),
		ToHour:   config.GetInt("filter.query1.toHour"),
	}

	query2 := filters.YearFilterConfig{
		FromYear: config.GetInt("filter.query2.fromYear"),
		ToYear:   config.GetInt("filter.query2.toYear"),
	}

	query3 := filters.DatetimeFilterConfig{
		FromYear: config.GetInt("filter.query3.fromYear"),
		ToYear:   config.GetInt("filter.query3.toYear"),
		FromHour: config.GetInt("filter.query3.fromHour"),
		ToHour:   config.GetInt("filter.query3.toHour"),
	}

	query4 := filters.YearFilterConfig{
		FromYear: config.GetInt("filter.query4.fromYear"),
		ToYear:   config.GetInt("filter.query4.toYear"),
	}

	filtersConfig := filters.NewFiltersConfig(query1, query2, query3, query4)

	filterWorker, err := filters.NewFilterWorker(rabbitConf, filtersConfig)
	if err != nil {
		logger.Errorf("Failed creating new filter worker: %s", err)
		os.Exit(STARTUP_ERROR_EXIT_CODE)
	}

	err = filterWorker.Run()
	if err != nil {
		logger.Errorf("Failed creating new filter worker: %s", err)
		os.Exit(ERROR_DURING_PROCESSING_EXIT_CODE)
	}

	os.Exit(SUCCESS_EXIT_CODE)
}
