package main

import (
	logger "common/logger"
	middleware "common/middleware"
	"common/watch_mesh"
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
	logger.Infof("Filter startup with: id: %s | filterCount: %d",
		v.GetString("filter.id"), v.GetInt("filter.count"),
	)

	logger.Infof("Config for filter by year: fromYear: %d | toYear: %d",
		v.GetInt("filter.year.fromYear"),
		v.GetInt("filter.year.toYear"),
	)

	logger.Infof("Config for filter by hour: fromHour: %d | toHour: %d",
		v.GetInt("filter.hour.fromHour"),
		v.GetInt("filter.hour.toHour"),
	)

	logger.Infof("Config for filter by amount: minAmount: %f",
		v.GetFloat64("filter.amount.minAmount"),
	)

	logger.Infof("RabbitMQ configuration: host: %s | port: %d | username: %s | password: %s",
		v.GetString("rabbitmq.host"),
		v.GetInt("rabbitmq.port"),
		v.GetString("rabbitmq.user"),
		v.GetString("rabbitmq.pass"),
	)

	logger.Infof("WatchMesh configuration: port: %d | heartbeatInterval: %.2f secs | "+
		"heartbeatTimeout: %.2f secs | addressResolvingRetries: %d | "+
		"addressResolvingIntervalSeconds: %.2f | showHeartbeatLogs: %v | maxResurrectionAttempts: %d | randomSeedForJitter: %d | crasherEnabled: %v",
		v.GetInt("watch_mesh.udp.port"),
		v.GetFloat64("watchMesh.heartbeatIntervalSeconds"),
		v.GetFloat64("watchMesh.heartbeatTimeoutSeconds"),
		v.GetInt("watchMesh.addressResolvingRetries"),
		v.GetFloat64("watchMesh.addressResolvingIntervalSeconds"),
		v.GetBool("watchMesh.showHeartbeatLogs"),
		v.GetInt("watchMesh.maxResurrectionAttempts"),
		v.GetInt("watchMesh.randomSeedForJitter"),
		v.GetBool("watchMesh.crasher.enabled"),
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

	yearConfig := filters.YearFilterConfig{
		FromYear: config.GetInt("filter.year.fromYear"),
		ToYear:   config.GetInt("filter.year.toYear"),
	}

	hourConfig := filters.HourFilterConfig{
		FromHour: config.GetInt("filter.hour.fromHour"),
		ToHour:   config.GetInt("filter.hour.toHour"),
	}

	amountConfig := filters.AmountFilterConfig{
		MinAmount: config.GetFloat64("filter.amount.minAmount"),
	}

	filterId := config.GetString("filter.id")
	filterIdNum := config.GetInt("filter.idNum")
	filterCount := config.GetInt("filter.count")

	filterType := config.GetString("filter.type")

	watchMeshPort := config.GetInt("watch.mesh.udp.port")
	heartbeatIntervalSecs := config.GetFloat64("watchMesh.heartbeatIntervalSeconds")
	heartbeatTimeoutSecs := config.GetFloat64("watchMesh.heartbeatTimeoutSeconds")
	addressResolvingRetries := config.GetInt("watchMesh.addressResolvingRetries")
	addressResolvingIntervalSeconds := config.GetFloat64("watchMesh.addressResolvingIntervalSeconds")
	showHeartbeatLogs := config.GetBool("watchMesh.showHeartbeatLogs")
	maxResurrectionAttempts := config.GetInt("watchMesh.maxResurrectionAttempts")
	randomSeedForJitter := config.GetInt64("watchMesh.randomSeedForJitter")
	crasherEnabled := config.GetBool("watchMesh.crasher.enabled")
	crasherProb := config.GetFloat64("watchMesh.crasher.probability")

	basicWatchMeshConfig := watch_mesh.NewBasicWatchMeshConfig(
		watchMeshPort,
		heartbeatIntervalSecs,
		heartbeatTimeoutSecs,
		addressResolvingRetries,
		addressResolvingIntervalSeconds,
		showHeartbeatLogs,
		maxResurrectionAttempts,
		randomSeedForJitter,
		crasherEnabled,
		crasherProb,
	)

	filterWorker, err := filters.CreateFilterWorker(
		filterType,
		rabbitConf,
		yearConfig,
		hourConfig,
		amountConfig,
		filterId,
		filterIdNum,
		filterCount,
		basicWatchMeshConfig,
	)

	if err != nil {
		logger.Errorf("Failed creating new filter worker: %s", err)
		os.Exit(STARTUP_ERROR_EXIT_CODE)
	}

	err = (*filterWorker).Run()
	if err != nil {
		logger.Errorf("Failed creating new filter worker: %s", err)
		os.Exit(ERROR_DURING_PROCESSING_EXIT_CODE)
	}

	os.Exit(SUCCESS_EXIT_CODE)
}
