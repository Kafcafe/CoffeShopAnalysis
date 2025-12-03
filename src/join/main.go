package main

import (
	logger "common/logger"
	middleware "common/middleware"
	"common/watch_mesh"
	"fmt"
	join "join/lib"
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
	logger.Infof("Join startup with: type %s | id: %s | JoinCount : %d",
		v.GetString("join.type"), v.GetString("join.id"), v.GetInt("join.count"),
	)

	logger.Infof("Detected RabbitMQ configuration: host: %s | port: %d | username: %s | password: %s",
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

	joinerId := config.GetString("join.id")
	joinerIdNum := config.GetInt("join.idnum")
	joinerCount := config.GetInt("join.count")
	joinerType := config.GetString("join.type")

	watchMeshPort := config.GetInt("watch.mesh.udp.port")
	heartbeatIntervalSecs := config.GetFloat64("watchMesh.heartbeatIntervalSeconds")
	heartbeatTimeoutSecs := config.GetFloat64("watchMesh.heartbeatTimeoutSeconds")
	addressResolvingRetries := config.GetInt("watchMesh.addressResolvingRetries")
	addressResolvingIntervalSeconds := config.GetFloat64("watchMesh.addressResolvingIntervalSeconds")
	showHeartbeatLogs := config.GetBool("watchMesh.showHeartbeatLogs")
	maxResurrectionAttempts := config.GetInt("watchMesh.maxResurrectionAttempts")
	randomSeedForJitter := config.GetInt64("watchMesh.randomSeedForJitter")
	crasherEnabled := config.GetBool("watchMesh.crasher.enabled")

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
	)

	joinItemsWorker, err := join.CreateJoinerWorker(
		joinerType,
		rabbitConf,
		joinerId,
		joinerIdNum,
		joinerCount,
		basicWatchMeshConfig,
		crasherEnabled,
	)

	if err != nil {
		logger.Errorf("Failed creating new joiner worker: %s", err)
		os.Exit(STARTUP_ERROR_EXIT_CODE)
	}

	err = (*joinItemsWorker).Run()
	if err != nil {
		logger.Errorf("Failed creating new joiner worker: %s", err)
		os.Exit(ERROR_DURING_PROCESSING_EXIT_CODE)
	}

	os.Exit(SUCCESS_EXIT_CODE)
}
