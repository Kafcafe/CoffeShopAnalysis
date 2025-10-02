package main

import (
	logger "common/logger"
	middleware "common/middleware"
	"fmt"
	"os"
	"strings"
	topk "topk/lib"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

const (
	SUSSESS_EXIT_CODE      = 0
	FAIL_EXIT_CODE         = 1
	ERROR_DURING_EXIT_CODE = 2
)

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
	logger.Infof("ClientHandler up with configuration: loglevel: %s | Ktop: %d",
		v.GetString("log.level"),
		v.GetInt("k"),
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

	topKNodeId := config.GetString("topk.id")
	Kconfig := config.GetInt("k")
	totalNodes := config.GetInt("nodes")

	if Kconfig <= 0 {
		logger.Errorf("K must be a positive integer. Current value: %d", Kconfig)
		return
	}

	topKWorker, err := topk.NewTopKWorker(Kconfig, totalNodes, topKNodeId, rabbitConf)

	if err != nil {
		logger.Errorf("Error initializing TopKWorker: %v", err)
		os.Exit(FAIL_EXIT_CODE)
	}

	err = topKWorker.Run()

	if err != nil {
		logger.Errorf("Error running TopKWorker: %v", err)
		os.Exit(FAIL_EXIT_CODE)
	}

}
