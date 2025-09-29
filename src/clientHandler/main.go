package main

import (
	clientHandler "ClientHandler/lib"
	logger "common/logger"
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
	logger.Infof("ClientHandler up with configuration: ip: %s | port: %d | loglevel: %s",
		v.GetString("server.ip"),
		v.GetInt("server.port"),
		v.GetString("log.level"),
	)

	logger.Infof("Detected RabbitMQ configuration: host: %s | port: %d | username: %s | password: %s",
		v.GetString("rabbitmq.host"),
		v.GetInt("rabbitmq.port"),
		v.GetString("rabbitmq.user"),
		v.GetString("rabbitmq.pass"),
	)
}

// main is the entry point of the application.
// It initializes configuration, logger, and starts the server acceptor.
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

	// Create clientHandler configuration from the loaded config
	clientHandlerConfig := clientHandler.NewAcceptorConfig(
		config.GetString("server.ip"),
		config.GetInt("server.port"),
		config.GetString("rabbitmq.user"),
		config.GetString("rabbitmq.pass"),
		config.GetString("rabbitmq.host"),
		config.GetInt("rabbitmq.port"),
	)

	// Create acceptor without running it yet
	acceptor, err := clientHandler.NewAcceptor(clientHandlerConfig)
	if err != nil {
		logger.Error("Error creating acceptor: %v", err)
		os.Exit(STARTUP_ERROR_EXIT_CODE)
	}

	// Start acceptor to handle new connections until finished
	err = acceptor.Run()
	if err != nil {
		logger.Errorf("Error running acceptor: %v\n", err)
		os.Exit(ERROR_DURING_PROCESSING_EXIT_CODE)
	}

	logger.Info("ClinetHandler shutdown without errors")
	os.Exit(SUCCESS_EXIT_CODE)
}
