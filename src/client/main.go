package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	logger "common/logger"

	client "github.com/Kafcafe/CoffeShopAnalysis/client/lib"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

const (
	SUCCESS_EXIT_CODE       = 0
	STARTUP_ERROR_EXIT_CODE = 1
)

func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()

	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("log", "level")
	v.BindEnv("batch", "maxAmount")
	v.BindEnv("datapath", "folder")
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

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper, logger *logging.Logger) {
	logger.Infof("action: config | result: success | client_id: %s | server_address: %s | log_level: %s | batch_max_amount: %s | datapath_folder: %s",
		v.GetString("client.id"),
		v.GetString("server.address"),
		v.GetString("log.level"),
		v.GetString("batch.maxAmount"),
		v.GetString("datapath.folder"),
	)
}

func main() {
	config, err := InitConfig()
	if err != nil {
		fmt.Printf("action: init configs | result: error | Error initializing configuration: %v\n", err)
		return
	}

	err = logger.InitGlobalLogger(config.GetString("log.level"))
	if err != nil {
		fmt.Printf("action: init logger | result: error | Error initializing logger: %v\n", err)
		return
	}

	logger := logger.GetLoggerWithPrefix("[MAIN]")

	PrintConfig(config, logger)

	clientId := config.GetString("client.id")
	logger.Infof("Client %s started", clientId)

	filetypes := config.GetString("filetypes")

	clientConfig := client.NewClientConfig(
		config.GetString("server.address"),
		config.GetString("datapath.folder"),
		config.GetInt("batch.maxAmount"),
	)

	client := client.NewClient(clientConfig, clientId, filetypes)

	if client == nil {
		logger.Criticalf("| action: create client | result: error | client_id: %s | Client could not be created", clientId)
		os.Exit(STARTUP_ERROR_EXIT_CODE)
	}

	start := time.Now()

	if err := client.Run(); err != nil {
		logger.Criticalf("| action: run client | result: error | client_id: %s | Client execution failed: %s", clientId, err)
		os.Exit(STARTUP_ERROR_EXIT_CODE)
	}

	elapsed := time.Since(start)

	logger.Infof("| action: finish | result: success | client_id: %s | Client %s finished", clientId, clientId)
	logger.Infof("| action: log execution time | client_id: %s | Execution took %s\n", clientId, elapsed)

	os.Exit(SUCCESS_EXIT_CODE)
}
