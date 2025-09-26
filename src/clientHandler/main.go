package main

import (
	clientHandler "ClientHandler/common"
	"fmt"
	"os"
	"strings"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

func InitConfig() (*viper.Viper, error) {

	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./server.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05.000} %{level:.5s} %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

func PrintConfig(v *viper.Viper) {
	log.Infof("Server up with configuration: server ip %s, server port %d, log level %s",
		v.GetString("server.ip"),
		v.GetInt("server.port"),
		v.GetString("log.level"),
	)
}

func main() {

	config, err := InitConfig()
	if err != nil {
		fmt.Printf("Error initializing configuration: %v\n", err)
		return
	}

	err = InitLogger(config.GetString("log.level"))
	if err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		return
	}

	PrintConfig(config)

	serverConfigs := clientHandler.NewServerConfig(
		config.GetString("server.ip"),
		config.GetInt("server.port"),
	)

	acceptor, err := clientHandler.NewAcceptor(serverConfigs)
	if err != nil {
		log.Error("Error creating acceptor: %v", err)
		return
	}
	err = acceptor.Run()
	if err != nil {
		fmt.Printf("Error running acceptor: %v\n", err)
	}

}
