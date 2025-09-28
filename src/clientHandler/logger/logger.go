package logger

import (
	"os"

	"github.com/op/go-logging"
)

var (
	initialized bool
	backend     logging.Backend
)

// Init initializes the logging backend and formatter
func InitGlobalLogger(logLevel string) error {
	if initialized {
		return nil
	}

	backend = logging.NewLogBackend(os.Stderr, "", 0)

	// %{module} will be the prefix set in logging.MustGetLogger(prefix)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05.000} [%{color}%{level:.5s}%{color:reset}] %{module}: %{message}`,
	)

	backendFormatter := logging.NewBackendFormatter(backend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}

	backendLeveled.SetLevel(logLevelCode, "")

	logging.SetBackend(backendLeveled)

	initialized = true
	return nil
}

// GetLogger returns a new logger with its own prefix (per module)
func GetLoggerWithPrefix(prefix string) *logging.Logger {
	// Create a logger with the given module name (prefix)
	return logging.MustGetLogger(prefix)
}
