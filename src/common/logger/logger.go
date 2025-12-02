package logger

import (
	"os"

	"github.com/op/go-logging"
)

var (
	initialized bool
	backend     logging.Backend
)

func initGlobalLoggerInternal(logLevel string, format logging.Formatter) error {
	if initialized {
		return nil
	}

	backend = logging.NewLogBackend(os.Stderr, "", 0)

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

// Init initializes the logging backend and formatter
func InitGlobalLogger(logLevel string) error {
	if initialized {
		return nil
	}

	// %{module} will be the prefix set in logging.MustGetLogger(prefix)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05.000} %{color}%{level:.5s}%{color:reset} %{module}: %{message}`,
	)

	return initGlobalLoggerInternal(logLevel, format)
}

func InitGlobalLoggerWithShortfile(logLevel string) error {
	if initialized {
		return nil
	}

	// %{module} will be the prefix set in logging.MustGetLogger(prefix)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05.000} %{color}%{level:.5s}%{color:reset} %{shortfile} %{module}: %{message}`,
	)

	return initGlobalLoggerInternal(logLevel, format)
}

// GetLogger returns a new logger with its own prefix (per module)
func GetLoggerWithPrefix(prefix string) *logging.Logger {
	// Create a logger with the given module name (prefix)
	return logging.MustGetLogger(prefix)
}
