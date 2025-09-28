package logger

import (
	"os"

	"github.com/op/go-logging"
)

var (
	log         = logging.MustGetLogger("default")
	initialized bool
	backend     logging.Backend
)

// Init initializes the logging backend and formatter
func InitGlobalLogger(logLevel string) error {
	if initialized {
		return nil
	}

	// Create backend (writes to stderr)
	backend = logging.NewLogBackend(os.Stderr, "", 0)

	// Format with prefix support
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05.000} [%{color} %{level:.5s} %{color:reset}] %{message}`,
	)

	backendFormatter := logging.NewBackendFormatter(backend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}

	backendLeveled.SetLevel(logLevelCode, "")

	logging.SetBackend(backendFormatter)

	initialized = true
	return nil
}

// GetLogger returns a new logger with its own prefix (per module)
func GetLoggerWithPrefix(prefix string) *logging.Logger {
	// Create a logger with the given module name (prefix)
	return logging.MustGetLogger(prefix)
}
