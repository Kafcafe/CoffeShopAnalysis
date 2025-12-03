package crasher

import (
	"common/logger"
	"math/rand"
	"os"
	"time"

	"github.com/op/go-logging"
)

const FORCE_EXIT_PROBABILITY float64 = 0.00005 // 1 in 20000

type Crasher struct {
	rng     *rand.Rand
	logger  *logging.Logger
	enabled bool
}

func NewCrasher(enabled bool) *Crasher {
	seed := time.Now().UnixNano() ^
		int64(rand.Int63())

	return &Crasher{
		rng:     rand.New(rand.NewSource(seed)),
		logger:  logger.GetLoggerWithPrefix("[CRASHER]"),
		enabled: enabled,
	}
}

func (c *Crasher) ThrowDiceAndForceExit(message string) {
	if c.enabled && c.rng.Float64() < FORCE_EXIT_PROBABILITY {
		c.logger.Errorf("Force exit: %s", message)
		os.Exit(1)
	}
}
