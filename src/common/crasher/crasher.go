package crasher

import (
	"common/logger"
	"hash/fnv"
	"math/rand"
	"os"
	"time"

	"github.com/op/go-logging"
)

const FORCE_EXIT_PROBABILITY float64 = 0.00005 // 1 in 20000

type Crasher struct {
	rng      *rand.Rand
	nodeType string
	id       string
	logger   *logging.Logger
	enabled  bool
}

func NewCrasher(nodeType, id string, enabled bool) *Crasher {
	seed := time.Now().UnixNano() ^
		int64(rand.Int63()) ^
		int64(hashString(nodeType+id))

	return &Crasher{
		rng:      rand.New(rand.NewSource(seed)),
		nodeType: nodeType,
		id:       id,
		logger:   logger.GetLoggerWithPrefix("[CRASHER]"),
		enabled:  enabled,
	}
}

func (c *Crasher) ThrowDiceAndForceExit(message string) {
	if c.enabled && c.rng.Float64() < FORCE_EXIT_PROBABILITY {
		c.logger.Errorf("[%s%s] Force exit: %s", c.nodeType, c.id, message)
		os.Exit(1)
	}
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
