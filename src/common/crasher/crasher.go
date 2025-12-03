package crasher

import (
	"common/logger"
	"crypto/rand"
	"encoding/binary"
	mathrand "math/rand"
	"os"
	"time"
)

const FORCE_EXIT_PROBABILITY = 0.0005 // 0.05% chance to crash on each check if slot is allowed

type Crasher struct {
	id      int
	rng     *mathrand.Rand
	enabled bool
}

func NewCrasher(id int, enabled bool) *Crasher {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic("failed to read crypto randomness for RNG seed: " + err.Error())
	}

	seed := int64(binary.LittleEndian.Uint64(b[:]))

	return &Crasher{
		id:      id,
		rng:     mathrand.New(mathrand.NewSource(seed)),
		enabled: enabled,
	}
}

func (c *Crasher) ThrowDiceAndForceExit(message string) {
	if !c.enabled {
		return
	}

	if c.rng.Float64() < FORCE_EXIT_PROBABILITY {
		log := logger.GetLoggerWithPrefix("[CRASHER]")
		if !c.allowedToCrash() {
			log := logger.GetLoggerWithPrefix("[CRASHER]")
			log.Warning("Not allowed slot to crash now, skipping forced exit")
			return
		}
		log.Errorf("Force exit: %s", message)
		os.Exit(1)
	}
}

// allowedToCrash determines if the node is allowed to crash based on its ID
// and the current second of the minute.
func (c *Crasher) allowedToCrash() bool {
	sec := time.Now().Second()
	slot := c.id % 3
	switch slot {
	case 0:
		// slot 0: seconds < 10
		return sec <= 10
	case 1:
		// slot 1: seconds in [20,30]
		return sec >= 20 && sec <= 30
	default:
		// slot 2: seconds in [40,50]
		return sec >= 40 && sec <= 50
	}
}
