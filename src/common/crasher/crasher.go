package crasher

import (
	"common/logger"
	"crypto/rand"
	"encoding/binary"
	mathrand "math/rand"
	"os"
)

const FORCE_EXIT_PROBABILITY = 0.0001 //4 // 0.004% per event

type Crasher struct {
	rng     *mathrand.Rand
	enabled bool
}

func NewCrasher(enabled bool) *Crasher {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic("failed to read crypto randomness for RNG seed: " + err.Error())
	}

	seed := int64(binary.LittleEndian.Uint64(b[:]))

	return &Crasher{
		rng:     mathrand.New(mathrand.NewSource(seed)),
		enabled: enabled,
	}
}

func (c *Crasher) ThrowDiceAndForceExit(allowedToCrash bool, message string) {
	if !c.enabled {
		return
	}

	if c.rng.Float64() < FORCE_EXIT_PROBABILITY {
		log := logger.GetLoggerWithPrefix("[CRASHER]")
		if !allowedToCrash {
			log.Warning("Not allowed to crash now, skipping forced exit")
			return
		}
		log.Errorf("Force exit: %s", message)
		os.Exit(1)
	}
}
