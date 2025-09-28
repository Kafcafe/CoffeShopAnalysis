package clientHandler

import (
	"strings"

	"github.com/google/uuid"
)

type ClientUuid struct {
	Full  string
	Short string
}

const (
	SHORT_UUID_LEN = 8
)

func NewClientUuid() ClientUuid {
	// UUIDv4 Random based
	newUuid, _ := uuid.NewRandom()
	newUuidStr := newUuid.String()

	return ClientUuid{
		Full:  newUuidStr,
		Short: shortenUuid(newUuidStr),
	}
}

func shortenUuid(longUUID string) string {
	noDashUuid := strings.ReplaceAll(longUUID, "-", "")

	if SHORT_UUID_LEN > len(noDashUuid) {
		return noDashUuid
	}

	return noDashUuid[:SHORT_UUID_LEN]
}
