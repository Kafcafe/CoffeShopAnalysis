package middleware

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

func GetMessageId(data []byte, count int) (string, int) {
	hashValue := sha256.Sum256(data)
	partitionKey := binary.BigEndian.Uint64(hashValue[:8])
	return fmt.Sprintf("%x", hashValue), int(partitionKey%uint64(count)) + 1
}
