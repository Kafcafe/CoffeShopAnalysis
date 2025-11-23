package middleware

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

func GetMessageId(data []byte, count int) (string, int) {
	hashValue := sha256.Sum256(data)
	partitionKey := binary.BigEndian.Uint64(hashValue[:8])
	return hex.EncodeToString(hashValue[:]), int(partitionKey%uint64(count)) + 1
}
