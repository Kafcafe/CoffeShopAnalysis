package middleware

import (
	"hash/fnv"
	"strconv"
)

func GetMessageId(data []byte, count int) (string, int) {
	h := fnv.New64a()
	h.Write(data)
	hashValue := h.Sum64()
	return strconv.FormatUint(hashValue, 16), int(hashValue%uint64(count)) + 1
}
