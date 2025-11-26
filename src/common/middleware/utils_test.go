package middleware

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestGetMessageIdDeterministic(t *testing.T) {
	data := []byte("hello world")
	count := 10

	id1, part1 := GetMessageId(data, count)
	id2, part2 := GetMessageId(data, count)

	if id1 != id2 {
		t.Fatalf("expected same id for same input, got %q and %q", id1, id2)
	}
	if part1 != part2 {
		t.Fatalf("expected same partition for same input, got %d and %d", part1, part2)
	}

	// verify id matches sha256 hex representation and partition calculation
	expectedHash := sha256.Sum256(data)
	expectedId := hex.EncodeToString(expectedHash[:])
	expectedPart := int(binary.BigEndian.Uint64(expectedHash[:8])%uint64(count)) + 1

	if id1 != expectedId {
		t.Fatalf("id mismatch: expected %s, got %s", expectedId, id1)
	}
	if part1 != expectedPart {
		t.Fatalf("partition mismatch: expected %d, got %d", expectedPart, part1)
	}
}

func TestGetMessageIdEmptyData(t *testing.T) {
	data := []byte{}
	count := 5

	id, part := GetMessageId(data, count)

	expectedHash := sha256.Sum256(data)
	expectedId := hex.EncodeToString(expectedHash[:])
	expectedPart := int(binary.BigEndian.Uint64(expectedHash[:8])%uint64(count)) + 1

	if id != expectedId {
		t.Fatalf("empty data id mismatch: expected %s, got %s", expectedId, id)
	}
	if part != expectedPart {
		t.Fatalf("empty data partition mismatch: expected %d, got %d", expectedPart, part)
	}
}

func TestGetMessageIdDifferentDataLikelyDifferent(t *testing.T) {
	a := []byte("first")
	b := []byte("First")
	count := 100

	idA, partA := GetMessageId(a, count)
	idB, partB := GetMessageId(b, count)

	// It's extremely likely they differ; if they don't, still ensure deterministic mapping
	if idA == idB && partA == partB {
		t.Fatalf("unexpected collision for test inputs: same id %q and partition %d", idA, partA)
	}
}

func TestGetMessageIdCountOne(t *testing.T) {
	dataSamples := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte(""),
		[]byte("some longer payload"),
	}
	for _, d := range dataSamples {
		_, part := GetMessageId(d, 1)
		if part != 1 {
			t.Fatalf("expected partition 1 when count=1, got %d for data %q", part, string(d))
		}
	}
}

func TestGetMessageIdPanicsOnZeroCount(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when count is zero, but function did not panic")
		}
	}()
	// This should panic due to modulo by zero
	GetMessageId([]byte("test"), 0)
}
