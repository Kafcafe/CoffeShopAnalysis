package middleware

import (
	"fmt"
	"testing"
)

func TestCache_AddAndContains(t *testing.T) {
	cache := NewCache(3)

	cache.Add("a")
	if !cache.Contains("a") {
		t.Errorf("Expected cache to contain 'a'")
	}

	cache.Add("b")
	if !cache.Contains("b") {
		t.Errorf("Expected cache to contain 'b'")
	}

	cache.Add("c")
	if !cache.Contains("c") {
		t.Errorf("Expected cache to contain 'c'")
	}
}

func TestCache_Eviction(t *testing.T) {
	cache := NewCache(2)

	cache.Add("a")
	cache.Add("b")
	cache.Add("c")

	if cache.Contains("a") {
		t.Errorf("Expected 'a' to be evicted")
	}
	if !cache.Contains("b") {
		t.Errorf("Expected 'b' to remain")
	}
	if !cache.Contains("c") {
		t.Errorf("Expected 'c' to be added")
	}
}

func TestCache_Idempotency(t *testing.T) {
	cache := NewCache(2)

	cache.Add("a")
	cache.Add("b")
	cache.Add("a")

	if !cache.Contains("a") {
		t.Errorf("Expected 'a' to remain")
	}
	if !cache.Contains("b") {
		t.Errorf("Expected 'b' to remain")
	}

	cache.Add("c")
	if cache.Contains("a") {
		t.Errorf("Expected 'a' to be evicted")
	}
	if !cache.Contains("c") {
		t.Errorf("Expected 'c' to be added")
	}
}

func TestCache_Rotation(t *testing.T) {
	capacity := 10
	cache := NewCache(capacity)

	// Fill cache
	for i := 0; i < capacity; i++ {
		cache.Add(fmt.Sprintf("%d", i))
	}

	// Rotate
	iterations := 1000
	for i := capacity; i < capacity+iterations; i++ {
		newKey := fmt.Sprintf("%d", i)
		oldestKey := fmt.Sprintf("%d", i-capacity)

		cache.Add(newKey)

		if !cache.Contains(newKey) {
			t.Errorf("Expected new key '%s' to be present", newKey)
		}

		if cache.Contains(oldestKey) {
			t.Errorf("Expected oldest key '%s' to be evicted", oldestKey)
		}
	}
}
