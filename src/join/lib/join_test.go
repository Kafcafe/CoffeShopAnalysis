package join_test

import (
	join "join/lib"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinerIsNotNil(t *testing.T) {
	joiner := join.NewJoiner()
	require.NotNil(t, joiner, "Expected NewJoiner to return a non-nil Join instance")
}
