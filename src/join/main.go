package main

import (
	join "join/lib"
)

func main() {
	joiner := join.NewJoiner()
	if joiner == nil {
		panic("Joiner is nil")
	}
}
