package main

import (
	"common/logger"
	"fmt"
)

func main() {
	// Initialize the logger first
	err := logger.InitGlobalLogger("INFO")
	if err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		return
	}

	log := logger.GetLoggerWithPrefix("[ZOMBIE_MAIN]")
	log.Info("")
	zombie := NewZoombie()
	zombie.BringBack("filter-amount1")
}
