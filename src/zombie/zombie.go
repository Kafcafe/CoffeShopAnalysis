package main

import (
	"os/exec"
	"strings"
)

type Zoombie struct{}

func NewZoombie() *Zoombie {
	return &Zoombie{}
}

func (z *Zoombie) BringBack(target string) {
	cmd := "docker compose -f docker-compose-dev.yaml run " + target
	z.executeCommand(cmd)
}

func (z *Zoombie) executeCommand(cmd string) {
	// Implementation to execute the command
	parts := strings.Split(cmd, " ")
	println(parts)
	if len(parts) == 0 {
		return
	}
	command := exec.Command(parts[0], parts[1:]...)
	output, err := command.CombinedOutput()
	if err != nil {
		// Handle error
		println("Error executing command:", err.Error())
	}
	println(output)
}
