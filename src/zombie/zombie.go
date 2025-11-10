package main

import (
	"common/logger"
	"os/exec"
	"strings"

	"github.com/op/go-logging"
)

type Zoombie struct {
	log *logging.Logger
}

func NewZoombie() *Zoombie {
	return &Zoombie{
		log: logger.GetLoggerWithPrefix("[ZOMBIE]"),
	}
}

func (z *Zoombie) BringBack(target string) {
	cmd := "docker start " + target
	z.executeCommand(cmd)
}

func (z *Zoombie) executeCommand(cmd string) {
	parts := strings.Split(cmd, " ")
	println(parts)
	if len(parts) == 0 {
		z.log.Warningf("Command is empty")
		return
	}
	command := exec.Command(parts[0], parts[1:]...)
	output, err := command.CombinedOutput()
	if err != nil {
		z.log.Errorf("Error executing command: %v. Output: %s", err, strings.TrimSpace(string(output)))
		return
	}
	z.log.Infof("Command executed successfully: %s", output)
}
