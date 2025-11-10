package respawner

import (
	"common/logger"
	"os/exec"
	"strings"

	"github.com/op/go-logging"
)

type Respawner struct {
	log *logging.Logger
}

func NewRespawner() *Respawner {
	return &Respawner{
		log: logger.GetLoggerWithPrefix("[RESPAWNER]"),
	}
}

func (rswp *Respawner) BringBack(target string) {
	cmd := "docker start " + target
	rswp.executeCommand(cmd)
}

func (rswp *Respawner) executeCommand(cmd string) {
	parts := strings.Split(cmd, " ")
	println(parts)
	if len(parts) == 0 {
		rswp.log.Warningf("Command is empty")
		return
	}
	command := exec.Command(parts[0], parts[1:]...)
	output, err := command.CombinedOutput()
	if err != nil {
		rswp.log.Errorf("Error executing command: %v. Output: %s", err, strings.TrimSpace(string(output)))
		return
	}
	rswp.log.Infof("Command executed successfully: %s", output)
}
