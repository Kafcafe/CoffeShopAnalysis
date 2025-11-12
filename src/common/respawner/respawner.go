package respawner

import (
	"common/logger"
	"fmt"
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

func (rswp *Respawner) Respawn(target string) error {
	cmd := "docker start " + target
	return rswp.executeCommand(cmd)
}

func (rswp *Respawner) TestCommand() error {
	cmd := "docker ps -a"
	return rswp.executeCommand(cmd)
}

func (rswp *Respawner) executeCommand(cmd string) error {
	parts := strings.Split(cmd, " ")
	rswp.log.Infof("Executing command: %s", cmd)
	if len(parts) == 0 {
		rswp.log.Warningf("Command is empty")
		return fmt.Errorf("no command was executed")
	}
	command := exec.Command(parts[0], parts[1:]...)
	output, err := command.CombinedOutput()
	if err != nil {
		rswp.log.Errorf("Error executing command: %v. Output: %s", err, strings.TrimSpace(string(output)))
		return err
	}
	rswp.log.Infof("Command executed successfully: %s", output)
	return nil
}
