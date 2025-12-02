package respawner

import (
	"common/logger"
	"os/exec"
	"strings"

	"github.com/op/go-logging"
)

// Respawner is responsible for restarting docker containers.
type Respawner struct {
	log *logging.Logger
}

// NewRespawner creates a new instance of Respawner with a configured logger.
func NewRespawner() *Respawner {
	return &Respawner{
		log: logger.GetLoggerWithPrefix("[RESPAWNER]"),
	}
}

// Respawn starts the specified docker container.
func (rswp *Respawner) Respawn(target string) error {
	return rswp.executeCommand("docker", "start", target)
}

// Executes a test command (docker ps -a) to verify functionality.
func (rswp *Respawner) TestCommand() error {
	return rswp.executeCommand("docker", "ps", "-a")
}

// Executes a system command with the given name and arguments.
func (rswp *Respawner) executeCommand(name string, args ...string) error {
	rswp.log.Infof("Executing command: %s %s", name, strings.Join(args, " "))

	command := exec.Command(name, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		rswp.log.Errorf("Error executing command: %v. Output: %s", err, strings.TrimSpace(string(output)))
		return err
	}

	rswp.log.Infof("Command executed successfully: %s", output)
	return nil
}
