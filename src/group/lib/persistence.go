package group

import (
	"common/middleware"
	"fmt"
)

func (g *GroupByGenericWorker) dumpData(msg *middleware.Message, hash string, dataType DataType) error {
	if g.conf.ofType == GROUP_TYPE_TOPK {
		metadata := []string{msg.ClientId, hash, dataType}
		if err := g.atomicWritter.Write(msg.Payload, metadata); err != nil {
			return fmt.Errorf("error writing grouped data to file for client %s: %v", msg.ClientId, err)
		}
		return nil
	}

	toSave := g.group.Get(msg.ClientId, g.conf.factory).ToFullStringList()
	metadata := []string{msg.ClientId, dataType}
	if err := g.atomicWritter.Write(toSave, metadata); err != nil {
		return fmt.Errorf("error writing grouped data to file for client %s: %v", msg.ClientId, err)
	}

	return nil
}

func (g *GroupByGenericWorker) recover() {

	data, err := g.atomicWritter.Recover()

	if err != nil {
		g.log.Errorf("Error during recovery: %v", err)
		return
	}

	for clientId, data := range data {
		g.log.Infof("Recovering data for client %s", clientId)
		g.group.Add(clientId, data.GetData(), g.conf.factory)
	}

}
