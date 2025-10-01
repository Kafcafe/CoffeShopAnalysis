package join

import (
	"common/middleware"
	"fmt"
)

const (
	JOIN_ITEMS_TYPE = "items"
)

type JoinItemsWorker interface {
	Run() error
}

func CreateJoinItemsWorker(joinItemsType string,
	rabbitConf middleware.RabbitConfig,
	joinerId string,
	joinerCount int,
) (*JoinItemsWorker, error) {

	var joinItemsWorker JoinItemsWorker
	var err error

	switch joinItemsType {
	case JOIN_ITEMS_TYPE:
		joinItemsWorker, err = NewJoinItemsWorker(rabbitConf, joinerId, joinerCount)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unknown joinItems type: %s", joinItemsType)
	}

	return &joinItemsWorker, nil
}
