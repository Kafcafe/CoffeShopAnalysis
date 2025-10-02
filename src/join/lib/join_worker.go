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

type JoinWorkerConfig struct {
	id              string
	count           int
	ofType          string
	prevStageSub    string
	sideTableSub    string
	nextStagePub    string
	messageCallback func(joiner *Join, sideTable []string, payload map[string][]string) (joinedItems []string)
}

func JoinItemsConfig(joinId string, joinCount int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:           joinId,
		count:        joinCount,
		ofType:       JOIN_ITEMS_TYPE,
		prevStageSub: "transactions.items.group.yearmonth",
		sideTableSub: "transactions.items.menu.items",
		nextStagePub: "results.q2",
		messageCallback: func(joiner *Join, sideTable []string, payload map[string][]string) (joinedItems []string) {
			flattenedItems := make([]string, 0)
			for yearMonth, items := range payload {
				for _, item := range items {
					flattenedItems = append(flattenedItems, fmt.Sprintf("%s,%s", yearMonth, item))
				}
			}
			return joiner.JoinByIndex(sideTable, flattenedItems, 1, 0, 1)
		},
	}
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
		config := JoinItemsConfig(joinerId, joinerCount)
		joinItemsWorker, err = NewJoinWorker(rabbitConf, config)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unknown joinItems type: %s", joinItemsType)
	}

	return &joinItemsWorker, nil
}
