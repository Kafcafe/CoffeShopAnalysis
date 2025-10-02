package join

import (
	"common/middleware"
	"fmt"
)

const (
	JOIN_ITEMS_TYPE = "items"
	JOIN_STORE_TYPE = "store"
	JOIN_USERS_TYPE = "users"
)

type JoinItemsWorker interface {
	Run() error
}

type JoinWorkerConfig struct {
	id                             string
	count                          int
	ofType                         string
	prevStageSub                   string
	sideTableSub                   string
	nextStagePub                   string
	messageCallback                func(joiner *Join, sideTable []string, payload map[string][]string) (joinedItems []string)
	messageCallbackUpdateSideTable func(sideTable []string, payload []string) (updatedSideTable []string)
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

func JoinStoreConfig(joinId string, joinCount int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:           joinId,
		count:        joinCount,
		ofType:       "store",
		prevStageSub: "transactions.transactions.topk", // TODO: change when defined -> Maybe ready
		sideTableSub: "transactions.store",
		nextStagePub: "transactions.transactions.join.store",
		messageCallback: func(joiner *Join, sideTable []string, payload map[string][]string) (joinedItems []string) {
			flattenedStores := make([]string, 0)
			for store, users := range payload {
				for _, user := range users {
					flattenedStores = append(flattenedStores, fmt.Sprintf("%s,%s", store, user))
				}
			}
			return joiner.JoinByIndex(sideTable, flattenedStores, 1, 0, 0)
		},
	}
}

func JoinUsersConfig(joinId string, joinCount int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:                             joinId,
		count:                          joinCount,
		ofType:                         "users",
		prevStageSub:                   "transactions.users", // TODO: change when defined
		sideTableSub:                   "transactions.transactions.join.store",
		nextStagePub:                   "results.q4",
		messageCallbackUpdateSideTable: UpdatedSideTableWithUsers,
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
	case JOIN_STORE_TYPE:
		config := JoinStoreConfig(joinerId, joinerCount)
		joinItemsWorker, err = NewJoinWorker(rabbitConf, config)
		if err != nil {
			return nil, err
		}
	case JOIN_USERS_TYPE:
		config := JoinUsersConfig(joinerId, joinerCount)
		joinItemsWorker, err = NewJoinWorker(rabbitConf, config)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unknown joinItems type: %s", joinItemsType)
	}

	return &joinItemsWorker, nil
}
