package join

import (
	"common/middleware"
	"fmt"
)

const (
	JOIN_ITEMS_TYPE    = "items"
	JOIN_STORE_TYPE    = "store"
	JOIN_STORE_Q3_TYPE = "store_q3"
	JOIN_USERS_TYPE    = "users"
)

type JoinItemsWorker interface {
	Run() error
}

type JoinWorkerConfig struct {
	id                             string
	count                          int
	ofType                         string
	queryId                        int
	prevStageSub                   string
	sideTableSub                   string
	nextStagePubs                  map[string]string
	flattenPayload                 func(payload map[string][]string) (flattenedPayload []string)
	joinTables                     func(joiner *Join, sideTable []string, mainTable []string) (joinedItems []string)
	messageCallbackUpdateSideTable func(sideTable []string, payload []string) (updatedSideTable []string)
}

func JoinItemsConfig(joinId string, joinCount int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:            joinId,
		count:         joinCount,
		ofType:        JOIN_ITEMS_TYPE,
		queryId:       2,
		prevStageSub:  "transactions.items.group.yearmonth",
		sideTableSub:  "transactions.items.menu.items",
		nextStagePubs: map[string]string{}, // Empty because it is generated at runtime as results.clientUUID
		flattenPayload: func(payload map[string][]string) (flattenedPayload []string) {
			flattenedItems := make([]string, 0)
			for yearMonth, items := range payload {
				for _, item := range items {
					flattenedItems = append(flattenedItems, fmt.Sprintf("%s,%s", yearMonth, item))
				}
			}
			return flattenedItems
		},
		joinTables: func(joiner *Join, sideTable []string, mainTable []string) (joinedItems []string) {
			return joiner.JoinByIndex(sideTable, mainTable, 1, 0, 1)
		},
	}
}

func JoinStoreConfig(joinId string, joinCount int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:           joinId,
		count:        joinCount,
		ofType:       "store",
		prevStageSub: "transactions.transactions.topk",
		sideTableSub: "transactions.store",
		nextStagePubs: map[string]string{
			JOIN_STORE_TYPE: "transactions.transactions.join.store",
		},
		flattenPayload: func(payload map[string][]string) (flattenedPayload []string) {
			flattenedStores := make([]string, 0)
			for store, users := range payload {
				for _, user := range users {
					flattenedStores = append(flattenedStores, fmt.Sprintf("%s,%s", store, user))
				}
			}
			return flattenedStores
		},
		joinTables: func(joiner *Join, sideTable []string, mainTable []string) (joinedItems []string) {
			return joiner.JoinByIndex(sideTable, mainTable, 1, 0, 0)
		},
	}
}

func JoinStoreQ3Config(joinId string, joinCount int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:            joinId,
		count:         joinCount,
		ofType:        "store_q3",
		queryId:       3,
		prevStageSub:  "transactions.transactions.group.semester",
		sideTableSub:  "transactions.store",
		nextStagePubs: map[string]string{}, // Empty because it is generated at runtime as results.clientUUID
		flattenPayload: func(payload map[string][]string) (flattenedPayload []string) {
			flattenedStores := make([]string, 0)
			for semester, storesAndTPV := range payload {
				for _, storeAndTPV := range storesAndTPV {
					flattenedStores = append(flattenedStores, fmt.Sprintf("%s,%s", semester, storeAndTPV))
				}
			}
			return flattenedStores
		},
		joinTables: func(joiner *Join, sideTable []string, mainTable []string) (joinedItems []string) {
			return joiner.JoinByIndex(sideTable, mainTable, 1, 0, 1)
		},
	}
}

func JoinUsersConfig(joinId string, joinCount int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:                             joinId,
		count:                          joinCount,
		ofType:                         "users",
		queryId:                        4,
		prevStageSub:                   "transactions.users",
		sideTableSub:                   "transactions.transactions.join.store",
		nextStagePubs:                  map[string]string{}, // Empty because it is generated at runtime as results.clientUUID
		messageCallbackUpdateSideTable: UpdatedSideTableWithUsers,
	}
}

func CreateJoinerWorker(joinItemsType string,
	rabbitConf middleware.RabbitConfig,
	joinerId string,
	joinerCount int,
) (*JoinItemsWorker, error) {

	var joinItemsWorker JoinItemsWorker
	var err error
	var config JoinWorkerConfig

	switch joinItemsType {
	case JOIN_ITEMS_TYPE:
		config = JoinItemsConfig(joinerId, joinerCount)
	case JOIN_STORE_TYPE:
		config = JoinStoreConfig(joinerId, joinerCount)
	case JOIN_USERS_TYPE:
		config = JoinUsersConfig(joinerId, joinerCount)
	case JOIN_STORE_Q3_TYPE:
		config = JoinStoreQ3Config(joinerId, joinerCount)
	default:
		return nil, fmt.Errorf("unknown joiner type: %s", joinItemsType)
	}
	joinItemsWorker, err = NewJoinWorker(rabbitConf, config)
	if err != nil {
		return nil, err
	}

	return &joinItemsWorker, nil
}
