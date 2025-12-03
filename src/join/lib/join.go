package join

import (
	"common/middleware"
	"common/watch_mesh"
	"fmt"
	"time"
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
	idNum                          int
	count                          int
	ofType                         string
	queryId                        int
	prevStageSub                   string
	sideTableSub                   string
	nextStagePubs                  map[string]string
	joinTables                     func(joiner *Join, sideTable []string, mainTable []string) (joinedItems []string)
	messageCallbackUpdateSideTable func(sideTable []string, payload []string) (updatedSideTable []string)
	crasherEnabled                 bool
}

func JoinItemsConfig(joinId string, joinCount, joinIdNum int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:            joinId,
		idNum:         joinIdNum,
		count:         joinCount,
		ofType:        JOIN_ITEMS_TYPE,
		queryId:       2,
		prevStageSub:  "transactions.items.group.yearmonth",
		sideTableSub:  "transactions.items.menu.items",
		nextStagePubs: map[string]string{}, // Empty because it is generated at runtime as results.clientUUID
		joinTables: func(joiner *Join, sideTable []string, mainTable []string) (joinedItems []string) {
			return joiner.JoinByIndex(sideTable, mainTable, 1, 0, 1)
		},
	}
}

func JoinStoreConfig(joinId string, joinCount, joinIdNum int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:           joinId,
		idNum:        joinIdNum,
		count:        joinCount,
		ofType:       "store",
		prevStageSub: "transactions.transactions.topk",
		sideTableSub: "transactions.store",
		nextStagePubs: map[string]string{
			JOIN_STORE_TYPE: "transactions.transactions.join.store",
		},
		joinTables: func(joiner *Join, sideTable []string, mainTable []string) (joinedItems []string) {
			return joiner.JoinByIndex(sideTable, mainTable, 1, 0, 0)
		},
	}
}

func JoinStoreQ3Config(joinId string, joinCount, joinIdNum int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:            joinId,
		idNum:         joinIdNum,
		count:         joinCount,
		ofType:        "store_q3",
		queryId:       3,
		prevStageSub:  "transactions.transactions.group.semester",
		sideTableSub:  "transactions.store",
		nextStagePubs: map[string]string{}, // Empty because it is generated at runtime as results.clientUUID
		joinTables: func(joiner *Join, sideTable []string, mainTable []string) (joinedItems []string) {
			return joiner.JoinByIndex(sideTable, mainTable, 1, 0, 1)
		},
	}
}

func JoinUsersConfig(joinId string, joinCount, joinIdNum int) JoinWorkerConfig {
	return JoinWorkerConfig{
		id:                             joinId,
		idNum:                          joinIdNum,
		count:                          joinCount,
		ofType:                         "users",
		queryId:                        4,
		prevStageSub:                   "transactions.users",
		sideTableSub:                   "transactions.transactions.join.store",
		nextStagePubs:                  map[string]string{}, // Empty because it is generated at runtime as results.clientUUID
		messageCallbackUpdateSideTable: UpdatedSideTableWithUsers,
	}
}

func CreateJoinerWorker(
	joinItemsType string,
	rabbitConf middleware.RabbitConfig,
	joinerId string,
	joinerIdNum int,
	joinerCount int,
	basicWatchMeshConfig watch_mesh.BasicWatchMeshConfig,
	crasherEnabled bool,
) (*JoinItemsWorker, error) {

	var joinItemsWorker JoinItemsWorker
	var err error
	var config JoinWorkerConfig

	switch joinItemsType {
	case JOIN_ITEMS_TYPE:
		config = JoinItemsConfig(joinerId, joinerCount, joinerIdNum)
	case JOIN_STORE_TYPE:
		config = JoinStoreConfig(joinerId, joinerCount, joinerIdNum)
	case JOIN_USERS_TYPE:
		config = JoinUsersConfig(joinerId, joinerCount, joinerIdNum)
	case JOIN_STORE_Q3_TYPE:
		config = JoinStoreQ3Config(joinerId, joinerCount, joinerIdNum)
	default:
		return nil, fmt.Errorf("unknown joiner type: %s", joinItemsType)
	}

	config.crasherEnabled = crasherEnabled

	// Prepare addresses for WatchMesh
	peerAddresses := []string{}
	myAddress := fmt.Sprintf("join%s", config.id)
	for i := 1; i < config.count+1; i++ {
		peerIp := fmt.Sprintf("join-%s%d", config.ofType, i)

		if peerIp != myAddress {
			peerAddresses = append(peerAddresses, peerIp)
		}
	}

	heartbeatIntervalSeconds := time.Duration(basicWatchMeshConfig.HeartbeatIntervalSeconds) * time.Second
	heartbeatTimeoutSeconds := time.Duration(basicWatchMeshConfig.HeartbeatTimeoutSeconds) * time.Second
	addressResolvingIntervalSeconds := time.Duration(basicWatchMeshConfig.AddressResolvingIntervalSeconds) * 1000 * time.Millisecond

	watchMeshConfig := watch_mesh.NewWatchMeshConfig(
		config.id,
		basicWatchMeshConfig.Port,
		peerAddresses,
		heartbeatIntervalSeconds,
		heartbeatTimeoutSeconds,
		basicWatchMeshConfig.AddressResolvingRetries,
		addressResolvingIntervalSeconds,
		basicWatchMeshConfig.ShowHeartbeatLogs,
		"join",
		basicWatchMeshConfig.MaxResurrectionAttempts,
		basicWatchMeshConfig.RandomSeedForJitter,
	)

	joinItemsWorker, err = NewJoinWorker(rabbitConf, config, watchMeshConfig)
	if err != nil {
		return nil, err
	}

	return &joinItemsWorker, nil
}
