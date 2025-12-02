package group

import (
	"common/middleware"
	"common/watch_mesh"
	"fmt"
	"group/structures"
	"time"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

const (
	GROUP_TYPE_YEARMONTH = "yearmonth"
	GROUP_TYPE_SEMESTER  = "semester"
	GROUP_TYPE_STORE     = "store"
	GROUP_TYPE_TOPK      = "topk"
)

type GroupByWorker interface {
	Run() error
}

type GroupByConfig struct {
	id           string
	count        int
	ofType       string
	prevStageSub string
	nextStagePub string
	factory      func() structures.AllowedGroup
	idNum        int
}

func GroupByYearMonthConfig(groupId string, groupCount, idNum int) GroupByConfig {
	return GroupByConfig{
		id:           groupId,
		count:        groupCount,
		ofType:       GROUP_TYPE_YEARMONTH,
		prevStageSub: "transactions.items",
		nextStagePub: "transactions.items.group.yearmonth",
		factory:      func() structures.AllowedGroup { return structures.NewYearMonthGroup() },
		idNum:        idNum,
	}
}

func GroupBySemesterConfig(groupId string, groupCount, idNum int) GroupByConfig {
	return GroupByConfig{
		id:           groupId,
		count:        groupCount,
		ofType:       GROUP_TYPE_SEMESTER,
		prevStageSub: "transactions.year-hour-filtered.all",
		nextStagePub: "transactions.transactions.group.semester",
		factory:      func() structures.AllowedGroup { return structures.NewSemesterGroup() },
		idNum:        idNum,
	}
}

func GroupByTopKConfig(groupId string, groupCount, k, idNum int) GroupByConfig {
	return GroupByConfig{
		id:           groupId,
		count:        groupCount,
		ofType:       GROUP_TYPE_TOPK,
		prevStageSub: "transactions.transactions.all",
		nextStagePub: "transactions.transactions.topk",
		factory:      func() structures.AllowedGroup { return structures.NewTopKStoreGroup(k) },
		idNum:        idNum,
	}
}

func createWatchMeshConfig(
	basicWatchMeshConfig watch_mesh.BasicWatchMeshConfig,
	id string,
	groupsCount int,
	ofType string,
) watch_mesh.WatchMeshConfig {
	// Prepare addresses for WatchMesh
	peerAddresses := []string{}
	myAddress := fmt.Sprintf("group%s", id)
	for i := 1; i < groupsCount+1; i++ {
		peerIp := fmt.Sprintf("group-%s%d", ofType, i)

		if peerIp != myAddress {
			peerAddresses = append(peerAddresses, peerIp)
		}
	}

	heartbeatIntervalSeconds := time.Duration(basicWatchMeshConfig.HeartbeatIntervalSeconds) * time.Second
	heartbeatTimeoutSeconds := time.Duration(basicWatchMeshConfig.HeartbeatTimeoutSeconds) * time.Second
	addressResolvingIntervalSeconds := time.Duration(basicWatchMeshConfig.AddressResolvingIntervalSeconds) * 1000 * time.Millisecond

	watchMeshConfig := watch_mesh.NewWatchMeshConfig(
		id,
		basicWatchMeshConfig.Port,
		peerAddresses,
		heartbeatIntervalSeconds,
		heartbeatTimeoutSeconds,
		basicWatchMeshConfig.AddressResolvingRetries,
		addressResolvingIntervalSeconds,
		basicWatchMeshConfig.ShowHeartbeatLogs,
		"group",
		basicWatchMeshConfig.MaxResurrectionAttempts,
		basicWatchMeshConfig.RandomSeedForJitter,
	)

	return watchMeshConfig
}

func CreateGroupByWorker(
	groupType string,
	rabbitConf middleware.RabbitConfig,
	groupId string,
	groupCount int,
	envConfig *viper.Viper,
	logger *logging.Logger,
	basicWatchMeshConfig watch_mesh.BasicWatchMeshConfig,
	idNum int,
) (*GroupByWorker, error) {
	var groupByWorker GroupByWorker
	var err error

	logger.Infof("Creating groupBy worker for group %d", idNum)

	switch groupType {
	case GROUP_TYPE_YEARMONTH:
		config := GroupByYearMonthConfig(groupId, groupCount, idNum)
		watchMeshConfig := createWatchMeshConfig(basicWatchMeshConfig, config.id, config.count, config.ofType)
		groupByWorker, err = NewGroupByGenericWorker(rabbitConf, config, watchMeshConfig)
		if err != nil {
			return nil, err
		}

	case GROUP_TYPE_SEMESTER:
		config := GroupBySemesterConfig(groupId, groupCount, idNum)
		watchMeshConfig := createWatchMeshConfig(basicWatchMeshConfig, config.id, config.count, config.ofType)
		groupByWorker, err = NewGroupByGenericWorker(rabbitConf, config, watchMeshConfig)
		if err != nil {
			return nil, err
		}

	case GROUP_TYPE_TOPK:
		Kconfig := envConfig.GetInt("k")
		logger.Infof("GroupBy type %s using k: %d", GROUP_TYPE_TOPK, Kconfig)
		config := GroupByTopKConfig(groupId, groupCount, Kconfig, idNum)
		watchMeshConfig := createWatchMeshConfig(basicWatchMeshConfig, config.id, config.count, config.ofType)
		groupByWorker, err = NewGroupByGenericWorker(rabbitConf, config, watchMeshConfig)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unknown groupBy type: %s", groupType)
	}

	return &groupByWorker, nil
}
