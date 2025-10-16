package group

import (
	"common/middleware"
	"fmt"
	"group/structures"

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
}

func GroupByYearMonthConfig(groupId string, groupCount int) GroupByConfig {
	return GroupByConfig{
		id:           groupId,
		count:        groupCount,
		ofType:       GROUP_TYPE_YEARMONTH,
		prevStageSub: "transactions.items",
		nextStagePub: "transactions.items.group.yearmonth",
		factory:      func() structures.AllowedGroup { return structures.NewYearMonthGroup() },
	}
}

func GroupBySemesterConfig(groupId string, groupCount int) GroupByConfig {
	return GroupByConfig{
		id:           groupId,
		count:        groupCount,
		ofType:       GROUP_TYPE_SEMESTER,
		prevStageSub: "transactions.year-hour-filtered.all",
		nextStagePub: "transactions.transactions.group.semester",
		factory:      func() structures.AllowedGroup { return structures.NewSemesterGroup() },
	}
}

func GroupByTopKConfig(groupId string, groupCount int, k int) GroupByConfig {
	return GroupByConfig{
		id:           groupId,
		count:        groupCount,
		ofType:       GROUP_TYPE_TOPK,
		prevStageSub: "transactions.transactions.all",
		nextStagePub: "transactions.transactions.topk",
		factory:      func() structures.AllowedGroup { return structures.NewTopKStoreGroup(k) },
	}
}

func CreateGroupByWorker(groupType string,
	rabbitConf middleware.RabbitConfig,
	groupId string,
	groupCount int,
	envConfig *viper.Viper,
	logger *logging.Logger,
) (*GroupByWorker, error) {

	var groupByWorker GroupByWorker
	var err error

	switch groupType {
	case GROUP_TYPE_YEARMONTH:
		config := GroupByYearMonthConfig(groupId, groupCount)
		groupByWorker, err = NewGroupByGenericWorker(rabbitConf, config)
		if err != nil {
			return nil, err
		}
	case GROUP_TYPE_SEMESTER:
		config := GroupBySemesterConfig(groupId, groupCount)
		groupByWorker, err = NewGroupByGenericWorker(rabbitConf, config)
		if err != nil {
			return nil, err
		}
	case GROUP_TYPE_TOPK:
		Kconfig := envConfig.GetInt("k")
		logger.Infof("GroupBy type %s using k: %d", GROUP_TYPE_TOPK, Kconfig)
		config := GroupByTopKConfig(groupId, groupCount, Kconfig)
		groupByWorker, err = NewGroupByGenericWorker(rabbitConf, config)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown groupBy type: %s", groupType)
	}

	return &groupByWorker, nil
}
