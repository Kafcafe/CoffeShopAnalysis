package group

import (
	"common/middleware"
	"fmt"

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
		groupByWorker, err = NewGroupByYearmonthWorker(rabbitConf, groupId, groupCount)
		if err != nil {
			return nil, err
		}
	case GROUP_TYPE_SEMESTER:
		groupByWorker, err = NewGroupBySemesterWorker(rabbitConf, groupId, groupCount)
		if err != nil {
			return nil, err
		}
	case GROUP_TYPE_STORE:
		groupByWorker, err = NewGroupByStoreWorker(rabbitConf, groupId, groupCount)
		if err != nil {
			return nil, err
		}
	case GROUP_TYPE_TOPK:
		Kconfig := envConfig.GetInt("k")
		logger.Infof("GroupBy type %s using k: %d", GROUP_TYPE_TOPK, Kconfig)
		groupByWorker, err = NewGroupByTopKBestClients(rabbitConf, groupId, groupCount, Kconfig)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Unknown groupBy type: %s", groupType)
	}

	return &groupByWorker, nil
}
