package group

import (
	"common/middleware"
	"fmt"
)

const (
	GROUP_TYPE_YEARMONTH = "yearmonth"
	GROUP_TYPE_SEMESTER  = "semester"
)

type GroupByWorker interface {
	Run() error
}

func CreateGroupByWorker(groupType string,
	rabbitConf middleware.RabbitConfig,
	groupId string,
	groupCount int,
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
	default:
		return nil, fmt.Errorf("Unknown groupBy type: %s", groupType)
	}

	return &groupByWorker, nil
}
