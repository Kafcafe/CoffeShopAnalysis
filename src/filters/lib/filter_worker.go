package filters

import (
	"common/middleware"
	"fmt"
)

const (
	FILTER_TYPE_YEAR   = "year"
	FILTER_TYPE_HOUR   = "hour"
	FILTER_TYPE_AMOUNT = "amount"
)

type FilterWorker interface {
	Run() error
}

func CreateFilterWorker(filterType string,
	rabbitConf middleware.RabbitConfig,
	yearConfig YearFilterConfig,
	hourConfig HourFilterConfig) (*FilterWorker, error) {

	var filterWorker FilterWorker
	var err error

	switch filterType {
	case FILTER_TYPE_YEAR:
		filterWorker, err = NewFilterByYearWorker(rabbitConf, yearConfig)
		if err != nil {
			return nil, err
		}
	case FILTER_TYPE_HOUR:
		filterWorker, err = NewFilterByHourWorker(rabbitConf, hourConfig)
		if err != nil {
			return nil, err
		}
	case FILTER_TYPE_AMOUNT:
	default:
		return nil, fmt.Errorf("Unknown filter type: %s", filterType)
	}

	return &filterWorker, nil
}
