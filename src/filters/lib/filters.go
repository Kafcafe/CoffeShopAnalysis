package filters

import (
	"common/middleware"
	"common/watch_mesh"
	"fmt"
	"time"
)

const (
	FILTER_TYPE_YEAR   = "year"
	FILTER_TYPE_HOUR   = "hour"
	FILTER_TYPE_AMOUNT = "amount"
)

type FilterWorker interface {
	Run() error
}

type FilterConfig struct {
	id              string
	idNum           int
	ofType          string
	filtersCount    int
	prevStageSub    string
	nextStagePubs   map[string]string // dataType -> routeKey
	messageCallback func(filter *Filter, batch []string) (filteredBatch []string)
}

func FilterByYearConfig(filterId string, filterIdNum, filterCount int, config YearFilterConfig) FilterConfig {
	return FilterConfig{
		id:           filterId,
		idNum:        filterIdNum,
		ofType:       FILTER_TYPE_YEAR,
		filtersCount: filterCount,
		prevStageSub: "transactions",
		nextStagePubs: map[string]string{
			"transactions":      "transactions.transactions.all",
			"transaction_items": "transactions.items",
		},
		messageCallback: func(filter *Filter, batch []string) (filteredBatch []string) {
			return filter.FilterByYear(batch, config.FromYear, config.ToYear)
		},
	}
}

func FilterByHourConfig(filterId string, filterIdNum, filterCount int, hourConfig HourFilterConfig) FilterConfig {
	return FilterConfig{
		id:           filterId,
		idNum:        filterIdNum,
		ofType:       FILTER_TYPE_HOUR,
		filtersCount: filterCount,
		prevStageSub: "transactions.transactions.all",
		nextStagePubs: map[string]string{
			"transactions": "transactions.year-hour-filtered.all",
		},
		messageCallback: func(filter *Filter, batch []string) (filteredBatch []string) {
			return filter.FilterByHour(batch, hourConfig.FromHour, hourConfig.ToHour)
		},
	}
}

func FilterByAmountConfig(filterId string, filterIdNum, filterCount int, amountConfig AmountFilterConfig) FilterConfig {
	return FilterConfig{
		id:            filterId,
		idNum:         filterIdNum,
		ofType:        FILTER_TYPE_AMOUNT,
		filtersCount:  filterCount,
		prevStageSub:  "transactions.year-hour-filtered.all",
		nextStagePubs: map[string]string{}, // Empty because it is generated at runtime as results.clientUUID
		messageCallback: func(filter *Filter, batch []string) (filteredBatch []string) {
			return filter.FilterByAmount(batch, amountConfig.MinAmount)
		},
	}
}

func CreateFilterWorker(
	filterType string,
	rabbitConf middleware.RabbitConfig,
	yearConfig YearFilterConfig,
	hourConfig HourFilterConfig,
	amountConfig AmountFilterConfig,
	filterId string,
	filterIdNum int,
	filterCount int,
	basicWatchMeshConfig watch_mesh.BasicWatchMeshConfig,
) (*FilterGenericWorker, error) {
	var config FilterConfig

	switch filterType {
	case FILTER_TYPE_YEAR:
		config = FilterByYearConfig(filterId, filterIdNum, filterCount, yearConfig)
	case FILTER_TYPE_HOUR:
		config = FilterByHourConfig(filterId, filterIdNum, filterCount, hourConfig)
	case FILTER_TYPE_AMOUNT:
		config = FilterByAmountConfig(filterId, filterIdNum, filterCount, amountConfig)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", filterType)
	}

	// Prepare addresses for WatchMesh
	peerAddresses := []string{}
	myAddress := fmt.Sprintf("filter%s", config.id)
	for i := 1; i < config.filtersCount+1; i++ {
		peerIp := fmt.Sprintf("filter-%s%d", config.ofType, i)

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
		"filter",
		basicWatchMeshConfig.MaxResurrectionAttempts,
		basicWatchMeshConfig.RandomSeedForJitter,
		basicWatchMeshConfig.CrasherEnabled,
	)

	filterWorker, err := NewFilterGenericWorker(rabbitConf, config, watchMeshConfig)
	if err != nil {
		return nil, err
	}

	return filterWorker, nil
}
