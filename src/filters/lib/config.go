package filters

type DatetimeFilterConfig struct {
	FromYear int
	ToYear   int
	FromHour int
	ToHour   int
}

type YearFilterConfig struct {
	FromYear int
	ToYear   int
}

type FiltersConfig struct {
	query1 DatetimeFilterConfig
	query2 YearFilterConfig
	query3 DatetimeFilterConfig
	query4 YearFilterConfig
}

func NewFiltersConfig(query1 DatetimeFilterConfig, query2 YearFilterConfig,
	query3 DatetimeFilterConfig, query4 YearFilterConfig) FiltersConfig {
	return FiltersConfig{
		query1: query1,
		query2: query2,
		query3: query3,
		query4: query4,
	}
}
