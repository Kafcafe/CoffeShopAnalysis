package filters

type YearFilterConfig struct {
	FromYear int
	ToYear   int
}

type HourFilterConfig struct {
	FromHour int
	ToHour   int
}

type AmountFilterConfig struct {
	MinAmount float64
}

type BasicWatchMeshConfig struct {
	Port                            int
	HeartbeatIntervalSeconds        float64
	HeartbeatTimeoutSeconds         float64
	AddressResolvingRetries         int
	AddressResolvingIntervalSeconds float64
}
