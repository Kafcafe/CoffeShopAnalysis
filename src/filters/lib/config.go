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
