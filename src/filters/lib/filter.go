package filter

import (
	"fmt"
	"strings"
)

type Filter struct {
}

func NewFilter() *Filter {
	return &Filter{}
}

func (f *Filter) Run() error {
	return nil
}

func (f *Filter) FilterByDatetimeHour(batch []string) []string {
	return []string{}
}

func (f *Filter) FilterByYearMonth(batch []string) []string {
	return []string{}
}

func (f *Filter) FilterByAmount(batch []string, amount int) []string {
	result := make([]string, 0)
	for _, record := range batch {
		splited := strings.Split(record, ",")
		if len(splited) < 6 {
			continue
		}

		if splited[5] == "" {
			continue
		}

		var value float64
		if _, err := fmt.Sscanf(splited[5], "%f", &value); err != nil {
			continue
		}

		if value < float64(amount) {
			continue
		}

		result = append(result, record)
	}
	return result
}
