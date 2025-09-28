package filter

import (
	"fmt"
	"strconv"
	"strings"
)

type Filter struct{}

func NewFilter() *Filter {
	return &Filter{}
}

func (f *Filter) FilterByDatetimeHour(batch []string, fromYear, toYear, fromHour, toHour int) []string {
	byYear := f.FilterByYear(batch, fromYear, toYear)
	result := make([]string, 0)
	for _, record := range byYear {
		splited := strings.Split(record, ",")
		datetime := splited[8]
		dateParts := strings.Split(datetime, " ")
		if !f.respectHourLimits(dateParts[1], fromHour, toHour) {
			continue
		}
		result = append(result, record)
	}
	return result
}

func (f *Filter) respectHourLimits(hourString string, fromHour, toHour int) bool {
	hourParts := strings.Split(hourString, ":")
	if len(hourParts) != 3 {
		return false
	}

	hour, err := strconv.Atoi(hourParts[0])

	if err != nil {
		return false
	}

	if hour < fromHour || hour > toHour {
		return false
	}

	minutes := hourParts[1]
	seconds := hourParts[2]
	atoiMinutes, err := strconv.Atoi(minutes)

	if err != nil {
		return false
	}
	atoiSeconds, err := strconv.Atoi(seconds)

	if err != nil {
		return false
	}

	if hour == toHour && atoiMinutes > 0 && atoiSeconds > 0 {
		return false
	}

	return true
}

func (f *Filter) FilterByYear(batch []string, fromYear, toYear int) []string {
	result := make([]string, 0)
	for _, record := range batch {
		splited := strings.Split(record, ",")
		if len(splited) < 9 {
			continue
		}

		if splited[8] == "" {
			continue
		}

		datetime := splited[8]
		dateParts := strings.Split(datetime, " ")
		if len(dateParts) != 2 {
			continue
		}

		date := dateParts[0]
		dateComponents := strings.Split(date, "-")
		if len(dateComponents) != 3 {
			continue
		}

		year, err := strconv.Atoi(dateComponents[0])

		if err != nil {
			continue
		}

		if !f.respectLimit(year, toYear, fromYear) {
			continue
		}
		result = append(result, record)

	}
	return result
}

func (f *Filter) respectLimit(value, upLimit, downLimit int) bool {
	return value <= upLimit && value >= downLimit
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
