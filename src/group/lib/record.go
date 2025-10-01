package group

import (
	"strconv"
	"strings"
)

type ParsedRecord struct {
	yearMonth YearMonth
	ItemID    ItemID
	Quantity  int
	Profit    float64
}

func ExtractYearMonth(record Record) YearMonth {
	// Assuming the date is in the format "YYYY-MM-DD HH:MM:SS"
	// Assumming item_id,quantity,subtotal,date
	// Example: "6,3,28.5,2023-07-01 07:00:00"
	fields := strings.Split(record, ",")
	dateField := fields[len(fields)-1]
	dateField = strings.TrimSpace(dateField)
	// Extract "YYYY-MM"
	yearMonth := dateField[:7]
	return YearMonth(yearMonth)
}

func parseRecord(record Record) (*ParsedRecord, error) {
	// Assuming the record format is:
	// item_id,quantity,subtotal
	// item_id,quantity,subtotal,date
	// Example:
	// "6,3,28.5
	fields := strings.Split(record, ",")
	itemID := fields[0]

	quantity, err := toInt(fields[1])
	if err != nil {
		return nil, err
	}
	profit, err := toFloat(fields[2])
	if err != nil {
		return nil, err
	}

	ym := ExtractYearMonth(record)

	return &ParsedRecord{
		yearMonth: ym,
		ItemID:    ItemID(itemID),
		Quantity:  quantity,
		Profit:    profit,
	}, nil
}

func toInt(s string) (int, error) {
	// Convert string to int, returning an error if the conversion fails
	i, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}
	return i, nil
}

func toFloat(s string) (float64, error) {
	// Convert string to float64, returning an error if the conversion fails
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, err
	}
	return f, nil
}
