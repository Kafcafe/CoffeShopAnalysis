package quantityandprofit

import (
	"strconv"
	"strings"
)

type ParsedRecord struct {
	ItemID   ItemID
	Quantity int
	Profit   float64
}

func parseRecord(record Record) (*ParsedRecord, error) {
	// Assuming the record format is:
	// item_id,quantity,subtotal
	// Example:
	// "6,3,28.5
	fields := strings.Split(record, ",")
	itemID, err := toInt(fields[0])
	if err != nil {
		return nil, err
	}
	quantity, err := toInt(fields[1])
	if err != nil {
		return nil, err
	}
	profit, err := toFloat(fields[2])
	if err != nil {
		return nil, err
	}

	return &ParsedRecord{
		ItemID:   ItemID(itemID),
		Quantity: quantity,
		Profit:   profit,
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
