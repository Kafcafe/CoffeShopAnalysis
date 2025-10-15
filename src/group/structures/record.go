package structures

import (
	"fmt"
	"strconv"
	"strings"
)

type ParsedTransactionItemRecord struct {
	yearMonth YearMonth
	ItemID    ItemID
	Quantity  int
	Profit    float64
}

type UserID string

type ParsedTransactionRecord struct {
	StoreID     StoreID
	UserID      UserID
	FinalAmount float64
	Semester    Semester
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

func parseRecordForYearMonth(record Record) (*ParsedTransactionItemRecord, error) {
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

	return &ParsedTransactionItemRecord{
		yearMonth: ym,
		ItemID:    ItemID(itemID),
		Quantity:  quantity,
		Profit:    profit,
	}, nil
}

func parseRecordForSemester(record Record) (*ParsedTransactionRecord, error) {
	// Assuming the record format is:
	// item_id,quantity,subtotal
	// item_id,quantity,subtotal,date
	// Example:
	// "6,3,28.5
	fields := strings.Split(record, ",")
	storeID := fields[1]
	userID := fields[2]

	finalAmount, err := toFloat(fields[3])
	if err != nil {
		return nil, err
	}

	semester, err := ExtractSemester(record)
	if err != nil {
		return nil, err
	}
	return &ParsedTransactionRecord{
		StoreID:     StoreID(storeID),
		UserID:      UserID(userID),
		FinalAmount: finalAmount,
		Semester:    *semester,
	}, nil
}

func ExtractSemester(record Record) (*Semester, error) {
	yearMonth := string(ExtractYearMonth(record))
	yearMonthSplit := strings.Split(yearMonth, "-")

	year := yearMonthSplit[0]
	month, err := toInt(yearMonthSplit[1])
	if err != nil {
		return nil, err
	}

	var semester string
	if month <= 6 {
		semester = fmt.Sprintf("%s-%s", year, FIRST_HALF_H1)
	} else {
		semester = fmt.Sprintf("%s-%s", year, SECOND_HALF_H2)
	}

	return (*Semester)(&semester), nil
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
