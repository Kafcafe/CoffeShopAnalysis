package clientHandler

import (
	"fmt"
	"strings"
)

// cleanTransaction cleans a single transaction line.
// Input format (CSV):
//
//	transaction_id,store_id,user_id,final_amount,created_at
//
// Example:
//
//	bb95f837-d7e3-4251-a99a-5f152748fd78,1,,78.0,2024-01-01 10:07:17
//
// Output format (CSV):
//
//	transaction_id,final_amount
//
// Example:
//
//	bb95f837-d7e3-4251-a99a-5f152748fd78,78.0
func cleanTransactionResult(transaction string) (string, error) {
	parts := strings.Split(transaction, ",")
	if len(parts) < 5 {
		return "", fmt.Errorf("invalid line: %s", transaction)
	}

	selected := []string{
		parts[0], // transaction_id
		parts[3], // final_amount
	}

	return strings.Join(selected, ","), nil
}

// cleanTransactions cleans multiple transaction lines.
//
// Input format:
//
//	 transaction_id,store_id,user_id,final_amount,created_at
//		[]string{
//		    "bb95f837-d7e3-4251-a99a-5f152748fd78,1,,78.0,2024-01-01 10:07:17",
//		}
//
// Output format:
//
//	 transaction_id,final_amount
//	[]string{
//	    "bb95f837-d7e3-4251-a99a-5f152748fd78,78.0",
//	}
func cleanTransactionResults(transactions []string) ([]string, error) {
	var cleaned []string

	for _, line := range transactions {
		result, err := cleanTransactionResult(line)
		if err != nil {
			return nil, err
		}
		cleaned = append(cleaned, result)
	}

	return cleaned, nil
}

// cleanTransactionItem cleans a single transaction line.
//
// Input format (CSV):
//
//	yearmonth,item_name,quantity,tpv
//
// Example:
//
//	2025-04,Mocha,301665,0.000000
//
// Output format (CSV):
//
//   - yearmonth,item_name,quantity,quantity_value
//   - yearmonth,item_name,tpv,tpv_value
//
// Examples:
//
//	2025-04,Mocha,quantity,301665
//	2024-11,Matcha Latte,tpv,3007480.000000
func cleanTransactionItemResult(transactionItem string) (string, error) {
	parts := strings.Split(transactionItem, ",")
	if len(parts) < 4 {
		return "", fmt.Errorf("invalid line: %s", transactionItem)
	}

	quantity := parts[2]
	tpv := parts[3]

	var resultType string
	var resultValue string

	if quantity == "0" {
		resultType = "tpv"
		resultValue = tpv
	} else {
		resultType = "quantity"
		resultValue = quantity
	}

	selected := []string{
		parts[0],    // yearmonth
		parts[1],    // item_name
		resultType,  // result_type
		resultValue, // result_value
	}

	return strings.Join(selected, ","), nil
}

// cleanTransactionItems cleans multiple transaction lines.
//
// Input format:
//
//	yearmonth,item_name,quantity,tpv
//
//	[]string{
//	    "2025-04,Mocha,301665,0.000000",
//	    "2024-11,Matcha Latte,0,3007480.000000",
//	}
//
// Output format:
//
//		yearmonth,item_name,result_type,result_value
//		This can be either:
//	 - yearmonth,item_name,quantity,quantity_value
//	 - yearmonth,item_name,tpv,tpv_value
//
//		[]string{
//		    "2025-04,Mocha,quantity,301665",
//		    "2024-11,Matcha Latte,tpv,3007480.000000",
//		}
func cleanTransactionItemsResults(transactionItems []string) ([]string, error) {
	var cleaned []string

	for _, line := range transactionItems {
		result, err := cleanTransactionItemResult(line)
		if err != nil {
			return nil, err
		}
		cleaned = append(cleaned, result)
	}

	return cleaned, nil
}
