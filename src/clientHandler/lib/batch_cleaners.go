package clientHandler

import (
	"fmt"
	"strings"
)

// cleanTransaction cleans a single transaction line.
//
// Input format (CSV):
//
//	transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
//
// Example:
//
//	ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,1,,,63.5,0.0,63.5,2024-01-01 10:06:50
//
// Output format (CSV):
//
//	transaction_id,store_id,user_id,final_amount,created_at
//
// Example:
//
//	ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,,63.5,2024-01-01 10:06:50
func cleanTransaction(transaction string) (string, error) {
	parts := strings.Split(transaction, ",")
	if len(parts) < 9 {
		return "", fmt.Errorf("invalid line: %s", transaction)
	}

	selected := []string{
		parts[0], // transaction_id
		parts[1], // store_id
		parts[4], // user_id
		parts[7], // final_amount
		parts[8], // created_at
	}

	return strings.Join(selected, ","), nil
}

// cleanTransactions cleans multiple transaction lines.
//
// Input format:
//
//	 transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
//		[]string{
//		    "ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,1,,,63.5,0.0,63.5,2024-01-01 10:06:50",
//		}
//
// Output format:
//
//	 transaction_id,store_id,user_id,final_amount,created_at
//	[]string{
//	    "ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,,63.5,2024-01-01 10:06:50",
//	}
func cleanTransactions(transactions []string) ([]string, error) {
	var cleaned []string

	for _, line := range transactions {
		result, err := cleanTransaction(line)
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
//	transaction_id,item_id,quantity,unit_price,subtotal,created_at
//
// Example:
//
//	fe688cc1-4b28-4a05-af82-fe6d0071bf81,8,1,10.0,10.0,2024-01-01 07:00:00
//
// Output format (CSV):
//
//	item_id,quantity,subtotal,created_at
//
// Example:
//
//	8,1,10.0,2024-01-01 07:00:00
func cleanTransactionItem(transactionItem string) (string, error) {
	parts := strings.Split(transactionItem, ",")
	if len(parts) < 6 {
		return "", fmt.Errorf("invalid line: %s", transactionItem)
	}

	selected := []string{
		parts[1], // item_id
		parts[2], // quantity
		parts[4], // subtotal
		parts[5], // created_at
	}

	return strings.Join(selected, ","), nil
}

// cleanTransactionItems cleans multiple transaction lines.
//
// Input format:
//
//	transaction_id,item_id,quantity,unit_price,subtotal,created_at
//
//	[]string{
//	    "fe688cc1-4b28-4a05-af82-fe6d0071bf81,8,1,10.0,10.0,2024-01-01 07:00:00",
//	    "9a688cc1-4b28-4a05-af82-fe6d0071bf81,12,3,5.0,15.0,2024-01-02 08:30:00",
//	}
//
// Output format:
//
//	item_id,quantity,subtotal,created_at
//	[]string{
//	    "8,1,10.0,2024-01-01 07:00:00",
//	    "12,3,15.0,2024-01-02 08:30:00",
//	}
func cleanTransactionItems(transactionItems []string) ([]string, error) {
	var cleaned []string

	for _, line := range transactionItems {
		result, err := cleanTransactionItem(line)
		if err != nil {
			return nil, err
		}
		cleaned = append(cleaned, result)
	}

	return cleaned, nil
}

// cleanMenuItems cleans multiple menu item lines.
//
// Input format:
//
//	item_id,item_name,category,price,is_seasonal,available_from,available_to
//
//	[]string{
//	    "8,Espresso,Beverages,10.0,false,2024-01-01,2024-12-31",
//	    "12,Cappuccino,Food,5.0,true,2024-06-01,2024-08-31",
//	}
//
// Output format:
//
//	item_id,item_name
//
//	[]string{
//	    "8,Espresso",
//	    "12,Cappuccino",
//	}
func cleanMenuItems(menuItems []string) ([]string, error) {
	var cleaned []string

	for _, line := range menuItems {
		result, err := cleanMenuItem(line)
		if err != nil {
			return nil, err
		}
		cleaned = append(cleaned, result)
	}

	return cleaned, nil
}

// cleanMenuItem cleans a single menu item line.
//
// Input format (CSV):
//
//	item_id,item_name,category,price,is_seasonal,available_from,available_to
//
// Example:
//
//	8,Espresso,Beverages,10.0,false,2024-01-01,2024-12-31
//
// Output format (CSV):
//
//	item_id,item_name
//
// Example:
//
//	8,Espresso
func cleanMenuItem(menuItem string) (string, error) {
	parts := strings.Split(menuItem, ",")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid line: %s", menuItem)
	}

	selected := []string{
		parts[0], // item_id
		parts[1], // item_name
	}

	return strings.Join(selected, ","), nil
}
