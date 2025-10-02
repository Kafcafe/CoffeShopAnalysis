package clientHandler

import (
	"fmt"
	"strconv"
	"strings"
)

func toInt(s string) (int, error) {
	// Convert string to int, returning an error if the conversion fails
	i, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}
	return i, nil
}

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
	// userIdParsed := ""
	userIdRaw := parts[4]
	userIdParsed := strings.Split(userIdRaw, ".")

	// if userIdRaw != "" {
	// 	userId, err := toInt(userIdRaw)
	// 	if err == nil {
	// 		userIdParsed = strconv.Itoa(userId)
	// 	}
	// }

	selected := []string{
		parts[0],        // transaction_id
		parts[1],        // store_id
		userIdParsed[0], // user_id
		parts[7],        // final_amount
		parts[8],        // created_at
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

// cleanStore cleans a single store line.
//
// Input format (CSV):
//
// store_id,store_name,street,postal_code,city,state,latitude,longitude
//
// Example:
// 5,Coffee Shop Downtown,123 Main St,12345,Metropolis,State,40.7128,-74.0060
//
// Output format (CSV):
// store_id,store_name
//
// Example:
// 5,Coffee Shop Downtown
func cleanStore(store string) (string, error) {
	parts := strings.Split(store, ",")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid line: %s", store)
	}

	selected := []string{
		parts[0], // store_id
		parts[1], // store_name
	}

	return strings.Join(selected, ","), nil
}

// cleanStores cleans multiple store lines.
//
// Input format:
//
// store_id,store_name,street,postal_code,city,state,latitude,longitude
//
//	[]string{
//	    "5,Coffee Shop Downtown,123 Main St,12345,Metropolis,State,40.7128,-74.0060",
//	    "6,Coffee Shop Uptown,456 Elm St,67890,Gotham,State,34.0522,-118.2437",
//	}
//
// Output format:
//
// store_id,store_name
//
//	[]string{
//	    "5,Coffee Shop Downtown",
//	    "6,Coffee Shop Uptown",
//	}
func cleanStores(stores []string) ([]string, error) {
	var cleaned []string

	for _, line := range stores {
		result, err := cleanStore(line)
		if err != nil {
			return nil, err
		}
		cleaned = append(cleaned, result)
	}

	return cleaned, nil
}

// cleanUser cleans a single user line.
//
// Input format (CSV):
//
// user_id,gender,birthdate,registered_at
// Example:
// 42,F,1990-05-15,2023-11-20
//
// Output format (CSV):
// user_id,birthdate
//
// Example:
// 42,1990-05-15
func cleanUser(user string) (string, error) {
	parts := strings.Split(user, ",")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid line: %s", user)
	}

	selected := []string{
		parts[0], // user_id
		parts[2], // birthdate
	}

	return strings.Join(selected, ","), nil
}

// cleanUsers cleans multiple user lines.
// Input format:
//
// user_id,gender,birthdate,registered_at
//
//	[]string{
//	    "42,F,1990-05-15,2023-11-20",
//	    "43,M,1985-08-30,2022-07-15",
//	}
//
// Output format:
//
// user_id,birthdate
//
//	[]string{
//	    "42,1990-05-15",
//	    "43,1985-08-30",
//	}
func cleanUsers(users []string) ([]string, error) {
	var cleaned []string

	for _, line := range users {
		result, err := cleanUser(line)
		if err != nil {
			return nil, err
		}
		cleaned = append(cleaned, result)
	}

	return cleaned, nil
}
