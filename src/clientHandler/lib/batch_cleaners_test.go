package clientHandler

import (
	"reflect"
	"testing"
)

func TestCleanTransactionItem_Valid(t *testing.T) {
	input := "fe688cc1-4b28-4a05-af82-fe6d0071bf81,8,1,10.0,10.0,2024-01-01 07:00:00"
	expected := "8,1,10.0,2024-01-01 07:00:00"
	result, err := cleanTransactionItem(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestCleanTransactionItem_Invalid(t *testing.T) {
	input := "a,b,c"
	_, err := cleanTransactionItem(input)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func TestCleanTransactionItem_Edge_Empty(t *testing.T) {
	input := ""
	_, err := cleanTransactionItem(input)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func TestCleanTransactionItem_Edge_ExtraFields(t *testing.T) {
	input := "fe688cc1-4b28-4a05-af82-fe6d0071bf81,8,1,10.0,10.0,2024-01-01 07:00:00,extra"
	expected := "8,1,10.0,2024-01-01 07:00:00"
	result, err := cleanTransactionItem(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestCleanTransactionItems_Valid(t *testing.T) {
	input := []string{
		"fe688cc1-4b28-4a05-af82-fe6d0071bf81,8,1,10.0,10.0,2024-01-01 07:00:00",
		"9a688cc1-4b28-4a05-af82-fe6d0071bf81,12,3,5.0,15.0,2024-01-02 08:30:00",
	}
	expected := []string{
		"8,1,10.0,2024-01-01 07:00:00",
		"12,3,15.0,2024-01-02 08:30:00",
	}
	result, err := cleanTransactionItems(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestCleanTransactionItems_Invalid(t *testing.T) {
	input := []string{
		"fe688cc1-4b28-4a05-af82-fe6d0071bf81,8,1,10.0,10.0,2024-01-01 07:00:00",
		"a,b,c",
	}
	_, err := cleanTransactionItems(input)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func TestCleanTransactionItems_Empty(t *testing.T) {
	input := []string{}
	result, err := cleanTransactionItems(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

func TestCleanTransaction_Valid(t *testing.T) {
	input := "ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,1,,,63.5,0.0,63.5,2024-01-01 10:06:50"
	expected := "ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,,63.5,2024-01-01 10:06:50"
	result, err := cleanTransaction(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestCleanTransaction_Invalid(t *testing.T) {
	input := "a,b,c,d,e,f,g,h"
	_, err := cleanTransaction(input)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func TestCleanTransaction_Edge_Empty(t *testing.T) {
	input := ""
	_, err := cleanTransaction(input)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func TestCleanTransaction_Edge_ExtraFields(t *testing.T) {
	input := "ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,1,,,63.5,0.0,63.5,2024-01-01 10:06:50,extra"
	expected := "ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,,63.5,2024-01-01 10:06:50"
	result, err := cleanTransaction(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestCleanTransactions_Valid(t *testing.T) {
	input := []string{
		"ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,1,,,63.5,0.0,63.5,2024-01-01 10:06:50",
	}
	expected := []string{
		"ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,,63.5,2024-01-01 10:06:50",
	}
	result, err := cleanTransactions(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestCleanTransactions_Invalid(t *testing.T) {
	input := []string{
		"ac6f851c-649f-42fb-a606-72be0fdcf8d2,5,1,,,63.5,0.0,63.5,2024-01-01 10:06:50",
		"a,b,c,d,e,f,g,h",
	}
	_, err := cleanTransactions(input)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func TestCleanTransactions_Empty(t *testing.T) {
	input := []string{}
	result, err := cleanTransactions(input)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}
