package client

import (
	"os"
	"strings"
)

type BatchGenerator struct {
	folderPath string
	files      []string
	isReading  bool
}

func NewBatchGenerator(folderPath string) *BatchGenerator {
	return &BatchGenerator{
		folderPath: folderPath,
		files:      []string{},
		isReading:  true,
	}
}

func (bg *BatchGenerator) SetUp() error {
	entries, err := os.ReadDir(bg.folderPath)

	if err != nil {
		log.Error("Error reading directory:", err)
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		file := entry.Name()
		bg.files = append(bg.files, file)

		transactions := []os.DirEntry{}
		users := []os.DirEntry{}
		transactionItems := []os.DirEntry{}
		stores := []os.DirEntry{}
		items := []os.DirEntry{}
		menuItems := []os.DirEntry{}

		if strings.HasPrefix(file, "transaction_items") {
			transactionItems = append(transactionItems, entry)
		} else if strings.HasPrefix(file, "transactions") {
			transactions = append(transactions, entry)
		} else if strings.HasPrefix(file, "users") {
			users = append(users, entry)
		} else if strings.HasPrefix(file, "stores") {
			stores = append(stores, entry)
		} else if strings.HasPrefix(file, "items") {
			items = append(items, entry)
		} else if strings.HasPrefix(file, "menu_items") {
			menuItems = append(menuItems, entry)
		}

		matrix := [][]os.DirEntry{}

		matrix = append(matrix, transactions, transactionItems, stores, items, users, menuItems)

		for _, group := range matrix {
			for _, fileEntry := range group {
				log.Infof("File detected: %s", fileEntry.Name())
			}
		}
	}

	return nil
}
