package client

import (
	"bufio"
	"os"
)

type BatchGenerator struct {
	dataPath     string
	filename     string
	scanner      *bufio.Scanner
	isReading    bool
	lastLineRead string
	file         *os.File
}

func NewBatchGenerator(dataPath string, filename string) *BatchGenerator {

	file, err := os.Open(dataPath + "/" + filename)

	if err != nil {
		log.Errorf("Error opening file %s: %v", filename, err)
		return nil
	}

	scanner := bufio.NewScanner(file)
	scanner.Scan()

	return &BatchGenerator{
		dataPath:     dataPath,
		filename:     filename,
		scanner:      scanner,
		isReading:    true,
		lastLineRead: "",
		file:         file,
	}
}

func (bg *BatchGenerator) IsReading() bool {
	return bg.isReading
}

func (bg *BatchGenerator) GetNextBatch(batchSize int) (*Batch, error) {
	batch := NewBatch(batchSize)

	err := bg.processLastLine(batch)

	if err != nil {
		return nil, err
	}

	for bg.scanner.Scan() {
		line := bg.scanner.Text()

		if batch.AddItem(line) {
			continue
		}

		bg.lastLineRead = line
		return batch, nil
	}

	bg.isReading = false

	return batch, nil
}

func (bg *BatchGenerator) processLastLine(batch *Batch) error {
	if bg.lastLineRead == "" {
		return nil
	}

	if batch.AddItem(bg.lastLineRead) {
		bg.lastLineRead = ""
		return nil
	}

	return nil
}

func (bg *BatchGenerator) Close() {
	if bg.file != nil {
		bg.file.Close()
	}
}
