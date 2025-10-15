package client

import (
	logger "common/logger"
	"fmt"
	"os"
	"strings"

	"github.com/op/go-logging"
)

type FileHandler struct {
	folderPath string
	log        *logging.Logger
}

func NewFileHandler(folderPath string) *FileHandler {
	return &FileHandler{
		folderPath: folderPath,
		log:        logger.GetLoggerWithPrefix("[FILE-HANDL]"),
	}
}

func (fh *FileHandler) GetFilesWithPattern(pattern string) ([]string, error) {
	entries, err := os.ReadDir(fh.folderPath)

	if err != nil {
		fmt.Println("Error reading directory:", err)
		return nil, fmt.Errorf("error reading directory: %w", err)
	}

	var matchedFiles []os.DirEntry

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if strings.Contains(entry.Name(), pattern) {
			matchedFiles = append(matchedFiles, entry)
		}
	}

	if len(matchedFiles) == 0 {
		fh.log.Critical("No files matched the given pattern")
		return nil, fmt.Errorf("no files matched the given pattern")
	}

	fileNames := make([]string, len(matchedFiles))

	for i, file := range matchedFiles {
		fileNames[i] = file.Name()
	}

	return fileNames, nil
}
