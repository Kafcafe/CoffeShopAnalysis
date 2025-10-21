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
	var matchedFiles []string

	err := fh.walkDir(fh.folderPath, pattern, &matchedFiles)
	if err != nil {
		fh.log.Critical("Error walking directory: %v", err)
		return nil, err
	}

	if len(matchedFiles) == 0 {
		fh.log.Critical("No files matched the given pattern")
		return nil, fmt.Errorf("no files matched the given pattern")
	}

	return matchedFiles, nil
}

// walkDir recursively walks through directories and appends files matching the pattern.
func (fh *FileHandler) walkDir(path, pattern string, matchedFiles *[]string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("error reading directory %s: %w", path, err)
	}

	for _, entry := range entries {
		fullPath := fh.buildPath(path, entry.Name())
		if !strings.Contains(entry.Name(), pattern) {
			continue
		}
		if entry.IsDir() {
			if err := fh.walkDir(fullPath, pattern, matchedFiles); err != nil {
				return err
			}
		} else if strings.Contains(entry.Name(), pattern) {
			*matchedFiles = append(*matchedFiles, fullPath)
		}
	}
	return nil
}

func (fh *FileHandler) buildPath(filepath, filename string) string {
	return fmt.Sprintf("%s/%s", filepath, filename)
}
