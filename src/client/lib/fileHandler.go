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

	var matchedFiles []string

	for _, entry := range entries {
		if entry.IsDir() {
			subFiles, err := fh.GetSubDirectories(entry.Name(), pattern, fh.folderPath)

			if err != nil {
				return nil, err
			}
			matchedFiles = append(matchedFiles, subFiles...)
			continue
		}

		if strings.Contains(entry.Name(), pattern) {
			matchedFiles = append(matchedFiles, entry.Name())
		}
	}

	if len(matchedFiles) == 0 {
		fh.log.Critical("No files matched the given pattern")
		return nil, fmt.Errorf("no files matched the given pattern")
	}

	return matchedFiles, nil
}

func (fh *FileHandler) GetSubDirectories(path string, pattern string, parent string) ([]string, error) {

	currDir := fh.buildPath(parent, path)
	entries, err := os.ReadDir(currDir)

	if err != nil {
		return nil, fmt.Errorf("error reading directory: %w", err)
	}

	var directories []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if strings.Contains(entry.Name(), pattern) {
			directories = append(directories, fh.buildPath(currDir, entry.Name()))
		}
	}

	return directories, nil

}

func (fh *FileHandler) buildPath(filepath, filename string) string {
	return fmt.Sprintf("%s/%s", filepath, filename)
}
