package atomicwritter

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/op/go-logging"
)

const separator = "@"

type AtomicWriter struct {
	path string
	log  *logging.Logger
}

func NewAtomicWriter(path string) *AtomicWriter {
	return &AtomicWriter{path: path, log: logging.MustGetLogger("WRITTER")}
}

func (aw *AtomicWriter) Write(data, metadata []string) error {

	filename := strings.Join(metadata, separator) + ".csv"
	dstFile, err := aw.findFile(filename)

	if err != nil {
		return err
	}

	if dstFile == "" {
		dstFile = filepath.Join(aw.path, filename)
	}

	tmpFile, err := os.CreateTemp(aw.path, "tmpfile_*.csv")

	if err != nil {
		return err
	}

	for _, line := range data {
		if _, err := tmpFile.WriteString(line + "\n"); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			return err
		}
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return err
	}

	if err := tmpFile.Close(); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return err
	}

	if err := os.Chmod(tmpFile.Name(), 0644); err != nil {
		os.Remove(tmpFile.Name())
		return err
	}

	return os.Rename(tmpFile.Name(), dstFile)
}

func (aw *AtomicWriter) findFile(index string) (string, error) {
	entries, err := os.ReadDir(aw.path)

	if err != nil {
		return "", err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if strings.Contains(entry.Name(), index) {
			return filepath.Join(aw.path, entry.Name()), nil
		}
	}

	return "", nil
}

func (aw *AtomicWriter) Recover() (map[string]*SavedInfo, error) {
	results := make(map[string]*SavedInfo)
	files, err := os.ReadDir(aw.path)
	aw.log.Infof("Starting recovery from path: %s", aw.path)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		parts := strings.Split(file.Name(), separator)
		clientID := parts[0]
		dataType := parts[len(parts)-1]

		if strings.Contains(file.Name(), "tmpfile") {
			continue
		}

		filepath := filepath.Join(aw.path, file.Name())
		aw.log.Infof("Recovering data from file: %s and client: %s", filepath, clientID)
		lines, err := aw.ReadFileLines(filepath)
		if err != nil {
			return nil, err
		}

		if _, exists := results[clientID]; !exists {
			results[clientID] = NewSavedInfo([]string{})
		}

		results[clientID].Add(lines, dataType)
	}
	return results, nil
}

func (aw *AtomicWriter) ReadFileLines(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	results := []string{}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		results = append(results, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (aw *AtomicWriter) CleanClient(clientID string) error {
	return aw.cleanFiles(func(fileName string) bool {
		return strings.Contains(fileName, clientID)
	})
}

func (aw *AtomicWriter) CleanAll() error {
	return aw.cleanFiles(func(fileName string) bool {
		return true
	})
}

func (aw *AtomicWriter) cleanFiles(shouldRemove func(string) bool) error {
	files, err := os.ReadDir(aw.path)
	if err != nil {
		return err
	}

	var lastError error
	removedCount := 0

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if shouldRemove(file.Name()) {
			filepath := filepath.Join(aw.path, file.Name())
			err := os.Remove(filepath)
			if err != nil {
				aw.log.Errorf("Error removing file %s: %v", filepath, err)
			} else {
				aw.log.Debugf("Successfully removed file: %s", filepath)
				removedCount++
			}
		}
	}

	aw.log.Infof("Cleanup completed. Removed %d files from %s", removedCount, aw.path)
	return lastError
}
