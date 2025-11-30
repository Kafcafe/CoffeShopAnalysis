package atomicwritter

import (
	"bufio"
	"fmt"
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

func (aw *AtomicWriter) WriteLines(data, metadata []string) error {
	return aw.write(data, metadata, ".csv")
}

func (aw *AtomicWriter) write(data, metadata []string, extension string) error {

	filename := strings.Join(metadata, separator) + extension
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
	if err := os.MkdirAll(aw.path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %v", aw.path, err)
	}
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

		metadata := strings.Split(file.Name(), separator)

		if len(metadata) < 2 {
			continue
		}

		clientID := metadata[0]
		dataType := strings.Split(metadata[len(metadata)-1], ".")[0]
		filepath := filepath.Join(aw.path, file.Name())
		aw.log.Debugf("Recovering data from file: %s and client: %s and datatype: %s", filepath, clientID, dataType)
		lines, err := aw.ReadFileLines(filepath)
		if err != nil {
			return nil, err
		}

		if _, exists := results[clientID]; !exists {
			results[clientID] = NewSavedInfo()
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
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 128*1024)

	for scanner.Scan() {
		line := scanner.Text()
		results = append(results, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (aw *AtomicWriter) CleanClient(clientID string) (int, error) {
	aw.log.Infof("Deleting file for clientID check: %s", clientID)
	return aw.cleanFiles(func(fileName string) bool {
		return strings.Contains(fileName, clientID)
	})
}

func (aw *AtomicWriter) CleanAll() (int, error) {
	return aw.cleanFiles(func(fileName string) bool {
		return true
	})
}

func (aw *AtomicWriter) cleanFiles(shouldRemove func(string) bool) (int, error) {
	files, err := os.ReadDir(aw.path)
	if err != nil {
		return 0, err
	}

	var lastError error
	removedCount := 0

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		metadata := strings.Split(file.Name(), separator)

		if len(metadata) == 1 {
			continue
		}

		if shouldRemove(metadata[0]) {
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
	return removedCount, lastError
}

func (aw *AtomicWriter) WriteLine(data, extension string, metadata []string) error {
	return aw.write([]string{data}, metadata, extension)
}
