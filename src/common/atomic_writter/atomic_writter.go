package atomicwritter

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/op/go-logging"
)

type AtomicWriter struct {
	path string
	log  *logging.Logger
}

func NewAtomicWriter(path string) *AtomicWriter {
	return &AtomicWriter{path: path, log: logging.MustGetLogger("WRITTER")}
}

func (aw *AtomicWriter) Write(data []string, clientId string) error {

	dstFile, err := aw.findFile(clientId)

	if err != nil {
		return err
	}

	if dstFile == "" {
		dstFile = filepath.Join(aw.path, clientId+".csv")
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

func (aw *AtomicWriter) Recover() ([]string, error) {
	return nil, nil
}

func (aw *AtomicWriter) CleanClient(clientID string) error {
	files, err := os.ReadDir(aw.path)

	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() && strings.Contains(file.Name(), clientID) {
			filepath := filepath.Join(aw.path, file.Name())
			err := os.Remove(filepath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (aw *AtomicWriter) CleanAll() error {
	files, err := os.ReadDir(aw.path)

	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			filepath := filepath.Join(aw.path, file.Name())
			err := os.Remove(filepath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
