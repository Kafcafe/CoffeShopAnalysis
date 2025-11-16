package atomicwritter

import (
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type AtomicWriter struct {
	path   string
	prefix string
}

func NewAtomicWriter(path string, prefix string) *AtomicWriter {
	return &AtomicWriter{path: path, prefix: prefix}
}

func (aw *AtomicWriter) Write(data []string) error {
	dir := filepath.Dir(aw.path)

	time := strconv.FormatInt(time.Now().UnixNano(), 10)
	tmpDirName := "tmp_" + filepath.Base(dir) + time
	tmpFile, err := os.CreateTemp(dir, tmpDirName)

	if err != nil {
		return err
	}

	defer os.Remove(tmpFile.Name())

	for i := range data {
		if _, err = tmpFile.WriteString(data[i]); err != nil {
			tmpFile.Close()
			return err
		}
	}

	if err = tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return err
	}
	if err = tmpFile.Close(); err != nil {
		return err
	}

	tmpDirName = tmpFile.Name()

	return os.Rename(tmpDirName, aw.path+time)
}

func (aw *AtomicWriter) Recover() ([]string, error) {
	return nil, nil
}
