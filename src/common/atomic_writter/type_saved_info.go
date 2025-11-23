package atomicwritter

type SavedInfo struct {
	data     []string
	dataType string
	count    int
}

func NewSavedInfo() *SavedInfo {
	return &SavedInfo{
		data:  []string{},
		count: 0,
	}
}

func (si *SavedInfo) Add(data []string, dataType string) {
	si.data = append(si.data, data...)
	si.count++
}

func (si *SavedInfo) GetData() []string {
	return si.data
}

func (si *SavedInfo) GetCount() int {
	return si.count
}

func (si *SavedInfo) GetDataType() string {
	return si.dataType
}
