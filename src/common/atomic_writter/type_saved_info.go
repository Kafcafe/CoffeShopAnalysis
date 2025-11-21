package atomicwritter

type SavedInfo struct {
	data  []string
	count int
}

func NewSavedInfo(data []string) *SavedInfo {
	return &SavedInfo{
		data:  data,
		count: 1,
	}
}

func (si *SavedInfo) Add(data []string) {
	si.data = append(si.data, data...)
	si.count++
}

func (si *SavedInfo) GetData() []string {
	return si.data
}

func (si *SavedInfo) GetCount() int {
	return si.count
}
