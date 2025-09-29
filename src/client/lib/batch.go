package client

type Batch struct {
	Items []string
	size  int
}

func NewBatch(batchSize int) *Batch {
	return &Batch{
		Items: []string{},
		size:  batchSize,
	}
}

func (b *Batch) AddItem(item string) (successfulAddition bool) {
	if len(b.Items) > b.size {
		return false
	}

	b.Items = append(b.Items, item)
	return true
}
