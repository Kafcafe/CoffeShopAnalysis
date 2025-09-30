package join

type Join struct{}

func NewJoiner() *Join {
	return &Join{}
}

func (j *Join) JoinItemNameById() []string {
	// Placeholder implementation
	joinedItems := make([]string, 0)
	return joinedItems
}
