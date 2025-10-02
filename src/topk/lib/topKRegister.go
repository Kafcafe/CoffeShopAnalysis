package topk

type TopKRegister struct {
	StoreId string
	UserId  string
	Count   int
}

func CmpTransactions(a, b TopKRegister) int {
	if a.Count < b.Count {
		return 1
	} else if a.Count > b.Count {
		return -1
	}
	return 0
}

func NewTopKRegister(storeId string, userId string, count int) TopKRegister {
	return TopKRegister{
		StoreId: storeId,
		UserId:  userId,
		Count:   count,
	}
}

func (reg *TopKRegister) String() string {
	return reg.UserId
}
