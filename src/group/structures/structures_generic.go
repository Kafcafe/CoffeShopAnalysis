package structures

type AllowedGroup interface {
	AddBatch(records []string)
	ToMapString() map[string][]string
	AddMapString(data map[string][]string)
	GetMessageToSend() []map[string][]string
	ToFullStringList() []string
}

// Mapa genérico parametrizado
type GrouperPerClient[T AllowedGroup] map[ClientId]T

func NewGrouperPerClient[T AllowedGroup]() GrouperPerClient[T] {
	return make(GrouperPerClient[T])
}

// add necesita una factory function para crear un nuevo grupo
func (g GrouperPerClient[T]) Add(clientId ClientId, records []string, factory func() T) {
	group, ok := g[clientId]
	if !ok {
		group = factory()
	}
	group.AddBatch(records)
	g[clientId] = group
}

func (g GrouperPerClient[T]) Get(clientId ClientId, factory func() T) T {
	if group, ok := g[clientId]; ok {
		return group
	}
	return factory()
}

func (g GrouperPerClient[T]) Delete(clientId ClientId) {
	delete(g, clientId)
}
