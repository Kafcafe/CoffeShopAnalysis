package structures

type AllowedGroup interface {
	AddBatch(records []string)
	Add(record string) error
	ToMapString() map[string][]string
	Merge(other AllowedGroup)
	GetMessageToSend() map[string][]string
	FromMapString(data map[string][]string) AllowedGroup
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

func (g GrouperPerClient[T]) ToMapString(clientId ClientId) map[string][]string {
	if group, ok := g[clientId]; ok {
		return group.ToMapString()
	}
	return map[string][]string{}
}
