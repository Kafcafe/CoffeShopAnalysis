package structures

type AllowedGroup interface {
	AddBatch(records []string)
	ToMapString() map[string][]string
	AddMapString(data map[string][]string)
	GetMessageToSend() []map[string][]string
	ToFullStringList() []string
	Recover([]string) error
}

// Mapa genérico parametrizado
type GrouperPerClient[T AllowedGroup] struct {
	groups  map[ClientId]T
	factory func() T
}

func NewGrouperPerClient[T AllowedGroup](factory func() T) *GrouperPerClient[T] {
	return &GrouperPerClient[T]{
		groups:  make(map[ClientId]T),
		factory: factory,
	}
}

// add necesita una factory function para crear un nuevo grupo
func (g *GrouperPerClient[T]) Add(clientId ClientId, records []string) {
	group, ok := g.groups[clientId]
	if !ok {
		group = g.factory()
	}
	group.AddBatch(records)
	g.groups[clientId] = group
}

func (g *GrouperPerClient[T]) Get(clientId ClientId) T {
	if group, ok := g.groups[clientId]; ok {
		return group
	}
	return g.factory()
}

func (g *GrouperPerClient[T]) Recover(clientId ClientId, data []string) {
	group, ok := g.groups[clientId]
	if !ok {
		group = g.factory()
	}
	group.Recover(data)
	g.groups[clientId] = group
}

func (g *GrouperPerClient[T]) Delete(clientId ClientId) {
	delete(g.groups, clientId)
}
