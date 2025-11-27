package structures

import (
	"fmt"
	"strings"
)

type AllowedGroup interface {
	AddBatch(records []string)
	ToMapString() map[string][]string
	AddMapString(data map[string][]string)
	GetMessageToSend() []map[string][]string
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

func (g *GrouperPerClient[T]) Delete(clientId ClientId) {
	delete(g.groups, clientId)
}

func (g *GrouperPerClient[T]) ToFullStringList(clientId ClientId) []string {
	out := []string{}
	mapInfo := g.Get(clientId).ToMapString()
	for key, values := range mapInfo {
		for _, value := range values {
			line := fmt.Sprintf("%s,%s", key, value)
			out = append(out, line)
		}
	}
	return out
}

func (g *GrouperPerClient[T]) AddFullStringList(clientId ClientId, data []string) {
	group := g.Get(clientId)
	mapString := make(map[string][]string)
	for _, line := range data {
		parts := strings.SplitN(line, ",", 2)
		key, value := parts[0], parts[1]
		mapString[key] = append(mapString[key], value)
	}
	group.AddMapString(mapString)
	g.groups[clientId] = group
}
