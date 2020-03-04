
package consistent

import (
"errors"
"hash/crc32"
"sort"
"sync"
)

var ErrNodeNotFound = errors.New("node not found")

type Ring struct {
	Nodes Nodes
	sync.Mutex
}

func NewRing() *Ring {
	return &Ring{Nodes: Nodes{}}
}

func (r *Ring) AddNode(id string) {
	r.Lock()
	defer r.Unlock()

	node := NewNode(id)
	r.Nodes = append(r.Nodes, node)

	sort.Sort(r.Nodes)
}

func (r *Ring) RemoveNode(id string) error {
	r.Lock()
	defer r.Unlock()

	i := r.search(id)
	if i >= r.Nodes.Len() || r.Nodes[i].Id != id {
		return ErrNodeNotFound
	}

	r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)

	return nil
}

func (r *Ring) Get(id string) string {
	i := r.search(id)
	if i >= r.Nodes.Len() {
		i = 0
	}

	return r.Nodes[i].Id
}

func (r *Ring) search(id string) int {
	searchfn := func(i int) bool {
		return r.Nodes[i].HashId >= hashId(id)
	}

	return sort.Search(r.Nodes.Len(), searchfn)
}

//----------------------------------------------------------
// Node
//----------------------------------------------------------

type Node struct {
	Id     string
	HashId uint32
}

func NewNode(id string) *Node {
	return &Node{
		Id:     id,
		HashId: hashId(id),
	}
}

type Nodes []*Node

func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n Nodes) Less(i, j int) bool { return n[i].HashId < n[j].HashId }

//----------------------------------------------------------
// Helpers
//----------------------------------------------------------

func hashId(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
