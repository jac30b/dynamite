package partition

import (
	"fmt"
	"hash/crc32"
	"slices"
	"sort"
	"sync"
)

// Token represents a point on the hash ring.
type Token uint32

// Ring maintains the consistent hashing state.
type Ring struct {
	tokens       []Token
	tokenToNode  map[Token]string
	nodes        map[string]bool
	virtualNodes int
	mu           sync.RWMutex
}

// NewRing creates a new Ring instance with a specified number of virtual nodes (tokens) per physical node.
func NewRing(virtualNodes int) *Ring {
	return &Ring{
		tokens:       []Token{},
		tokenToNode:  make(map[Token]string),
		nodes:        make(map[string]bool),
		virtualNodes: virtualNodes,
	}
}

// hash generates a Token for a given string.
func (r *Ring) hash(key string) Token {
	return Token(crc32.ChecksumIEEE([]byte(key)))
}

// AddNode adds a physical node to the ring by generating its virtual node tokens.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return
	}

	for i := 0; i < r.virtualNodes; i++ {
		token := r.hash(fmt.Sprintf("%s#%d", nodeID, i))
		r.tokens = append(r.tokens, token)
		r.tokenToNode[token] = nodeID
	}

	slices.Sort(r.tokens)

	r.nodes[nodeID] = true
}

// RemoveNode removes a physical node and its associated tokens from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeID] {
		return
	}

	newTokens := []Token{}
	for _, t := range r.tokens {
		if r.tokenToNode[t] != nodeID {
			newTokens = append(newTokens, t)
		} else {
			delete(r.tokenToNode, t)
		}
	}
	r.tokens = newTokens
	delete(r.nodes, nodeID)
}

// GetNode returns the ID of the node responsible for the given key.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.tokens) == 0 {
		return ""
	}

	h := r.hash(key)
	idx := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i] >= h
	})

	if idx == len(r.tokens) {
		idx = 0
	}

	return r.tokenToNode[r.tokens[idx]]
}

// GetPreferenceList returns the top n distinct physical nodes responsible for
// the given key by walking clockwise from its position on the ring.
// As described in the Dynamo paper, virtual nodes mapping to the same physical
// node are skipped so the list contains n unique nodes for replication.
func (r *Ring) GetPreferenceList(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.tokens) == 0 {
		return nil
	}

	h := r.hash(key)
	idx := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i] >= h
	})

	if idx == len(r.tokens) {
		idx = 0
	}

	seen := make(map[string]bool)
	list := make([]string, 0, n)

	for i := 0; i < len(r.tokens) && len(list) < n; i++ {
		pos := (idx + i) % len(r.tokens)
		node := r.tokenToNode[r.tokens[pos]]
		if !seen[node] {
			seen[node] = true
			list = append(list, node)
		}
	}

	return list
}
