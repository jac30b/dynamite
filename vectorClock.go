package main

type VectorClock map[string]int

// Copy creates a deep copy of the vector clock
func (v VectorClock) Copy() VectorClock {
	c := make(VectorClock)
	for k, val := range v {
		c[k] = val
	}
	return c
}

// Increment updates the counter for the given node.
func (v VectorClock) Increment(node string) {
	v[node]++
}

// Descends checks if vector 'v' descends from or is equal to 'other'.
// It returns true if v has all nodes from other with counts >= other's counts.
func (v VectorClock) Descends(other VectorClock) bool {
	for node, count := range other {
		if v[node] < count {
			return false
		}
	}
	return true
}

// Merge combines this vector clock with another, taking the max of each counter
func (v VectorClock) Merge(other VectorClock) VectorClock {
	merged := v.Copy()
	for node, count := range other {
		if merged[node] < count {
			merged[node] = count
		}
	}
	return merged
}
