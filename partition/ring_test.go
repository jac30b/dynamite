package partition

import (
	"testing"
)

func TestRing_Basic(t *testing.T) {
	r := NewRing(100)
	r.AddNode("node1")
	r.AddNode("node2")
	r.AddNode("node3")

	key := "my-secret-key"
	node := r.GetNode(key)

	if node == "" {
		t.Fatal("Expected a node, got empty string")
	}

	// Ensure same key always returns same node
	if r.GetNode(key) != node {
		t.Error("Consistency failed: same key returned different node")
	}
}

func TestRing_RemoveNode(t *testing.T) {
	r := NewRing(10)
	r.AddNode("node1")
	r.AddNode("node2")

	key := "test-key"
	// Find which node owns it
	originalNode := r.GetNode(key)

	// Remove the other node, should still be the same
	otherNode := "node1"
	if originalNode == "node1" {
		otherNode = "node2"
	}
	r.RemoveNode(otherNode)

	if r.GetNode(key) != originalNode {
		t.Errorf("Expected key to stay on %s, but it moved", originalNode)
	}

	// Remove the owner, it must move
	r.RemoveNode(originalNode)
	newNode := r.GetNode(key)
	if newNode == originalNode {
		t.Error("Key stayed on removed node")
	}
}

func TestRing_Empty(t *testing.T) {
	r := NewRing(10)
	if r.GetNode("key") != "" {
		t.Error("Expected empty string for empty ring")
	}
}
