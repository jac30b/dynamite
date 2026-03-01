package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// replicate sends a PUT request to the target node's internal endpoint
// to store a replica of the key-value pair.
func (s *server) replicate(node, key string, entry DataEntry) error {
	url := fmt.Sprintf("http://%s/internal?key=%s", node, key)

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling data entry: %w", err)
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating replication request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("replication to %s failed: %w", node, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("replication to %s returned status %d", node, resp.StatusCode)
	}

	return nil
}

// fetchReplica sends a GET request to a target node's internal endpoint
// to retrieve a replica of the key-value pair and its vector clock.
func (s *server) fetchReplica(node, key string) (DataEntry, error) {
	var entry DataEntry
	url := fmt.Sprintf("http://%s/internal?key=%s", node, key)

	resp, err := http.Get(url)
	if err != nil {
		return entry, fmt.Errorf("fetching from %s failed: %w", node, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return entry, fmt.Errorf("%s - %w", key, errNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		return entry, fmt.Errorf("fetching from %s returned status %d", node, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&entry); err != nil {
		return entry, fmt.Errorf("decoding response from %s: %w", node, err)
	}

	return entry, nil
}
