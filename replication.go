package main

import (
	"fmt"
	"net/http"
)

// replicate sends a PUT request to the target node's internal endpoint
// to store a replica of the key-value pair.
func (s *server) replicate(node, key, value string) error {
	url := fmt.Sprintf("http://%s/internal?key=%s&value=%s", node, key, value)

	req, err := http.NewRequest(http.MethodPut, url, nil)
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
