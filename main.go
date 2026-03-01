package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync"

	"github.com/jac30b/dynamite/partition"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	// "golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

var (
	errNotFound = errors.New("not found")
)

type Config struct {
	ReplicationFactor int      `yaml:"replication_factor"`
	ReadQuorum        int      `yaml:"read_quorum"`
	WriteQuorum       int      `yaml:"write_quorum"`
	Self              string   `yaml:"self"`
	Nodes             []string `yaml:"nodes"`
}

type DataEntry struct {
	Value string      `json:"value"`
	Clock VectorClock `json:"clock"`
}

type server struct {
	ring   *partition.Ring
	config Config
	store  sync.Map
}

func main() {
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	configPath := flag.String("config", "config.yaml", "Path to the configuration file")
	flag.Parse()

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatal().Err(err).Str("path", *configPath).Msg("failed to read config")
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatal().Err(err).Msg("failed to parse config")
	}

	if cfg.ReplicationFactor > len(cfg.Nodes) {
		log.Fatal().
			Int("ReplicationFactor", cfg.ReplicationFactor).
			Int("Nodes", len(cfg.Nodes)).
			Msg("replication factor cannot exceed number of nodes")
	}

	if cfg.Self == "" {
		log.Fatal().Msg("self must be set in config")
	}

	if !slices.Contains(cfg.Nodes, cfg.Self) {
		log.Fatal().Str("self", cfg.Self).Msg("self must be one of the configured nodes")
	}

	log.Debug().
		Int("ReplicationFactor", cfg.ReplicationFactor).
		Int("ReadQuorum", cfg.ReadQuorum).
		Int("WriteQuorum", cfg.WriteQuorum).
		Str("Self", cfg.Self).
		Strs("Nodes", cfg.Nodes).
		Msg("config loaded")

	s := &server{
		ring:   partition.NewRing(10),
		config: cfg,
	}

	for _, node := range cfg.Nodes {
		s.ring.AddNode(node)
	}

	http.HandleFunc("/", s.handleRequest)
	http.HandleFunc("/internal", s.handleInternal)

	log.Info().Str("addr", cfg.Self).Msg("Starting server")
	log.Fatal().Err(http.ListenAndServe(cfg.Self, nil)).Msg("server stopped")
}

// handleRequest handles client-facing GET and PUT requests.
// If this node is not the coordinator for the key, the request is forwarded.
func (s *server) handleRequest(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	preferenceList := s.ring.GetPreferenceList(key, s.config.ReplicationFactor)
	coordinator := preferenceList[0]

	if coordinator != s.config.Self {
		log.Info().
			Str("key", key).
			Str("coordinator", coordinator).
			Msg("forwarding request to coordinator")
		s.forwardRequest(coordinator, r, w)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key, preferenceList)
	case http.MethodGet:
		s.handleGet(w, r, key, preferenceList)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePut stores the key-value pair locally and replicates to the preference list.
func (s *server) handlePut(w http.ResponseWriter, r *http.Request, key string, preferenceList []string) {
	var entry DataEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if entry.Value == "" {
		http.Error(w, "missing value", http.StatusBadRequest)
		return
	}

	if entry.Clock == nil {
		entry.Clock = make(VectorClock)
	}
	// Coordinator increments its counter
	entry.Clock.Increment(s.config.Self)

	// Store locally
	s.store.Store(key, entry)
	log.Info().Str("key", key).Str("value", entry.Value).Msg("stored locally")

	// Replicate to the remaining nodes in the preference list
	for _, node := range preferenceList[1:] {
		if err := s.replicate(node, key, entry); err != nil {
			log.Error().Err(err).Str("node", node).Str("key", key).Msg("replication failed")
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(entry)
}

// handleGet reads the value for the key from the local store and replicas to satisfy ReadQuorum.
func (s *server) handleGet(w http.ResponseWriter, r *http.Request, key string, preferenceList []string) {
	type result struct {
		entry DataEntry
		err   error
	}

	fetch := func(node, key string) (DataEntry, error) {
		if node == s.config.Self {
			val, ok := s.store.Load(key)
			if ok {
				return val.(DataEntry), nil
			}
			return DataEntry{}, errNotFound
		}

		return s.fetchReplica(node, key)
	}

	results := make(chan result, len(preferenceList))
	var eg errgroup.Group

	for _, node := range preferenceList {
		eg.Go(func() error {
			entry, err := fetch(node, key)
			results <- result{entry: entry, err: err}
			return nil
		})
	}

	go func() {
		eg.Wait()
		close(results)
	}()

	var (
		received      []DataEntry
		notFoundCount int
		errorCount    int
	)
	for res := range results {
		if res.err != nil {
			if errors.Is(res.err, errNotFound) {
				notFoundCount++
			} else {
				log.Warn().Err(res.err).Msg("error fetching from replica")
				errorCount++
			}
		} else {
			received = append(received, res.entry)
		}

		// Reached the required read quorum successfully
		if len(received) >= s.config.ReadQuorum {
			break
		}

		// Reached a quorum of "not found"
		if notFoundCount >= s.config.ReadQuorum {
			break
		}

		// Quick failure detection: if too many nodes fail, we can't possibly reach quorum
		if errorCount > len(preferenceList)-s.config.ReadQuorum {
			break
		}
	}

	if len(received) >= s.config.ReadQuorum {
		// Target happy path: Return multiple versions if there are any
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(received)
	} else if notFoundCount >= s.config.ReadQuorum {
		// Quorum reached but key was not found
		http.Error(w, "key not found", http.StatusNotFound)
	} else {
		// Failed to reach any quorum (either too many errors or combination of results)
		http.Error(w, "read quorum not met", http.StatusServiceUnavailable)
	}
}

// handleInternal handles internal requests (replication PUTs and read GETs) from other nodes.
// It accesses the local store without forwarding.
func (s *server) handleInternal(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		var entry DataEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		if entry.Value == "" {
			http.Error(w, "missing value", http.StatusBadRequest)
			return
		}
		s.store.Store(key, entry)
		log.Info().Str("key", key).Str("value", entry.Value).Msg("stored via replication")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(entry)

	case http.MethodGet:
		val, ok := s.store.Load(key)
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		entry := val.(DataEntry)
		log.Info().Str("key", key).Str("value", entry.Value).Msg("read via internal get")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(entry)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// forwardRequest proxies the request to the coordinator node.
func (s *server) forwardRequest(node string, r *http.Request, w http.ResponseWriter) {
	url := fmt.Sprintf("http://%s%s?%s", node, r.URL.Path, r.URL.RawQuery)

	req, err := http.NewRequest(r.Method, url, r.Body)
	if err != nil {
		http.Error(w, "failed to create forward request", http.StatusInternalServerError)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error().Err(err).Str("node", node).Msg("failed to forward request")
		http.Error(w, "failed to forward request", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
