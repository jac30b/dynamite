package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync"

	"github.com/jac30b/dynamite/partition"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ReplicationFactor int      `yaml:"replication_factor"`
	ReadQuorum        int      `yaml:"read_quorum"`
	WriteQuorum       int      `yaml:"write_quorum"`
	Self              string   `yaml:"self"`
	Nodes             []string `yaml:"nodes"`
}

type server struct {
	ring   *partition.Ring
	config Config
	store  sync.Map
}

func main() {
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to read config")
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

	log.Fatal().Err(http.ListenAndServe(":8080", nil)).Msg("server stopped")
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
		s.handleGet(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePut stores the key-value pair locally and replicates to the preference list.
func (s *server) handlePut(w http.ResponseWriter, r *http.Request, key string, preferenceList []string) {
	value := r.URL.Query().Get("value")
	if value == "" {
		http.Error(w, "missing value", http.StatusBadRequest)
		return
	}

	// Store locally
	s.store.Store(key, value)
	log.Info().Str("key", key).Str("value", value).Msg("stored locally")

	// Replicate to the remaining nodes in the preference list
	for _, node := range preferenceList[1:] {
		if err := s.replicate(node, key, value); err != nil {
			log.Error().Err(err).Str("node", node).Str("key", key).Msg("replication failed")
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "stored on %s", s.config.Self)
}

// handleGet reads the value for the key from the local store.
func (s *server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	value, ok := s.store.Load(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	log.Info().Str("key", key).Str("value", value.(string)).Msg("read locally")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value.(string)))
}

// handleInternal handles replication requests from other nodes.
// It stores directly without forwarding.
func (s *server) handleInternal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w, "missing key or value", http.StatusBadRequest)
		return
	}

	s.store.Store(key, value)
	log.Info().Str("key", key).Str("value", value).Msg("stored via replication")

	w.WriteHeader(http.StatusOK)
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
