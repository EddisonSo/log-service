package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gfs "eddisonso.com/go-gfs/pkg/go-gfs-sdk"
	pb "github.com/eddisonso/log-service/proto/logging"
)

const bufferSize = 1000

// bufferKey creates a unique key for source+level combination
func bufferKey(source string, level pb.LogLevel) string {
	return source + ":" + level.String()
}

// RingBuffer is a simple circular buffer for log entries
type RingBuffer struct {
	entries []*pb.LogEntry
	head    int
	count   int
	mu      sync.RWMutex
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		entries: make([]*pb.LogEntry, size),
	}
}

func (r *RingBuffer) Add(entry *pb.LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx := (r.head + r.count) % len(r.entries)
	r.entries[idx] = entry
	if r.count < len(r.entries) {
		r.count++
	} else {
		r.head = (r.head + 1) % len(r.entries)
	}
}

func (r *RingBuffer) GetAll() []*pb.LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*pb.LogEntry, 0, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.head + i) % len(r.entries)
		result = append(result, r.entries[idx])
	}
	return result
}

type LogServer struct {
	pb.UnimplementedLogServiceServer

	// Per-source, per-level ring buffers
	buffers   map[string]*RingBuffer
	buffersMu sync.RWMutex

	// Track known sources for enumeration
	sources   map[string]struct{}
	sourcesMu sync.RWMutex

	// WebSocket subscribers
	subscribers   map[chan *pb.LogEntry]struct{}
	subscribersMu sync.RWMutex

	// GFS client for persistence (optional)
	gfsClient *gfs.Client

	// Channel for async persistence
	persistCh chan *pb.LogEntry

	// For graceful shutdown
	done chan struct{}
}

func NewLogServer(gfsClient *gfs.Client) *LogServer {
	s := &LogServer{
		buffers:     make(map[string]*RingBuffer),
		sources:     make(map[string]struct{}),
		subscribers: make(map[chan *pb.LogEntry]struct{}),
		gfsClient:   gfsClient,
		persistCh:   make(chan *pb.LogEntry, 1000),
		done:        make(chan struct{}),
	}

	// Start persistence worker if GFS client is configured
	if gfsClient != nil {
		go s.persistenceWorker()
	}

	return s
}

func (s *LogServer) Close() {
	close(s.done)
}

// getOrCreateBuffer gets or creates a ring buffer for source+level
func (s *LogServer) getOrCreateBuffer(source string, level pb.LogLevel) *RingBuffer {
	key := bufferKey(source, level)

	s.buffersMu.RLock()
	buf, exists := s.buffers[key]
	s.buffersMu.RUnlock()

	if exists {
		return buf
	}

	s.buffersMu.Lock()
	defer s.buffersMu.Unlock()

	// Double-check after acquiring write lock
	if buf, exists = s.buffers[key]; exists {
		return buf
	}

	buf = NewRingBuffer(bufferSize)
	s.buffers[key] = buf
	return buf
}

// trackSource adds a source to the known sources set
func (s *LogServer) trackSource(source string) {
	s.sourcesMu.Lock()
	s.sources[source] = struct{}{}
	s.sourcesMu.Unlock()
}

// GetSources returns all known log sources
func (s *LogServer) GetSources() []string {
	s.sourcesMu.RLock()
	defer s.sourcesMu.RUnlock()

	result := make([]string, 0, len(s.sources))
	for src := range s.sources {
		result = append(result, src)
	}
	return result
}

// PushLog receives a log entry from a service
func (s *LogServer) PushLog(ctx context.Context, req *pb.PushLogRequest) (*pb.PushLogResponse, error) {
	if req.Entry == nil {
		return &pb.PushLogResponse{}, nil
	}

	entry := req.Entry
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().Unix()
	}

	// Track source
	s.trackSource(entry.Source)

	// Add to appropriate ring buffer
	buf := s.getOrCreateBuffer(entry.Source, entry.Level)
	buf.Add(entry)

	// Broadcast to subscribers
	s.broadcast(entry)

	// Queue for persistence (non-blocking)
	if s.gfsClient != nil {
		select {
		case s.persistCh <- entry:
		default:
			// Channel full, drop (logs are best-effort persisted)
		}
	}

	return &pb.PushLogResponse{}, nil
}

// StreamLogs streams log entries to clients (gRPC)
func (s *LogServer) StreamLogs(req *pb.StreamLogsRequest, stream pb.LogService_StreamLogsServer) error {
	ch := make(chan *pb.LogEntry, 100)

	// Register subscriber
	s.subscribersMu.Lock()
	s.subscribers[ch] = struct{}{}
	s.subscribersMu.Unlock()

	defer func() {
		s.subscribersMu.Lock()
		delete(s.subscribers, ch)
		s.subscribersMu.Unlock()
		close(ch)
	}()

	// Send recent entries from ring buffers first
	recent := s.getRecentEntries(req.Source, req.MinLevel)
	for _, entry := range recent {
		if err := stream.Send(entry); err != nil {
			return err
		}
	}

	// Stream new entries
	for {
		select {
		case entry, ok := <-ch:
			if !ok {
				return nil
			}
			if matchesFilter(entry, req.Source, req.MinLevel) {
				if err := stream.Send(entry); err != nil {
					return err
				}
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// GetLogs returns historical log entries
func (s *LogServer) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	entries := s.getRecentEntries(req.Source, req.MinLevel)

	// Apply since filter
	if req.Since > 0 {
		filtered := make([]*pb.LogEntry, 0)
		for _, e := range entries {
			if e.Timestamp >= req.Since {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}

	// Apply limit
	if req.Limit > 0 && int(req.Limit) < len(entries) {
		entries = entries[len(entries)-int(req.Limit):]
	}

	return &pb.GetLogsResponse{Entries: entries}, nil
}

// Subscribe creates a channel for receiving log entries (used by WebSocket handler)
func (s *LogServer) Subscribe(source string, minLevel pb.LogLevel) (<-chan *pb.LogEntry, func()) {
	ch := make(chan *pb.LogEntry, 100)

	s.subscribersMu.Lock()
	s.subscribers[ch] = struct{}{}
	s.subscribersMu.Unlock()

	unsubscribe := func() {
		s.subscribersMu.Lock()
		delete(s.subscribers, ch)
		s.subscribersMu.Unlock()
	}

	// Send recent entries in background
	go func() {
		recent := s.getRecentEntries(source, minLevel)
		for _, entry := range recent {
			select {
			case ch <- entry:
			default:
				// Channel full, skip
			}
		}
	}()

	return ch, unsubscribe
}

// getRecentEntries retrieves entries from ring buffers matching the filter
func (s *LogServer) getRecentEntries(source string, minLevel pb.LogLevel) []*pb.LogEntry {
	s.buffersMu.RLock()
	defer s.buffersMu.RUnlock()

	var entries []*pb.LogEntry

	// Collect from matching buffers
	for _, buf := range s.buffers {
		bufEntries := buf.GetAll()
		for _, entry := range bufEntries {
			if matchesFilter(entry, source, minLevel) {
				entries = append(entries, entry)
			}
		}
	}

	// Sort by timestamp
	sortByTimestamp(entries)

	return entries
}

func (s *LogServer) broadcast(entry *pb.LogEntry) {
	s.subscribersMu.RLock()
	defer s.subscribersMu.RUnlock()

	for ch := range s.subscribers {
		select {
		case ch <- entry:
		default:
			// Drop if subscriber is slow
		}
	}
}

func matchesFilter(entry *pb.LogEntry, source string, minLevel pb.LogLevel) bool {
	if entry == nil {
		return false
	}
	if source != "" && entry.Source != source {
		return false
	}
	if entry.Level < minLevel {
		return false
	}
	return true
}

// sortByTimestamp sorts entries by timestamp (oldest first)
func sortByTimestamp(entries []*pb.LogEntry) {
	// Simple insertion sort - entries are mostly sorted already
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].Timestamp < entries[j-1].Timestamp; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
}

// persistenceWorker batches and writes logs to GFS
func (s *LogServer) persistenceWorker() {
	const (
		batchSize     = 500
		flushInterval = 1 * time.Minute
		namespace     = "core-logs"
	)

	batch := make([]*pb.LogEntry, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Group by date and source for organized storage
		groups := make(map[string][]*pb.LogEntry)
		for _, entry := range batch {
			t := time.Unix(entry.Timestamp, 0).UTC()
			key := fmt.Sprintf("/%s/%s.jsonl", t.Format("2006-01-02"), entry.Source)
			groups[key] = append(groups[key], entry)
		}

		// Write each group to GFS (SDK auto-creates files if needed)
		ctx := context.Background()
		for path, entries := range groups {
			var data []byte
			for _, entry := range entries {
				line, err := json.Marshal(entry)
				if err != nil {
					continue
				}
				data = append(data, line...)
				data = append(data, '\n')
			}

			if _, err := s.gfsClient.AppendWithNamespace(ctx, path, namespace, data); err != nil {
				slog.Warn("failed to persist logs", "path", path, "error", err)
			}
		}

		batch = batch[:0]
	}

	for {
		select {
		case entry := <-s.persistCh:
			batch = append(batch, entry)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.done:
			flush()
			return
		}
	}
}
