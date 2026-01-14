package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gfs "eddisonso.com/go-gfs/pkg/go-gfs-sdk"
	pb "github.com/eddisonso/log-service/proto/logging"
)

const (
	ringBufferSize     = 1000
	flushInterval      = 30 * time.Second
	flushBytesThreshold = 1 << 20 // 1MB
	namespace          = "core-logging"
)

type LogServer struct {
	pb.UnimplementedLogServiceServer

	gfsClient *gfs.Client
	mu        sync.RWMutex
	ringBuf   []*pb.LogEntry
	ringHead  int
	ringTail  int
	ringCount int

	// Pending entries to flush to GFS
	pending      []*pb.LogEntry
	pendingBytes int
	pendingMu    sync.Mutex

	// WebSocket subscribers
	subscribers   map[chan *pb.LogEntry]struct{}
	subscribersMu sync.RWMutex

	// For graceful shutdown
	done chan struct{}
}

func NewLogServer(gfsClient *gfs.Client) *LogServer {
	s := &LogServer{
		gfsClient:   gfsClient,
		ringBuf:     make([]*pb.LogEntry, ringBufferSize),
		subscribers: make(map[chan *pb.LogEntry]struct{}),
		done:        make(chan struct{}),
	}
	go s.flushLoop()
	return s
}

func (s *LogServer) Close() {
	close(s.done)
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

	// Add to ring buffer
	s.addToRing(entry)

	// Add to pending flush
	s.pendingMu.Lock()
	s.pending = append(s.pending, entry)
	// Estimate size: message + source + ~50 bytes overhead for JSON
	s.pendingBytes += len(entry.Message) + len(entry.Source) + 50
	needsFlush := s.pendingBytes >= flushBytesThreshold
	s.pendingMu.Unlock()

	// Broadcast to subscribers
	s.broadcast(entry)

	// Trigger flush if threshold reached
	if needsFlush {
		go s.flush()
	}

	return &pb.PushLogResponse{}, nil
}

// StreamLogs streams log entries to clients
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

	// Send recent entries from ring buffer first
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
	// For now, return from ring buffer
	// TODO: Read from GFS for historical data
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

	// Send recent entries
	go func() {
		recent := s.getRecentEntries(source, minLevel)
		for _, entry := range recent {
			select {
			case ch <- entry:
			default:
			}
		}
	}()

	return ch, unsubscribe
}

func (s *LogServer) addToRing(entry *pb.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ringBuf[s.ringTail] = entry
	s.ringTail = (s.ringTail + 1) % ringBufferSize
	if s.ringCount < ringBufferSize {
		s.ringCount++
	} else {
		s.ringHead = (s.ringHead + 1) % ringBufferSize
	}
}

func (s *LogServer) getRecentEntries(source string, minLevel pb.LogLevel) []*pb.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]*pb.LogEntry, 0, s.ringCount)
	idx := s.ringHead
	for i := 0; i < s.ringCount; i++ {
		entry := s.ringBuf[idx]
		if matchesFilter(entry, source, minLevel) {
			entries = append(entries, entry)
		}
		idx = (idx + 1) % ringBufferSize
	}
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

func (s *LogServer) flushLoop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.flush()
		case <-s.done:
			s.flush() // Final flush
			return
		}
	}
}

func (s *LogServer) flush() {
	s.pendingMu.Lock()
	if len(s.pending) == 0 {
		s.pendingMu.Unlock()
		return
	}
	entries := s.pending
	s.pending = nil
	s.pendingBytes = 0
	s.pendingMu.Unlock()

	// Group by source
	bySource := make(map[string][]*pb.LogEntry)
	for _, e := range entries {
		bySource[e.Source] = append(bySource[e.Source], e)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for source, sourceEntries := range bySource {
		s.flushToGFS(ctx, source, sourceEntries)
	}
}

func (s *LogServer) flushToGFS(ctx context.Context, source string, entries []*pb.LogEntry) {
	if s.gfsClient == nil || len(entries) == 0 {
		return
	}

	// Build path: {source}/{date}.jsonl
	date := time.Now().Format("2006-01-02")
	path := fmt.Sprintf("/%s/%s.jsonl", source, date)

	// Convert entries to JSONL
	var buf bytes.Buffer
	for _, e := range entries {
		data, err := json.Marshal(e)
		if err != nil {
			slog.Error("failed to marshal log entry", "error", err)
			continue
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	// Append to file
	_, err := s.gfsClient.AppendWithNamespace(ctx, path, namespace, buf.Bytes())
	if err != nil {
		slog.Error("failed to append logs to GFS", "source", source, "error", err)
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
