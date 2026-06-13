package bbdb

import "time"

// Event is a single event to write to BBDB.
// PartitionKey is optional — if empty, the server assigns a UUID.
// EventType must be 1–255 (0 is reserved as "all types" in queries).
// Timestamp defaults to time.Now() if zero.
// Payload is raw bytes (typically JSON).
type Event struct {
	PartitionKey []byte
	EventType    uint8
	Timestamp    time.Time
	Payload      []byte
}

// WriteResult is the acknowledgement from the server for one batch.
type WriteResult struct {
	BatchID       string
	Accepted      uint32
	PartitionKeys [][]byte // resolved keys, len == len(batch events)
	Err           error    // non-nil if the server reported an error
}

// QueryResult holds all events returned by a Query call.
type QueryResult struct {
	Events []QueryEvent
	Total  uint64
}

// QueryEvent is a single event returned from a Query.
type QueryEvent struct {
	PartitionKey []byte
	EventType    uint8
	Timestamp    time.Time
	Payload      []byte
}
