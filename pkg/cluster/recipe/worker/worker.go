package worker

import (
	"context"
	"time"
)

// TimeService provides an abstraction for getting the current time,
// allowing for deterministic testing by using a mock implementation.
type TimeService interface {
	Now() time.Time
}

// Config holds the configuration for a worker.
type Config struct {
	Domain            string
	HeartbeatInterval time.Duration
}

// Worker is the interface for a helix worker. It defines the lifecycle methods
// for starting, stopping, and identifying a worker instance.
type Worker interface {
	// Start begins the worker's lifecycle. This includes registering the worker
	// in the database and starting the background heartbeat process.
	// The method should block until the worker is stopped or an error occurs.
	Start(ctx context.Context) error

	// Stop gracefully terminates the worker's lifecycle. It signals the
	// heartbeat loop to cease and cleans up any resources.
	Stop()

	// ID returns the unique identifier (UUID) of the worker instance.
	ID() string
}
