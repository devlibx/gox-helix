# Helix Worker Component

This document covers the design principles, lifecycle, and concepts related to the `worker` component in the gox-helix project.

## Core Concepts

A **Worker** is a runtime instance responsible for executing tasks within the helix framework. It is designed to be a fundamental building block for creating distributed, scalable, and resilient systems.

- **Domain Association:** Each worker belongs to a specific `domain`.
- **Unique Identity:** Every worker has a unique `worker_id` (UUID) to distinguish it from all other workers, even those on the same host.
- **Coordinator Role:** While all workers can execute tasks, a worker can also assume the special role of a `coordinator`. A coordinator acquires a distributed lock on a `(domain, tasklist)` pair to manage work distribution for other workers, similar to a Kafka consumer group coordinator.

## Worker Lifecycle

The worker lifecycle is managed through a `Start()` and `Stop()` interface and is underpinned by a database registration and heartbeating mechanism.

1.  **Registration:**
    - On `Start()`, the worker generates a unique ID and registers itself in the `helix_workers` database table.
    - Its initial `status` is set to `active`, and its `created_at` and `last_heartbeat_at` timestamps are recorded.

2.  **Heartbeating:**
    - After starting, the worker runs a background goroutine that sends a heartbeat at a regular interval (e.g., every 10 seconds).
    - The heartbeat is an `UPDATE` statement that refreshes the `last_heartbeat_at` timestamp in the database for its `worker_id`.

3.  **Health Checking & Self-Termination:**
    - With each heartbeat, the worker also queries its own `status` from the database.
    - A separate, external **Monitor** process (to be built later) is responsible for checking for stale heartbeats. If a worker's `last_heartbeat_at` is too old, the monitor will update its `status` to `inactive`.
    - If the worker detects that its status has been changed to `inactive`, it will initiate a graceful shutdown by stopping its heartbeat loop and terminating itself. This prevents "zombie" workers from continuing to run when they are considered dead by the rest of the system.

4.  **Termination:**
    - The `Stop()` method can be called to gracefully shut down the worker. This stops the heartbeat loop and cancels any ongoing work.
