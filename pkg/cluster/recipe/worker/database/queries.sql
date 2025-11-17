-- name: RegisterWorker :exec
INSERT INTO helix_workers (worker_id, domain, status, created_at, last_heartbeat_at)
VALUES (?, ?, 'active', ?, ?);

-- name: SendHeartbeat :exec
UPDATE helix_workers
SET last_heartbeat_at = ?
WHERE domain = ?
  AND worker_id = ?
  AND status = 'active';

-- name: DeregisterWorker :exec
UPDATE helix_workers
SET status = 'inactive'
WHERE domain = ?
  AND worker_id = ?;


-- name: GetWorkerStatus :one
SELECT status
FROM helix_workers
WHERE domain = ?
  AND worker_id = ?;

-- name: GetWorker :one
SELECT worker_id, domain, status, created_at, last_heartbeat_at, updated_at
FROM helix_workers
WHERE worker_id = ?
  AND domain = ?;
