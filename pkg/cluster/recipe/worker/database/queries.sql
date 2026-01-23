-- name: RegisterWorker :exec
INSERT INTO helix_workers (worker_id, domain, status, created_at, last_heartbeat_at)
VALUES (?, ?, 1, ?, ?);

-- name: SendHeartbeat :execresult
UPDATE helix_workers
SET last_heartbeat_at = ?
WHERE domain = ?
  AND worker_id = ?
  AND status = 1;

-- name: DeregisterWorker :exec
UPDATE helix_workers
SET status = 0
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

-- name: GetAllActiveWorkersByDomain :many
SELECT worker_id
FROM helix_workers
WHERE domain = ?
  and status = 1;


-- name: GetWorkerByWorkerIdAndDomain :one
SELECT *
FROM helix_workers
WHERE domain = ?
  and worker_id = ?;

-- name: MarkInactiveWorkers :exec
UPDATE helix_workers
SET status = 0
WHERE last_heartbeat_at < ?
  AND status = 1;

