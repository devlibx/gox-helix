-- name: UpsertPartition :exec
INSERT /*+ MAX_EXECUTION_TIME(1000) */
INTO helix_worker_partition_mapping(domain, tasklist, owner_id, metadata, status)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE metadata=VALUES(metadata),
                        status=VALUES(status),
                        owner_id=VALUES(owner_id);

-- name: MarkPartitionInactive :exec
UPDATE helix_worker_partition_mapping
SET status=0
WHERE domain = ?
  and tasklist = ?;

-- name: MarkPartitionAssigned :exec
UPDATE helix_worker_partition_mapping
SET status=1
WHERE domain = ?
  and tasklist = ?;


-- name: MarkPartitionUnassigned :exec
UPDATE helix_worker_partition_mapping
SET status=2
WHERE domain = ?
  and tasklist = ?;


-- name: GetPartitionByOwnerId :one
SELECT /*+ MAX_EXECUTION_TIME(1000) */
    *
FROM helix_worker_partition_mapping
WHERE domain = ?
  AND tasklist = ?;

-- name: GetValidPartitionByOwnerId :one
SELECT /*+ MAX_EXECUTION_TIME(1000) */
    *
FROM helix_worker_partition_mapping
WHERE domain = ?
  AND tasklist = ?
  AND status in (1, 2);


-- name: GetAllPartitionForDomainAndTaskList :many
SELECT /*+ MAX_EXECUTION_TIME(1000) */
    *
FROM helix_worker_partition_mapping
WHERE domain = ?
  AND tasklist = ?;

-- name: GetAllValidPartitionForDomainAndTaskList :many
SELECT /*+ MAX_EXECUTION_TIME(1000) */
    *
FROM helix_worker_partition_mapping
WHERE domain = ?
  AND tasklist = ?
  AND status in (1, 2);
