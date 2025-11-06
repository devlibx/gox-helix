-- name: UpsertTasklist :exec
INSERT /*+ MAX_EXECUTION_TIME(1000) */
INTO helix_domain (domain, tasklist, metadata, partition_count, status)
VALUES (?, ?, ?, ?, 1)
ON DUPLICATE KEY UPDATE metadata        = VALUES(metadata),
                        partition_count = VALUES(partition_count),
                        status          = 1;

-- name: GetDomainByDomainAndTasklist :one
SELECT /*+ MAX_EXECUTION_TIME(1000) */
    id,
    domain,
    tasklist,
    metadata,
    partition_count,
    status,
    created_at,
    updated_at
FROM helix_domain
WHERE domain = ?
  AND tasklist = ?
  AND status = 1;

-- name: GetDomainsByDomain :many
SELECT /*+ MAX_EXECUTION_TIME(1000) */
    id,
    domain,
    tasklist,
    metadata,
    partition_count,
    status,
    created_at,
    updated_at
FROM helix_domain
WHERE domain = ?
  AND status = 1;

-- name: InsertDomainWorker :exec
INSERT /*+ MAX_EXECUTION_TIME(1000) */
INTO helix_worker (domain, unique_id, metadata, last_hb_time, status, version)
VALUES (?, ?, ?, ?, 1, 1);

-- name: AcquireLock :execresult
INSERT /*+ MAX_EXECUTION_TIME(1000) */ INTO helix_locks
    (domain, lock_key, owner_id, expires_at, epoch, status)
VALUES (?, ?, ?, ?, 1, 1)
ON DUPLICATE KEY UPDATE
    status = IF((@original_owner_id := owner_id) IS NOT NULL, 1, 1),
    owner_id = IF(
        @original_owner_id = VALUES(owner_id) OR expires_at < ?,
        VALUES(owner_id),
        owner_id
    ),
    expires_at = IF(
        @original_owner_id = VALUES(owner_id) OR expires_at < ?,
        VALUES(expires_at),
        expires_at
    ),
    epoch = IF(
        @original_owner_id != VALUES(owner_id) AND expires_at < ?,
        epoch + 1,
        epoch
    );