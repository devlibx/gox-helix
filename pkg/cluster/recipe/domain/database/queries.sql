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