-- name: AcquireLock :execresult
INSERT /*+ MAX_EXECUTION_TIME(1000) */ INTO helix_locks
    (domain, lock_key, owner_id, expires_at, epoch, status)
VALUES (?, ?, ?, ?, 1, 1)
ON DUPLICATE KEY UPDATE
    status = IF((@original_owner_id := owner_id) IS NOT NULL AND (@original_expires_at := expires_at) IS NOT NULL, 1, 1),
    owner_id = IF(
        @original_owner_id = VALUES(owner_id) OR @original_expires_at < ?,
        VALUES(owner_id),
        owner_id
    ),
    expires_at = IF(
        @original_owner_id = VALUES(owner_id) OR @original_expires_at < ?,
        VALUES(expires_at),
        expires_at
    ),
    epoch = IF(
        @original_owner_id != VALUES(owner_id) AND @original_expires_at < ?,
        epoch + 1,
        epoch
    );

-- name: ReleaseLock :execresult
UPDATE helix_locks
SET owner_id = '', expires_at = ?
WHERE domain = ?
  AND lock_key = ?
  AND owner_id = ?;