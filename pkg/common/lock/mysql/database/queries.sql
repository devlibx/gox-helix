-- name: UpsertLock :exec
INSERT INTO helix_locks (lock_key, owner_id, expires_at, status)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    owner_id = VALUES(owner_id),
    expires_at = VALUES(expires_at),
    status = VALUES(status),
    updated_at = CURRENT_TIMESTAMP;