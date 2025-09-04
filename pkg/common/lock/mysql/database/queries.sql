-- name: UpsertLock :exec
INSERT INTO helix_locks (lock_key, owner_id, expires_at, status)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    owner_id = VALUES(owner_id),
    expires_at = VALUES(expires_at),
    status = VALUES(status),
    updated_at = CURRENT_TIMESTAMP;

-- name: TryAcquireLock :execresult
INSERT INTO helix_locks (lock_key, owner_id, expires_at, status)
VALUES (?, ?, ?, 'active')
ON DUPLICATE KEY UPDATE
    owner_id = CASE 
        WHEN expires_at <= NOW() THEN VALUES(owner_id)
        ELSE owner_id
    END,
    expires_at = CASE 
        WHEN expires_at <= NOW() THEN VALUES(expires_at)
        ELSE expires_at
    END;

-- name: CheckLockOwnership :one
SELECT owner_id, expires_at, status
FROM helix_locks
WHERE lock_key = ? AND status = 'active'
ORDER BY updated_at DESC
LIMIT 1;