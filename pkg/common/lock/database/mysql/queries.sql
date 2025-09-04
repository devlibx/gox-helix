-- name: CreateLock :exec
INSERT INTO locks (lock_key, owner_id, expires_at)
VALUES (?, ?, ?);

-- name: GetLock :one
SELECT * FROM locks
WHERE lock_key = ?;

-- name: UpdateLock :exec
UPDATE locks
SET owner_id = ?, expires_at = ?
WHERE lock_key = ?;

-- name: RenewLock :exec
UPDATE locks
SET expires_at = ?
WHERE lock_key = ? AND owner_id = ?;

-- name: TryAcquireLock :exec
INSERT INTO locks (lock_key, owner_id, expires_at)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE
    owner_id = IF(expires_at < NOW(), VALUES(owner_id), owner_id),
    expires_at = IF(expires_at < NOW(), VALUES(expires_at), expires_at);
