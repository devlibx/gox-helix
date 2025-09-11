-- name: GetLockByLockKey :one
SELECT /*+ MAX_EXECUTION_TIME(1000) */ owner_id, expires_at, epoch
FROM helix_locks
WHERE lock_key = ?
  AND status = 1;

-- name: TryUpsertLock :exec
INSERT /*+ MAX_EXECUTION_TIME(1000) */ INTO helix_locks (lock_key, owner_id, expires_at, epoch, status)
VALUES (?, ?, ?, 1, 1)
ON DUPLICATE KEY
    UPDATE owner_id   = IF(owner_id = VALUES(owner_id) OR expires_at < ?, VALUES(owner_id), owner_id),
           expires_at = IF(owner_id = VALUES(owner_id) OR expires_at < ?, VALUES(expires_at), expires_at),
           epoch      = IF(owner_id = VALUES(owner_id) OR expires_at < ?, epoch + 1, epoch)