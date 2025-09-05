-- name: GetLockByLockKey :one
SELECT owner_id, expires_at, epoch
FROM helix_locks
WHERE lock_key = ?
  AND status = 'active';

-- name: TryUpsertLock :exec
INSERT INTO helix_locks (lock_key, owner_id, expires_at, epoch, status)
VALUES (?, ?, ?, 1, 'active')
ON DUPLICATE KEY
    UPDATE owner_id   = CASE WHEN expires_at < VALUES(expires_at) THEN VALUES(owner_id) ELSE owner_id END,
           expires_at = CASE WHEN expires_at < VALUES(expires_at) THEN VALUES(expires_at) ELSE expires_at END,
           epoch = CASE WHEN expires_at < VALUES(expires_at) THEN epoch + 1 ELSE epoch END;

