-- name: GetLockByLockKey :one
SELECT owner_id, expires_at, epoch
FROM helix_locks
WHERE lock_key = ?
  AND status = 'active';

-- name: TryUpsertLock :exec
INSERT INTO helix_locks (lock_key, owner_id, expires_at, epoch, status)
VALUES (?, ?, ?, 1, 'active')
ON DUPLICATE KEY
    UPDATE expires_at = (@orig_expires := expires_at),
           expires_at = IF(@orig_expires < VALUES(expires_at), VALUES(expires_at), @orig_expires),
           owner_id   = IF(@orig_expires < VALUES(expires_at), VALUES(owner_id), owner_id),
           epoch      = IF(@orig_expires < VALUES(expires_at), epoch + 1, epoch);