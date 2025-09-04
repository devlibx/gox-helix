-- name: CreateDomain :exec
INSERT INTO domains (name)
VALUES (?);

-- name: GetDomain :one
SELECT * FROM domains
WHERE id = ?;

-- name: ListDomains :many
SELECT * FROM domains
ORDER BY name;

-- name: CreateQueue :exec
INSERT INTO queues (domain_id, name)
VALUES (?, ?);

-- name: GetQueue :one
SELECT * FROM queues
WHERE id = ?;

-- name: ListQueues :many
SELECT * FROM queues
WHERE domain_id = ?
ORDER BY name;

-- name: CreatePartition :exec
INSERT INTO partitions (queue_id, name)
VALUES (?, ?);

-- name: GetPartition :one
SELECT * FROM partitions
WHERE id = ?;

-- name: ListPartitions :many
SELECT * FROM partitions
WHERE queue_id = ?
ORDER BY name;