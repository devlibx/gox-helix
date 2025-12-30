-- name: CreateJob :exec
-- Inserts a new job into the jobs table.
INSERT INTO jobs (id, domain, tasklist, partition_id, status, payload)
VALUES (?, ?, ?, ?, ?, ?);

-- name: GetNextJob :one
-- Retrieves the next pending job for a specific partition, ordered by ID.
-- This is the core query for our consumer.
SELECT id
FROM jobs
WHERE domain = ?
  AND tasklist = ?
  AND partition_id = ?
  AND status = 'created'
ORDER BY domain, tasklist, partition_id, status, id ASC
LIMIT 1;

-- name: GetNextJobForUpdate :one
-- Retrieves the next pending job for a specific partition using a pessimistic lock.
SELECT id
FROM jobs
WHERE domain = ?
  AND tasklist = ?
  AND partition_id = ?
  AND status = 'created'
ORDER BY domain, tasklist, partition_id, status, id ASC
LIMIT 1
for
update skip locked;

-- name: GetNextJobMin :one
-- Retrieves the next pending job for a specific partition using a MIN(id) subquery.
SELECT MIN(id)
FROM jobs
WHERE domain = ?
  AND tasklist = ?
  AND partition_id = ?
  AND status = 'created';


-- name: GetNextJobMinForUpdate :one
-- Retrieves the next pending job for a specific partition using a MIN(id) subquery and a pessimistic lock.
SELECT MIN(id)
FROM jobs
WHERE domain = ?
  AND tasklist = ?
  AND partition_id = ?
  AND status = 'created' for
update skip locked;


-- name: UpdateJobStatus :exec
-- Updates the status of a specific job.
UPDATE jobs
SET status = ?
WHERE id = ?;