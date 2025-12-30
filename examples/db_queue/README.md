# DB Queue Example

This example demonstrates a simple database-backed job queue using MySQL.

## 1. Setup

First, create the `jobs` table in your MySQL database.

```sql
-- The `jobs` table stores individual tasks to be processed by the workers.
CREATE TABLE jobs
(
    -- The primary key, a ULID, which is lexicographically sortable and ideal for job queues.
    id           VARCHAR(26) PRIMARY KEY,

    -- The domain and tasklist categorize the job, used by gox-helix for routing.
    domain       VARCHAR(64)  NOT NULL,
    tasklist     VARCHAR(64)  NOT NULL,

    -- The partition key. gox-helix ensures that only one worker processes a given partition at a time,
    -- which is the basis for our lock-free queue processing.
    partition_id INT UNSIGNED NOT NULL,

    -- The status of the job.
    -- 'pending': Ready to be picked up by a worker.
    -- 'in_progress': Currently being processed by a worker.
    -- 'completed': Successfully processed.
    -- 'failed': An error occurred during processing.
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',

    -- The job's data, stored in JSON format.
    payload      JSON         NOT NULL,

    -- Standard timestamps for tracking.
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Constraint to ensure the status is one of the allowed values.
    CONSTRAINT status_check CHECK (status IN ('created', 'pending', 'in_progress', 'completed', 'failed')),

    -- The primary index for our queue polling query.
    -- This index allows workers to efficiently find the next pending job for a specific partition.
    INDEX job_queue_idx (domain, tasklist, partition_id, status, id)
);
```

## 2. Run Producer

The producer inserts jobs into the `jobs` table.

```bash
# Truncate the jobs table and run the producer
mysql -u root -pcredroot -e "TRUNCATE TABLE automation.jobs;"; go run producer/main.go
```

## 3. Run Consumer

The consumer fetches and processes jobs from the `jobs` table. You can specify different algorithms for fetching the next job.

```bash
go run consumer/main.go --algo <algorithm>
```

Replace `<algorithm>` with one of the following:

*   `GetNextJob`
*   `GetNextJobForUpdate`
*   `GetNextJobMin`
*   `GetNextJobMinForUpdate`

Example:

```bash
go run consumer/main.go --algo GetNextJobMin
```

## 4. Consumer Algorithms

Here are the different algorithms the consumer can use to fetch the next job, along with the corresponding SQL queries from `database/queries.sql`.

### `GetNextJob`

Retrieves the next pending job for a specific partition, ordered by ID.

```sql
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
```

### `GetNextJobForUpdate`

Retrieves the next pending job for a specific partition using a pessimistic lock (`FOR UPDATE SKIP LOCKED`). This prevents other transactions from accessing the same job.

```sql
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
```

### `GetNextJobMin`

Retrieves the next pending job for a specific partition using a `MIN(id)` subquery. This can be more efficient than `ORDER BY` and `LIMIT 1` in some cases.

```sql
-- name: GetNextJobMin :one
-- Retrieves the next pending job for a specific partition using a MIN(id) subquery.
SELECT MIN(id)
FROM jobs
WHERE domain = ?
  AND tasklist = ?
  AND partition_id = ?
  AND status = 'created';
```

### `GetNextJobMinForUpdate`

Retrieves the next pending job for a specific partition using a `MIN(id)` subquery and a pessimistic lock.

```sql
-- name: GetNextJobMinForUpdate :one
-- Retrieves the next pending job for a specific partition using a MIN(id) subquery and a pessimistic lock.
SELECT MIN(id)
FROM jobs
WHERE domain = ?
  AND tasklist = ?
  AND partition_id = ?
  AND status = 'created' for
update skip locked;
```

## 5. Results

Here are some performance metrics for `GetNextJob` and `GetNextJobMin`.

### `GetNextJob`

```
2025/12/31 00:32:01 INFO Flushing metrics algo=GetNextJob count=12531 rps=1253 99=2.435268ms 999=13.608824ms
2025/12/31 00:32:11 INFO Flushing metrics algo=GetNextJob count=15378 rps=1537 99=2.690798ms 999=7.949665ms
2025/12/31 00:32:21 INFO Flushing metrics algo=GetNextJob count=15061 rps=1506 99=2.605748ms 999=17.262827ms
2025/12/31 00:32:31 INFO Flushing metrics algo=GetNextJob count=14354 rps=1435 99=2.413333ms 999=7.342532ms
2025/12/31 00:32:41 INFO Flushing metrics algo=GetNextJob count=14621 rps=1462 99=2.587732ms 999=17.67804ms
```

### `GetNextJobMin`

```
2025/12/31 00:32:59 INFO Flushing metrics algo=GetNextJobMin count=13251 rps=1325 99=135.042µs 999=663.767µs
2025/12/31 00:33:09 INFO Flushing metrics algo=GetNextJobMin count=377015 rps=37701 99=122.642µs 999=289.562µs
2025/12/31 00:33:19 INFO Flushing metrics algo=GetNextJobMin count=377751 rps=37775 99=130.042µs 999=203.54µs
2025/12/31 00:33:29 INFO Flushing metrics algo=GetNextJobMin count=385310 rps=38531 99=111.039µs 999=229.593µs
2025/12/31 00:33:39 INFO Flushing metrics algo=GetNextJobMin count=356984 rps=35698 99=253.515µs 999=947.238µs
2025/12/31 00:33:49 INFO Flushing metrics algo=GetNextJobMin count=375263 rps=37526 99=157.512µs 999=459.648µs
2_025/12/31 00:33:59 INFO Flushing metrics algo=GetNextJobMin count=379581 rps=37958 99=131.995µs 999=271.462µs
2025/12/31 00:34:09 INFO Flushing metrics algo=GetNextJobMin count=377992 rps=37799 99=111.205µs 999=332.964µs
2025/12/31 00:34:19 INFO Flushing metrics algo=GetNextJobMin count=375610 rps=37561 99=130.35µs 999=229.615µs
2025/12/31 00:34:29 INFO Flushing metrics algo=GetNextJobMin count=370017 rps=37001 99=145.461µs 999=748.969µs
```

### Analysis

As you can see from the results, `GetNextJobMin` is significantly more performant than `GetNextJob`.

*   **Throughput (rps):** `GetNextJobMin` achieves a throughput of ~37,000 requests per second, while `GetNextJob` only achieves ~1,500 rps. This is a performance improvement of over 20x.
*   **Latency (99th percentile):** `GetNextJobMin` has a 99th percentile latency of ~130µs, while `GetNextJob` has a latency of ~2.6ms (2600µs). This means `GetNextJobMin` is about 20x faster.

The reason for this performance difference is that `GetNextJobMin` uses a `MIN(id)` subquery, which is a more efficient way to find the next available job than using `ORDER BY ... LIMIT 1`. The `ORDER BY` clause requires the database to sort the rows before it can find the first one, which is a much more expensive operation. This demonstrates how a small change in the query can have a huge impact on performance, especially for high-throughput systems like job queues. In this case, `GetNextJobMin` is not just 8-10x better, but up to **20x better** in terms of both throughput and latency.
