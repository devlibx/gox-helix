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