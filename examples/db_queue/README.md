# Database Job Queue with gox-helix

This example demonstrates how to build a scalable, database-backed job queue using `gox-helix`.
It consists of two main parts:
1.  A **Producer** that populates a `jobs` table in a MySQL database with tasks.
2.  A **Consumer** built with `gox-helix` that concurrently processes these jobs, using database partitions to ensure that each job is processed exactly once without requiring row-level database locks.

## Prerequisites
- Go 1.21+
- A running MySQL 8.0 instance.
- `sqlc` for code generation (the generated code is already checked in, but you'll need it if you modify the queries). You can install it via:
  ```bash
  go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
  ```

## How It Works

The key idea is to leverage `gox-helix`'s partition management.
1.  Jobs are inserted into the `jobs` table with a `partition_id`.
2.  `gox-helix` assigns partitions to different consumer instances (workers).
3.  Because `gox-helix` guarantees that only one worker can own a partition at any given time, our consumer can poll for jobs within its assigned partition (`WHERE partition_id = ?`) without needing to use `SELECT ... FOR UPDATE` or other locking mechanisms. This avoids lock contention and improves scalability.

## Setup

### 1. Create the `jobs` Table
Connect to your MySQL instance (e.g., via `mysql -u root -pcredroot automation`) and run the schema definition to create the `jobs` table.
You can find the schema at `examples/db_queue/database/schema.sql`.

```bash
# Example: Pipe the schema directly if MySQL is accessible via CLI
cat examples/db_queue/database/schema.sql | mysql -u root -pcredroot automation
```

## How to Run

### Step 1: Run the Job Producer
The producer is a simple Go application that concurrently inserts thousands of jobs into the `jobs` table for the consumer to process.

Open a terminal and run:
```bash
go run examples/db_queue/producer/main.go
```
You will see logs indicating that jobs are being inserted. Wait for it to complete.

### Step 2: Run the Job Consumer
The consumer is a `gox-helix` worker that will connect to the database, get its assigned partitions, and start processing jobs from the queue.

Open another terminal and run:
```bash
go run examples/db_queue/consumer/main.go
```

You will see logs from `gox-helix` as it starts up, followed by logs from the consumer as it processes jobs, for example:
```
INFO Processing job job_id=01J8X... producer_id=1...
INFO Processing job job_id=01J8Y... producer_id=2...
```

You can start multiple instances of the consumer, and `gox-helix` will automatically rebalance the partitions among them.
