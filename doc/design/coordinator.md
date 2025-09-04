# Coordinator Election and Failover

This document outlines the design for coordinator election, ensuring a single active coordinator and handling failover.

## 1. Functional Requirements

*   **Single Leader:** Only one worker node can be the coordinator at any given time.
*   **Leader Election:** A mechanism for a non-coordinator node to become the coordinator if one does not exist.
*   **Failover:** If the current coordinator fails, another node must be able to take over as the new coordinator.
*   **Failure Detection:** The system must detect the failure of the current coordinator in a timely manner.

## 2. Non-Functional Requirements

*   **Reliability:** The leader election process must be reliable and prevent split-brain scenarios (multiple coordinators).
*   **Performance:** The overhead of the leader election and health-checking mechanism should be minimal.
*   **Simplicity:** The implementation should be as simple as possible while still meeting the requirements.

## 3. Proposed Approach

We will use a distributed lock implemented in MySQL to manage coordinator election. A dedicated `locks` table will be used for this purpose.

### 3.1. Database Schema for Locking

We will create a `locks` table with the following schema:

```sql
CREATE TABLE locks (
    lock_key VARCHAR(255) PRIMARY KEY,
    owner_id VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP NOT NULL
);
```

*   `lock_key`: A unique name for the lock. For our purpose, this will be a constant value like `coordinator_lock`.
*   `owner_id`: The unique identifier of the worker node that currently holds the lock (i.e., the coordinator).
*   `expires_at`: A timestamp indicating when the lock will expire if not renewed.

### 3.2. Leader Election Workflow

1.  A worker node starts up and attempts to become the coordinator.
2.  It tries to insert a row into the `locks` table with `lock_key = 'coordinator_lock'`, its own unique `owner_id`, and an `expires_at` timestamp set to `NOW() + lock_timeout` (e.g., 10 seconds).
3.  If the `INSERT` is successful, the node becomes the coordinator.
4.  If the `INSERT` fails due to a primary key violation, it means another node is already the coordinator.

### 3.3. Maintaining Leadership (Heartbeating)

The active coordinator is responsible for renewing its lock before it expires.

1.  The coordinator will periodically (e.g., every 5 seconds, which is half of the `lock_timeout`) execute an `UPDATE` statement to extend the `expires_at` timestamp.

    ```sql
    UPDATE locks
    SET expires_at = NOW() + 10
    WHERE lock_key = 'coordinator_lock' AND owner_id = <current_coordinator_id>;
    ```

2.  If this update affects one row, the coordinator continues to be the leader. If it affects zero rows (which could happen in a race condition where another node took over), it must step down.

### 3.4. Failover Mechanism

Non-coordinator nodes will periodically check for a stale lock.

1.  Every 10 seconds, a non-coordinator node will query the `locks` table.
2.  If no `coordinator_lock` row exists, or if the `expires_at` timestamp is in the past, the node will attempt to acquire the lock.
3.  To prevent race conditions, a node can use an `INSERT ... ON DUPLICATE KEY UPDATE` statement to try and claim the lock:

    ```sql
    INSERT INTO locks (lock_key, owner_id, expires_at)
    VALUES ('coordinator_lock', <new_worker_id>, NOW() + 10)
    ON DUPLICATE KEY UPDATE
        owner_id = IF(expires_at < NOW(), VALUES(owner_id), owner_id),
        expires_at = IF(expires_at < NOW(), VALUES(expires_at), expires_at);
    ```
4.  After executing this query, the node must read the `locks` table again to confirm that its `owner_id` is now the current owner of the lock. If it is, it becomes the new coordinator.
