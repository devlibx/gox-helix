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

We will use a distributed lock implemented in MySQL to manage coordinator election. A dedicated `helix_locks` table will be used for this purpose.

The coordinator functionality is encapsulated within the `pkg/cluster/recipe/coordinator` package, providing a clear separation of concerns. The `Coordinator` interface defines the contract for coordinator operations, and `coordinatorImpl` provides the concrete implementation.

### 3.1. Database Schema for Locking

We will use the `helix_locks` table with the following schema:

```sql
CREATE TABLE helix_locks (
    id           bigint unsigned NOT NULL AUTO_INCREMENT,
    lock_key     VARCHAR(255)    NOT NULL,
    owner_id     VARCHAR(255)    NOT NULL,
    expires_at   TIMESTAMP       NOT NULL,
    epoch        bigint          NOT NULL DEFAULT 0,
    status       TINYINT         NOT NULL DEFAULT 1, -- 1: active, 0: inactive, 2: deletable
    created_at   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `lock_key_status_unique_key` (`lock_key`, `status`),
    KEY `lock_key_ids` (`lock_key`)
);
```

*   `lock_key`: A unique name for the lock (e.g., `coordinator_lock`).
*   `owner_id`: The unique identifier of the worker node that currently holds the lock.
*   `expires_at`: A timestamp indicating when the lock will expire if not renewed.
*   `epoch`: A version token for optimistic locking, incremented on successful acquisition or renewal by a new owner. This helps prevent split-brain scenarios.
*   `status`: A `TINYINT` representing the lock's state (1 for active, 0 for inactive, 2 for deletable).

### 3.2. Leader Election Workflow

1.  A worker node starts up and attempts to become the coordinator by calling the `BecomeClusterCoordinator` method on the `ClusterManager` interface.
2.  This method attempts to acquire the lock using an atomic `INSERT ... ON DUPLICATE KEY UPDATE` operation. This operation tries to insert a new lock record or update an existing one if it's expired or owned by the same node.

    ```sql
    INSERT INTO helix_locks (lock_key, owner_id, expires_at, epoch, status)
    VALUES ('coordinator_lock', <new_worker_id>, NOW() + <lock_timeout_seconds>, 1, 1)
    ON DUPLICATE KEY
        UPDATE owner_id   = IF(owner_id = VALUES(owner_id) OR expires_at < VALUES(expires_at), VALUES(owner_id), owner_id),
               expires_at = IF(owner_id = VALUES(owner_id) OR expires_at < VALUES(expires_at), VALUES(expires_at), expires_at),
               epoch      = IF(owner_id = VALUES(owner_id) OR expires_at < VALUES(expires_at), epoch + 1, epoch);
    ```
    *   The `epoch` is initialized to 1 for a new lock and incremented on successful acquisition/renewal.
    *   The `status` is set to 1 (active).

3.  After executing this query, the node reads the `helix_locks` table to confirm if its `owner_id` is now the current owner and if the `epoch` has been incremented (if it was an update). If confirmed, it becomes the coordinator.

### 3.3. Maintaining Leadership (Heartbeating)

The active coordinator is responsible for renewing its lock before it expires. This is done by re-executing the `TryUpsertLock` operation with its own `owner_id` and an updated `expires_at`. The `TTL` for the coordinator lock is configurable via the `ControllerTtl` field in `ClusterManagerConfig`.

1.  The coordinator will periodically (e.g., every 5 seconds, which is half of the `ControllerTtl`) execute the `TryUpsertLock` query.

    ```sql
    INSERT INTO helix_locks (lock_key, owner_id, expires_at, epoch, status)
    VALUES ('coordinator_lock', <current_coordinator_id>, NOW() + <lock_timeout_seconds>, 1, 1)
    ON DUPLICATE KEY
        UPDATE owner_id   = IF(owner_id = VALUES(owner_id) OR expires_at < VALUES(expires_at), VALUES(owner_id), owner_id),
               expires_at = IF(owner_id = VALUES(owner_id) OR expires_at < VALUES(expires_at), VALUES(expires_at), expires_at),
               epoch      = IF(owner_id = VALUES(owner_id) OR expires_at < VALUES(expires_at), epoch + 1, epoch);
    ```
2.  If the operation succeeds and the `owner_id` remains its own, and the `epoch` is incremented (for renewals), the coordinator continues to be the leader. If the `owner_id` changes or the `epoch` is not incremented as expected (indicating another node took over), it must step down.

### 3.4. Failover Mechanism

Non-coordinator nodes will periodically check for a stale lock.

1.  Every 10 seconds, a non-coordinator node will query the `helix_locks` table to check the `expires_at` and `status` of the `coordinator_lock`.
2.  If the lock is expired (`expires_at` is in the past) or if no active `coordinator_lock` row exists, the node will attempt to acquire the lock using the `TryUpsertLock` operation described in Section 3.2.
3.  The atomic nature of `TryUpsertLock` ensures that only one node can successfully acquire the lock at a time, even in a race condition. The `epoch` field further guarantees that only the latest successful acquisition is considered valid.
4.  After executing the `TryUpsertLock` query, the node must read the `helix_locks` table again to confirm that its `owner_id` is now the current owner and the `epoch` reflects its successful acquisition. If confirmed, it becomes the new coordinator.

### 3.5. Partition Allocation Orchestration

The elected coordinator is responsible for orchestrating partition allocation across the cluster. This involves:

1.  **Discovering Task Lists:** The coordinator periodically queries the database to discover all active domains and task lists within its cluster.
2.  **Calculating Allocation:** For each discovered task list, the coordinator invokes the `AllocationManager` (specifically, the `CalculateAllocation` method) to determine the optimal partition assignments based on the current cluster state and active nodes.
3.  **Updating Assignments:** The coordinator then updates the database with the new partition assignments. This ensures that all workers have an up-to-date view of which partitions they are responsible for.

This orchestration ensures that partitions are continuously balanced and reassigned as nodes join, leave, or fail, maintaining the desired distribution across the cluster.
