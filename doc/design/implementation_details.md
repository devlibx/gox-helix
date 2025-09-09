# Implementation Details

This document provides details about the implementation of the `gox-helix` framework.

## Database Schema and Code Generation

We use `sqlc` to generate type-safe Go code from our SQL schema and queries. `sqlc` is a command-line tool that automates the creation of Go code for database interactions.

### Why `sqlc`?

*   **Type Safety:** `sqlc` generates type-safe Go code, which helps prevent SQL injection vulnerabilities and common database-related bugs.
*   **Developer Productivity:** By automating boilerplate database code generation, `sqlc` allows developers to focus on core business logic.
*   **Maintainability:** `sqlc` ensures that the Go code remains synchronized with the database schema, simplifying maintenance.

### Workflow

The workflow for using `sqlc` is as follows:

1.  **Define the database schema:** The database schema is defined in SQL files (e.g., `pkg/common/lock/mysql/database/schema.sql`).
2.  **Write SQL queries:** SQL queries for all database operations are written in separate SQL files (e.g., `pkg/common/lock/mysql/database/queries.sql`).
3.  **Run `sqlc`:** `sqlc` is executed to generate Go code from the schema and queries.
4.  **Use the generated code:** The generated Go code is then used in the application to interact with the database.

### `sqlc.yaml` Configuration

We use `sqlc.yaml` to configure `sqlc`. This file specifies the location of our schema and query files, along with other generation options. For the distributed locking mechanism, the configuration is as follows:

```yaml
version: "2"
sql:
  - engine: "mysql"
    queries: "pkg/common/lock/mysql/database/queries.sql"
    schema: "pkg/common/lock/mysql/database/schema.sql"
    gen:
      go:
        emit_result_struct_pointers: true
        emit_sql_as_comment: true
        emit_prepared_queries: true
        emit_interface: true
        emit_exact_table_names: false
        emit_empty_slices: true
        emit_json_tags: true
        package: "helixMysql"
        out: "pkg/common/lock/mysql/database"
```

### Schema Details

`gox-helix` utilizes several key tables for its distributed coordination:

*   **`helix_locks`**: Central to the distributed locking mechanism, this table includes:
    *   `status`: A `TINYINT` representing the lock's state (e.g., `1` for active, `0` for inactive, `2` for deletable). Corresponding Go constants are defined in `pkg/common/lock/mysql/database/constants.go` for type-safe usage.
    *   `epoch`: A `bigint` field used for optimistic locking. It acts as a version token, incrementing with each successful lock acquisition or renewal by a new owner, which helps prevent race conditions and ensures consistency in a distributed environment.

*   **`helix_nodes`**: Stores information about active nodes in the cluster, including their registration, heartbeating, and status tracking.

*   **`helix_allocation`**: Records partition allocation information, detailing which partitions are assigned to which nodes, along with their current status and metadata.

## Cluster Coordinator Implementation

The cluster coordinator functionality is implemented within the `pkg/cluster/recipe/coordinator` package. Key aspects include:

*   **Coordinator Interface (`api.go`):** Defines the contract for coordinator operations, such as starting the coordination loop.
*   **Coordinator Implementation (`coordinator.go`):** Provides the concrete implementation of the coordinator, leveraging various `gox-helix` components like `ClusterManager`, `AllocationManager`, and `Locker`.
*   **Periodic Allocation (`start_operation.go`):** The coordinator's `Start` method initiates a periodic loop that performs partition allocation. This involves:
    *   Acquiring a cluster-wide lock to ensure single-coordinator operation.
    *   Discovering all active domains and task lists from the `helix_cluster` table.
    *   Invoking the `AllocationManager` to calculate optimal partition assignments for each task list.
    *   Updating the database with the new assignments.
*   **Error Handling:** The coordinator includes robust error handling, specifically filtering out expected leadership errors (e.g., when another node becomes the leader) to prevent log spam and ensure smooth transitions.

This modular design allows for clear separation of concerns and facilitates future enhancements to the coordinator's responsibilities.
