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

The `helix_locks` table, central to the distributed locking mechanism, includes key fields:

*   `status`: This field, now a `TINYINT`, represents the lock's state (e.g., `1` for active, `0` for inactive, `2` for deletable). Corresponding Go constants are defined in `pkg/common/lock/mysql/database/constants.go` for type-safe usage.
*   `epoch`: A `bigint` field used for optimistic locking. It acts as a version token, incrementing with each successful lock acquisition or renewal by a new owner, which helps prevent race conditions and ensures consistency in a distributed environment.
