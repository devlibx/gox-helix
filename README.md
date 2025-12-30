# Helix Integration Example

This example demonstrates how to integrate and use the `gox-helix` library in a Go application using the `go-fx` dependency injection framework. It sets up a complete worker that can register itself, acquire partitions, and process work.

## Prerequisites

Before running this example, you need a running MySQL database. The application expects to find the database connection details from the environment. The default connection URL is `root:password@tcp(127.0.0.1:3306)/gox_helix`. You can set up a local MySQL instance using Docker:

```bash
docker run --name gox-helix-mysql -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=gox_helix -p 3306:3306 -d mysql:8.0
```

## Understanding the Code

The application uses `go-fx` to manage dependencies and application lifecycle. Here are the key components:

### 1. Supplying Configuration and Singletons

At the beginning of the `fx.New` call, we supply the necessary configuration and application-level singletons:

```go
// Create a new application singleton. It manages the application's context and lifecycle.
// Note: The name has a typo in the original notes; it is `ApplicationSingleton`.
appSignal := pkgCommon.NewApplicationSingletonWithContext(ctx)

// ...

app := fx.New(
    // Supply the application config and the domain definitions
    fx.Supply(&appConfig),
    fx.Supply(&appConfig.Domains),

    // Supply the application singleton
    fx.Supply(appSignal),

    // ...
)
```
- `fx.Supply(&appConfig.Domains)`: Provides the domain configuration that defines the tasklists and their properties.
- `fx.Supply(appSignal)`: Provides the `ApplicationSingleton` instance, which is used throughout the application to manage state and lifecycle.


### 2. Providing Helix Dependencies

The core of the `gox-helix` framework is provided through a single module:

```go
goxHelixApi.Provider,
```
This provider sets up all the necessary components for the helix framework, including services for coordination, locking, domain management, and worker registration.

### 3. Invoking the Executor Lifecycle

To start the worker and make it run, you need to invoke the `executor.NewExecutorLifecycle`:

```go
fx.Invoke(executor.NewExecutorLifecycle),
```
This function sets up and starts the executor service, which is responsible for managing the worker's lifecycle, including starting and stopping all the necessary processes.

### 4. Implementing the Client Work Function

The actual work is done in the `ClientFunctionProcessWork`. This is where you define what your worker should do when it receives a task.

```go
fx.Provide(func() coordinator.ClientFunctionProcessWork {
    return func(ctx context.Context, work coordinator.Work) {
        if atomic.AddInt64(&count, 1)%10 == 0 {
            slog.Info("Got work to do", "work", work)
        }
        time.Sleep(10 * time.Second)
        work.CompletedChannel <- coordinator.WorkResponse{}
        close(work.CompletedChannel)
    }
}),
```
- This function is provided to the `fx` container.
- It receives a `coordinator.Work` object that contains information about the task.
- The actual work is executed within a goroutine, one for each partition assigned to the worker.
- **Important:** You *must* send a `coordinator.WorkResponse{}` to `work.CompletedChannel` when your processing for a given `work` item is complete. Failure to do so will block the process from picking up the next task.
- It is possible for the client to do any necessary work in this callback. To ensure efficient partition rebalancing and prevent two partitions from working on the same partition concurrently, try to keep the processing time within this function short (ideally < 100ms). For example, if you poll work from a database, you can run a busy loop to read from the database for 100-200ms and then acknowledge the channel. This strategy allows the partition to be safely reassigned if the worker goes down.

## Example Configuration

The example uses the following `config.yaml` to define domains and task lists:

```yaml
domains:
  mobility:
    worker_count_to_process_domain: 4
    task_list:
      booking:
        partition_count: 4
      allocation:
        partition_count: 4
      refund:
        partition_count: 8
  food:
    worker_count_to_process_domain: 2
    task_list:
      driver_allocation:
        partition_count: 3
      driver_pickup:
        partition_count: 6
      delivered:
        partition_count: 9
```

## Worker-Partition Mapping Example

The `gox-helix` framework dynamically allocates partitions to available workers. If there are multiple workers running, the partitions will be split among them to distribute the workload. The following example shows how partitions (represented by the `[...,0]` array) are mapped to a worker. In this specific example, there is only one worker, so the `worker_id` (`177e9c39-8e69-482a-a677-9e41e91d67a1`) is the same for all entries.

```
'1', 'food', 'driver_allocation', '177e9c39-8e69-482a-a677-9e41e91d67a1', '1', '[1,2,0]'
'2', 'mobility', 'allocation', '177e9c39-8e69-482a-a677-9e41e91d67a1', '1', '[1,2,3,0]'
'4', 'food', 'delivered', '177e9c39-8e69-482a-a677-9e41e91d67a1', '1', '[3,7,8,1,4,6,5,2,0]'
'8', 'mobility', 'booking', '177e9c39-8e69-482a-a677-9e41e91d67a1', '1', '[1,2,3,0]'
'9', 'food', 'driver_pickup', '177e9c39-8e69-482a-a677-9e41e91d67a1', '1', '[2,3,4,5,0,1]'
'10', 'mobility', 'refund', '177e9c39-8e69-482a-a677-9e41e91d67a1', '1', '[6,7,0,1,2,3,4,5]'
```

## How to Run

You can run the example directly from your terminal:

```bash
go run examples/integration/main.go
```

The application will start, register the worker, and begin processing work for the defined domains and tasklists. You will see log messages indicating the work being done. The application also truncates the database tables on startup for a clean run.

## Database Schema

Before running the application, you need to set up the database. Create a database named `automation` and then run the following SQL statements to create the necessary tables.

### `helix_locks`

```sql
CREATE TABLE helix_locks
(
    id           bigint unsigned NOT NULL AUTO_INCREMENT,
    domain       VARCHAR(64)     NOT NULL,
    lock_key     VARCHAR(255)    NOT NULL,
    owner_id     VARCHAR(64)     NOT NULL,
    expires_at   TIMESTAMP       NOT NULL,
    epoch        bigint unsigned NOT NULL DEFAULT 0,
    status       TINYINT         NOT NULL DEFAULT 1,
    `created_at` datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `lock_key_status_unique_key` (`domain`, `lock_key`, `status`),
    KEY `lock_key_ids` (`lock_key`)
);
```

### `helix_worker_partition_mapping`

```sql
CREATE TABLE helix_worker_partition_mapping
(
    id         bigint unsigned NOT NULL AUTO_INCREMENT,
    domain     VARCHAR(64)     NOT NULL,
    tasklist   VARCHAR(64)     NOT NULL,
    owner_id   VARCHAR(64)     NOT NULL,
    status     tinyint         NOT NULL default 1,
    metadata   TEXT            NULL,
    created_at datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `domain_tasklist_key` (`domain`, `tasklist`, `owner_id`)
);
```

### `helix_domain`

```sql
CREATE TABLE helix_domain
(
    id              bigint unsigned NOT NULL AUTO_INCREMENT,
    domain          VARCHAR(64)     NOT NULL,
    tasklist        VARCHAR(64)     NOT NULL,
    metadata        TEXT            NULL,
    partition_count INT UNSIGNED    NOT NULL DEFAULT 1,
    status          TINYINT         NOT NULL DEFAULT 1,
    created_at      datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      datetime        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`, `status`),
    UNIQUE KEY `domain_tasklist_status_unique` (`domain`, `tasklist`, `status`)
) PARTITION BY LIST (`status`) (
    PARTITION p_active VALUES IN (1),
    PARTITION p_inactive VALUES IN (0),
    PARTITION p_deletable VALUES IN (2)
    );
```

### `helix_workers`

```sql
CREATE TABLE helix_workers (
    id BIGINT AUTO_INCREMENT,
    worker_id VARCHAR(64) NOT NULL,
    domain VARCHAR(64) NOT NULL,
    status TINYINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL,
    last_heartbeat_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at),
    UNIQUE KEY uidx_domain_worker_id (domain, worker_id, created_at)
)
PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (
    PARTITION p2025_11 VALUES LESS THAN (UNIX_TIMESTAMP('2025-12-01 00:00:00')),
    PARTITION p2025_12 VALUES LESS THAN (UNIX_TIMESTAMP('2026-01-01 00:00:00')),
    PARTITION p2026_01 VALUES LESS THAN (UNIX_TIMESTAMP('2026-02-01 00:00:00')),
    PARTITION p2026_02 VALUES LESS THAN (UNIX_TIMESTAMP('2026-03-01 00:00:00')),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);
```