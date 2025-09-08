# gox-helix

[![Go Report Card](https://goreportcard.com/badge/github.com/devlibx/gox-helix)](https://goreportcard.com/report/github.com/devlibx/gox-helix)
[![Go](https://github.com/devlibx/gox-helix/actions/workflows/go.yml/badge.svg)](https://github.com/devlibx/gox-helix/actions/workflows/go.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`gox-helix` is an open-source framework for building distributed, scalable, and reliable applications in Go. It is inspired by Apache Helix and Cadence, and it uses MySQL as a backend for coordination and state management, providing a robust alternative to Zookeeper.

## Features

*   **Distributed Resource Management:** Manage and coordinate distributed resources in a cluster.
*   **Automatic Rebalancing:** Automatically rebalance resources when nodes join or leave the cluster.
*   **Task Scheduling:** Schedule and execute tasks on the cluster nodes.
*   **Fault Tolerance:** Detect node failures and automatically re-assign tasks to healthy nodes.
*   **Scalability:** Scale your application by adding more nodes to the cluster.
*   **MySQL Backend:** Use MySQL for coordination and state management, which is a familiar and widely used database.

## Design Overview

The high-level design of `gox-helix` involves a central coordinator and a set of workers, all leveraging MySQL for robust coordination and state management. The system is designed to manage and distribute tasks (partitions) among workers, ensuring scalability, reliability, and fault tolerance.

Key Concepts:

*   **Domain:** A logical grouping for related tasks and workers.
*   **Queue (Task List):** A queue of tasks within a domain.
*   **Partition:** The smallest unit of work assignable to a worker.
*   **Worker:** A stateless process executing tasks, capable of dynamically joining or leaving the cluster.
*   **Coordinator:** A single, elected node responsible for cluster coordination, including partition allocation, leader election, and failure detection.

The coordinator election and distributed locking mechanism utilize a dedicated `helix_locks` table in MySQL. This table employs a `TINYINT` type for the `status` field (e.g., `1` for active, `0` for inactive, `2` for deletable) and incorporates an `epoch` field for optimistic locking, preventing split-brain scenarios and ensuring consistent state.

All detailed design documents are maintained in the `doc/design` directory.

## Current Work Status

We are actively developing the core framework, focusing on robust distributed coordination and cluster management. Key achievements include:

1.  **Distributed Locking Mechanism:**
    *   **New Package Structure:** The lock implementation is located under `pkg/common/lock`, with MySQL-specific details in `pkg/common/lock/mysql/database`.
    *   **Schema Updates:** The `locks` table has been renamed to `helix_locks`, and a `status` column (TINYINT: `1` for active, `0` for inactive, `2` for deletable) and an `epoch` column for optimistic locking have been added.
    *   **SQLC Configuration:** `sqlc.yaml` and `queries.sql` have been updated for the new lock package.
    *   **Interface Definition:** A `Locker` interface has been defined in `pkg/common/lock/lock.go`.
    *   **Acquire Method Implementation:** The `Acquire` method for the MySQL locker has been implemented, leveraging a refined `TryUpsertLock` SQL query for robust and atomic lock acquisition.
    *   **Comprehensive Test Suite:** An extensive suite of unit and integration tests has been developed for the lock package.

2.  **Cluster Node Management:**
    *   **New Module:** Introduced `pkg/cluster` for managing cluster nodes, with MySQL database interactions in `pkg/cluster/mysql/database`.
    *   **`helix_nodes` Table:** A new table to store cluster node information, including registration, heartbeating, and status tracking.
    *   **Cluster Metadata Management:** Implemented the `helix_cluster` table and associated database operations to manage metadata about different clusters, domains, and task lists.
    *   **Core Cluster Management Logic:** Implemented node registration, automated heartbeating, inactive node detection, and retrieval of active nodes.
    *   **Distributed Controller Election:** Integrated the distributed locking mechanism to ensure only a single cluster manager instance is responsible for critical cluster-wide operations like marking inactive nodes.
    *   **Automatic Node Re-registration:** Implemented a mechanism to automatically re-register nodes if their heartbeat fails, ensuring high availability.

## Next Steps

To continue, we need to:

1.  **Complete Locker Interface:** Implement the `Release` and `Renew` methods of the `Locker` interface in `pkg/common/lock/mysql/service.go`.
2.  **Refine Coordinator Logic:** Implement the full coordinator election and failover logic using the new `Locker` and cluster management modules.
3.  **Worker Registration and Heartbeating:** Fully integrate and refine the worker registration and heartbeating mechanisms within the cluster management module.
4.  **Partition Allocation:** Develop the logic for dynamic partition allocation and rebalancing among active workers.
5.  **Task Scheduling:** Implement the core task scheduling and execution framework.

## Roadmap

This project is in active development. Here is a high-level roadmap of what we are planning to do:

*   **Phase 1: Core Framework (In Progress)**
    *   Implement the core distributed locking mechanism.
    *   Implement coordinator election and failover.
    *   Implement worker registration and heartbeating.
    *   Implement basic partition allocation logic.
*   **Phase 2: API and SDK**
    *   Design and implement a clean and easy-to-use API for defining domains, queues, and partitions.
    *   Provide a Go SDK for building workers.
*   **Phase 3: Advanced Features**
    *   Implement more advanced partition allocation strategies (e.g., sticky partitions, task prioritization).
    *   Add comprehensive metrics and monitoring capabilities.
    *   Explore alternative backend stores (e.g., PostgreSQL, etcd).


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

You need to have Go installed on your machine.

```sh
go version
```

### Installation

To get the latest version of the framework, you can use `go get`:

```sh
go get github.com/devlibx/gox-helix
```

## Usage

To run the application:

```sh
go run main.go
```

## Contributing

We welcome contributions from the community. Please read our [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Community

*   **Slack:** Join our Slack channel to chat with other users and developers.
*   **GitHub Issues:** If you have a question or a bug report, please open an issue on GitHub.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
