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

The high-level design is to create a system that can manage and distribute tasks (partitions) among a set of workers. The system will be composed of the following key concepts:

*   **Domain:** A logical grouping of related tasks and workers.
*   **Queue (Task List):** A queue of tasks within a domain.
*   **Partition:** A unit of work that can be assigned to a worker.

### Worker Management and Task Allocation

*   **Dynamic Worker Registration:** Workers are stateless and can come up and go down at any time. They register themselves with a unique ID (UUID) to participate in the work.
*   **Partition Allocation:** The framework is responsible for allocating partitions to the available workers. This allocation is dynamic and is re-evaluated periodically (e.g., every 10 seconds).
*   **Rebalancing:** When new workers join or existing workers leave, the partitions are rebalanced among the active workers to ensure an even distribution of work.
*   **Heartbeating:** Workers send heartbeats to the system to indicate that they are alive and healthy. This allows the system to detect failed workers and re-assign their partitions.
*   **Coordinator:** A single node acts as a coordinator to manage the allocation of partitions to workers. This coordinator acquires a lock to ensure that it is the only one performing this task at any given time.

For example, we can have a domain `domain_order` with a task list `food_order` and 50 partitions. Workers can register themselves to get work for this domain and task list. The framework will then distribute the 50 partitions among the active workers.

All design documents will be maintained in the `doc/design` directory.

## Roadmap

This project is in the early stages of development. Here is a high-level roadmap of what we are planning to do:

*   [ ] **Phase 1: Core Framework**
    *   [ ] Implement the core data model in MySQL.
    *   [ ] Implement the worker registration and heartbeating mechanism.
    *   [ ] Implement the coordinator election and partition allocation logic.
*   [ ] **Phase 2: API and SDK**
    *   [ ] Design and implement a clean and easy-to-use API for defining domains, queues, and partitions.
    *   [ ] Provide a Go SDK for building workers.
*   [ ] **Phase 3: Advanced Features**
    *   [ ] Implement more advanced partition allocation strategies (e.g., sticky partitions).
    *   [ ] Add support for task prioritization.
    *   [ ] Add metrics and monitoring capabilities.

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
