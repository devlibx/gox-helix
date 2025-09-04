# High-Level Architecture

This document outlines the high-level architecture of the `gox-helix` framework.

## 1. Overview

`gox-helix` is a distributed resource management and task scheduling framework inspired by Apache Helix and Cadence. It uses MySQL as a backend for coordination and state management.

The primary goals of the framework are:

*   **Scalability:** The framework should be able to scale to manage a large number of nodes and tasks.
*   **Reliability:** The framework should be resilient to node failures and network partitions.
*   **Extensibility:** The framework should be extensible to support different types of applications and use cases.

## 2. Key Concepts

The framework is based on the following key concepts:

*   **Domain:** A logical namespace for a set of related resources and tasks.
*   **Queue (Task List):** A queue of tasks within a domain.
*   **Partition:** A unit of work that can be assigned to a worker. A partition is the smallest unit of parallelism.
*   **Worker:** A process that executes tasks. Workers are stateless and can join and leave the cluster at any time.
*   **Coordinator:** A special node that is responsible for coordinating the cluster. The coordinator is responsible for tasks such as partition allocation, leader election, and failure detection.

## 3. Architecture

The architecture of `gox-helix` is based on a central coordinator and a set of workers.

### 3.1. MySQL Backend

The framework uses MySQL as a backend to store the state of the cluster. This includes information about:

*   Domains, queues, and partitions
*   Registered workers
*   Partition assignments
*   Locks for leader election

Using MySQL as a backend provides a familiar and reliable way to store the state of the cluster.

### 3.2. Coordinator

The coordinator is a single node that is responsible for managing the cluster. The coordinator is elected using a distributed lock in MySQL.

The responsibilities of the coordinator include:

*   **Worker Registration:** When a new worker starts, it registers itself with the coordinator.
*   **Heartbeating:** Workers send periodic heartbeats to the coordinator to indicate that they are alive.
*   **Failure Detection:** The coordinator detects worker failures by monitoring heartbeats. If a worker misses a certain number of heartbeats, it is considered to be dead.
*   **Partition Allocation:** The coordinator is responsible for allocating partitions to workers. The allocation is dynamic and is re-evaluated periodically.
*   **Rebalancing:** When a worker joins or leaves the cluster, the coordinator rebalances the partitions among the available workers.

### 3.3. Worker

Workers are responsible for executing tasks. Each worker is assigned a set of partitions by the coordinator.

The responsibilities of a worker include:

*   **Registration:** Registering with the coordinator when it starts.
*   **Heartbeating:** Sending periodic heartbeats to the coordinator.
*   **Task Execution:** Executing the tasks for the partitions that are assigned to it.

## 4. Workflow

The following is a high-level overview of the workflow:

1.  An application defines a domain, a set of queues, and a set of partitions.
2.  Workers are started. Each worker registers itself with the coordinator.
3.  The coordinator allocates the partitions to the available workers.
4.  Workers execute the tasks for the partitions that are assigned to them.
5.  If a worker fails, the coordinator detects the failure and re-assigns its partitions to other workers.
6.  If a new worker joins the cluster, the coordinator rebalances the partitions to include the new worker.

## 5. Future Work

*   Implement more advanced partition allocation strategies.
*   Add support for task prioritization.
*   Add metrics and monitoring capabilities.
