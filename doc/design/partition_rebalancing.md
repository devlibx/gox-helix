# Partition Rebalancing Algorithm

This document describes the partition rebalancing algorithm used in `gox-helix`. The algorithm is implemented in the `distributeWork` function in `pkg/cluster/mgmt/allocation/default_algorithm.go`.

## Goal

The goal of the rebalancing algorithm is to distribute partitions evenly among the active nodes in a cluster. This ensures that the workload is balanced and that the system is resilient to node failures.

## Algorithm

The rebalancing algorithm works in two steps:

1.  **Assign unassigned partitions:** The algorithm first assigns any unassigned partitions to the active nodes with the fewest number of assigned partitions.
2.  **Rebalance over-allocated nodes:** The algorithm then rebalances the partitions from over-allocated nodes to under-allocated nodes.

### Step 1: Assign Unassigned Partitions

The algorithm iterates through all the unassigned partitions and assigns them to the active node with the fewest number of assigned partitions. This is done to ensure that the partitions are distributed as evenly as possible among the active nodes.

The target number of partitions per node is calculated as `total_partitions / total_active_nodes`. Any remainder is distributed one by one to the nodes.

### Step 2: Rebalance Over-allocated Nodes

After assigning all the unassigned partitions, the algorithm checks for any over-allocated nodes. An over-allocated node is a node that has more partitions than the target number of partitions per node.

If an over-allocated node is found, the algorithm moves the excess partitions to under-allocated nodes. An under-allocated node is a node that has fewer partitions than the target number of partitions per node.

This step ensures that the partitions are distributed as evenly as possible among the active nodes, even if the initial distribution of partitions was not even.

## Partition States

The algorithm considers the following partition states:

*   `Assigned`: The partition is assigned to an active node.
*   `Unassigned`: The partition is not assigned to any node.
*   `RequestedRelease`: The partition is in the process of being released from a node.
*   `PendingRelease`: The partition is pending release from a node.

Partitions in the `RequestedRelease` and `PendingRelease` states are not considered for rebalancing. This is to prevent race conditions and to ensure that the partitions are released gracefully.

## Stickiness

The algorithm preserves stickiness. This means that if a partition is already assigned to a node, it will not be moved to another node unless the node is over-allocated. This is to minimize the number of partition movements and to reduce the overhead of rebalancing.

## Scenarios

The rebalancing algorithm is tested with a variety of scenarios to ensure that it is robust and that it can handle different situations. The test cases are located in `pkg/cluster/mgmt/allocation/default_algorithm_rebalance_test.go`.

The following are some of the scenarios that are tested:

*   **Basic even distribution:** 15 partitions are distributed evenly among 5 nodes (3 partitions per node).
*   **Uneven distribution with remainder:** 16 partitions are distributed among 5 nodes (one node gets 4 partitions, and the other four nodes get 3 partitions).
*   **Rebalancing over-allocated nodes:** Partitions are moved from over-allocated nodes to under-allocated nodes.
*   **Partitions in release states:** Partitions in the `RequestedRelease` and `PendingRelease` states are not moved.
*   **Inactive node partitions:** Partitions assigned to inactive nodes are reassigned to active nodes.
*   **No active nodes:** No partitions are assigned if there are no active nodes.
*   **Single node:** All partitions are assigned to the single active node.
*   **Complex mixed scenario:** A complex scenario with a mix of different partition states and node states.
*   **Zero partitions:** No partitions are assigned if there are no partitions.
*   **More nodes than partitions:** Partitions are distributed among the nodes, and some nodes may not have any partitions.
