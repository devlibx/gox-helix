# Partition Rebalancing Algorithm

This document describes the partition rebalancing algorithm used in `gox-helix`. The algorithm is implemented in `pkg/cluster/mgmt/allocation/rebalance_algo_v1.go` and is used by `pkg/cluster/mgmt/allocation/default_algorithm.go`.

## Goal

The goal of the rebalancing algorithm is to distribute partitions evenly among the active nodes in a cluster. This ensures that the workload is balanced and that the system is resilient to node failures.

## Algorithm

The rebalancing algorithm works in a two-phase process, especially beneficial for graceful rebalancing when new nodes are added to an imbalanced cluster:

1.  **Two-Phase Migration (Placeholder Creation):**
    *   Identifies significantly under-allocated nodes (e.g., newly added nodes) and over-allocated nodes.
    *   For excess partitions on over-allocated nodes, it marks them for `RequestedRelease` and simultaneously creates `Placeholder` partitions on the target under-allocated nodes. This allows for a graceful transfer without immediate disruption.
    *   The `Placeholder` partitions count towards the target load of the new nodes, guiding the rebalancing process.

2.  **Assign Unassigned Partitions:**
    *   The algorithm assigns any truly `Unassigned` partitions (including those that were `RequestedRelease` or `PendingRelease` and timed out) to the active nodes with the fewest number of assigned partitions (considering `Placeholder` partitions as assigned for load calculation).

3.  **Rebalance Over-allocated Nodes (Direct Reassignment):**
    *   After the two-phase migration and initial unassigned partition assignment, the algorithm checks for any remaining over-allocated nodes.
    *   It directly reassigns excess `Assigned` partitions from these nodes to under-allocated nodes.

### Detailed Steps

*   **Identify Active Nodes:** The algorithm first identifies all currently active nodes in the cluster.
*   **Categorize Partitions:** Existing partitions are categorized based on their current status (`Assigned`, `RequestedRelease`, `PendingRelease`, `Unassigned`, `Placeholder`).
*   **Calculate Target Distribution:** The target number of partitions per active node is calculated as `total_partitions / total_active_nodes`. Any remainder is distributed one by one to ensure an even spread.
*   **Graceful Migration (Two-Phase):** If new nodes are introduced or significant imbalance exists, partitions are first marked for release on over-allocated nodes, and corresponding placeholders are created on under-allocated nodes. This allows for a controlled, non-disruptive transfer.
*   **Assign Unassigned:** All `Unassigned` partitions (including those from timed-out releases or inactive nodes) are assigned to the active nodes with the lowest current load (considering placeholders).
*   **Direct Rebalancing:** Any remaining over-allocated nodes (after the two-phase migration and unassigned assignment) will have their excess partitions directly reassigned to under-allocated nodes.

## Partition States

The algorithm considers the following partition states:

*   `Assigned`: The partition is actively assigned to a node.
*   `Unassigned`: The partition is not assigned to any node.
*   `RequestedRelease`: The partition is in the process of being released from a node, initiated by the rebalancer.
*   `PendingRelease`: The partition is pending release from a node, typically after a `RequestedRelease` and awaiting confirmation.
*   `Placeholder`: A temporary state indicating that a partition is intended to be assigned to a new node as part of a graceful two-phase migration. These count towards a node's load for rebalancing calculations but are not yet physically assigned.

Partitions in `RequestedRelease` and `PendingRelease` states are generally not directly reassigned by the rebalancer unless their release times out, at which point they become `Unassigned`. `Placeholder` partitions are used internally for load balancing calculations during the two-phase migration.

## Stickiness

The algorithm preserves stickiness. This means that if a partition is already `Assigned` to a node, it will not be moved to another node unless the node becomes over-allocated or inactive. This minimizes partition movements and reduces rebalancing overhead.

## Scenarios

The rebalancing algorithm is tested with a variety of scenarios to ensure that it is robust and that it can handle different situations. The test cases are located in `pkg/cluster/mgmt/allocation/rebalance_algo_v1_test.go` and `pkg/cluster/mgmt/allocation/default_algorithm_rebalance_e2e_test.go`.

The following are some of the scenarios that are tested:

*   **Basic even distribution:** Partitions are distributed evenly among active nodes.
*   **Uneven distribution with remainder:** Handles scenarios where partitions cannot be perfectly divided among nodes.
*   **Rebalancing over-allocated nodes:** Partitions are moved from nodes with excess load to those with less.
*   **Partitions in release states:** Verifies that partitions in `RequestedRelease` or `PendingRelease` states are handled correctly and not immediately reassigned.
*   **Inactive node partitions:** Partitions previously assigned to inactive nodes are detected and reassigned to active nodes.
*   **No active nodes:** Ensures the algorithm behaves correctly when no active nodes are available.
*   **Single node:** Verifies that all partitions are assigned to a single node if it's the only active one.
*   **Complex mixed scenario:** Tests the algorithm's behavior with a combination of different partition states and node loads.
*   **Zero partitions:** Confirms correct handling when there are no partitions to distribute.
*   **More nodes than partitions:** Ensures partitions are distributed, and some nodes may remain without partitions.
*   **Two-phase graceful migration:** Tests the new two-phase rebalancing mechanism, including placeholder creation and timed release of partitions, for adding new nodes to an imbalanced cluster.
*   **Stickiness preservation when node goes down:** Verifies that partitions assigned to a node that becomes inactive are correctly redistributed to other active nodes while preserving stickiness for partitions on remaining active nodes.
