-- name: UpsertNode :exec
INSERT INTO helix_nodes (cluster_name, node_uuid, node_metadata, last_hb_time, status)
VALUES (?, ?, ?, ?, 1)
ON DUPLICATE KEY UPDATE node_metadata = VALUES(node_metadata),
                        last_hb_time  = VALUES(last_hb_time),
                        status        = 1;


-- name: UpdateHeartbeat :execresult
UPDATE helix_nodes
SET last_hb_time = ?,
    version      = version + 1
WHERE cluster_name = ?
  AND node_uuid = ?
  AND status = 1;

-- name: DeregisterNode :exec
UPDATE helix_nodes
SET status  = 0,
    version = version + 1
WHERE cluster_name = ?
  AND node_uuid = ?
  AND status = 1;

-- name: GetActiveNodes :many
SELECT cluster_name,
       node_uuid,
       node_metadata,
       last_hb_time,
       status,
       version,
       created_at,
       updated_at
FROM helix_nodes
WHERE cluster_name = ?
  AND status = 1;

-- name: MarkInactiveNodes :exec
UPDATE helix_nodes
SET status  = 0,
    version = version + 1
WHERE cluster_name = ?
  AND status = 1
  AND last_hb_time < ?;

-- name: GetNodeById :one
SELECT *
FROM helix_nodes
WHERE cluster_name = ?
  AND node_uuid = ?
  AND status = 1;

-- name: UpsertCluster :exec
INSERT INTO helix_cluster (cluster, domain, tasklist, partition_count, metadata, status)
VALUES (?, ?, ?, ?, ?, 1)
ON DUPLICATE KEY UPDATE partition_count = VALUES(partition_count),
                        metadata        = VALUES(metadata),
                        status          = 1;

-- name: GetAllDomainsAndTaskListsByClusterCname :many
SELECT *
FROM helix_cluster
WHERE cluster = ?
  AND status = 1;

-- name: GetClustersByDomain :many
SELECT cluster,
       domain,
       tasklist,
       metadata,
       partition_count,
       status,
       created_at,
       updated_at
FROM helix_cluster
WHERE cluster = ?
  AND domain = ?
  AND status = 1;

-- name: GetCluster :one
SELECT cluster,
       domain,
       tasklist,
       metadata,
       partition_count,
       status,
       created_at,
       updated_at
FROM helix_cluster
WHERE cluster = ?
  AND domain = ?
  AND tasklist = ?
  AND status = 1;

-- name: UpsertAllocation :exec
INSERT INTO helix_allocation (cluster, domain, tasklist, node_id, partition_info, metadata, status)
VALUES (?, ?, ?, ?, ?, ?, 1)
ON DUPLICATE KEY UPDATE partition_info = VALUES(partition_info),
                        metadata       = VALUES(metadata),
                        status         = 1;

-- name: GetAllocationByNodeId :one
SELECT id,
       cluster,
       domain,
       tasklist,
       node_id,
       status,
       partition_info,
       metadata,
       created_at,
       updated_at
FROM helix_allocation
WHERE node_id = ?
  AND status = 1;

-- name: GetAllocationsForTasklist :many
SELECT id,
       cluster,
       domain,
       tasklist,
       node_id,
       status,
       partition_info,
       metadata,
       created_at,
       updated_at
FROM helix_allocation
WHERE cluster = ?
  AND domain = ?
  AND tasklist = ?
  AND status = 1;

-- name: MarkNodeInactive :exec
UPDATE helix_allocation
SET status = 0
WHERE node_id = ?
  AND status = 1;

-- name: MarkNodeDeletable :exec
UPDATE helix_allocation
SET status = 2
WHERE node_id = ?
  AND status = 0;

-- name: GetAllocationById :one
SELECT id,
       cluster,
       domain,
       tasklist,
       node_id,
       status,
       partition_info,
       metadata,
       created_at,
       updated_at
FROM helix_allocation
WHERE id = ?
  AND status = ?;