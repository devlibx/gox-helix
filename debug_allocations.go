package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"

	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixLock "github.com/devlibx/gox-helix/pkg/common/lock/mysql"

	_ "github.com/go-sql-driver/mysql"
)

// AllocationInfo represents the JSON structure in partition_info
type AllocationInfo struct {
	PartitionAllocationInfos []managment.PartitionAllocationInfo `json:"partition_allocation_infos"`
}

func main() {
	// Database configuration
	config := &helixLock.MySqlConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "credroot",
		Database: "automation",
	}

	// Connect to database
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	queries := helixClusterMysql.New(db)

	// Target allocation parameters
	cluster := "soak-test-cluster-1-0cf3c92b"
	domain := "domain-2-34a1aeaa"
	tasklist := "tasklist-9-7d33de03"

	fmt.Printf("🔍 ALLOCATION ANALYSIS\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("Cluster: %s\n", cluster)
	fmt.Printf("Domain: %s\n", domain)
	fmt.Printf("Tasklist: %s\n", tasklist)
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n\n")

	// First, get the expected partition count
	clusterInfo, err := queries.GetCluster(context.Background(), helixClusterMysql.GetClusterParams{
		Cluster:  cluster,
		Domain:   domain,
		Tasklist: tasklist,
	})
	if err != nil {
		log.Fatalf("Failed to get cluster info: %v", err)
	}

	expectedPartitions := int(clusterInfo.PartitionCount)
	fmt.Printf("📊 Expected Partitions: %d\n", expectedPartitions)

	// Get all allocations for this tasklist (including inactive ones)
	rows, err := db.Query(`
		SELECT ha.node_id, ha.status, ha.partition_info, hn.status as node_status
		FROM helix_allocation ha
		LEFT JOIN helix_nodes hn ON ha.node_id = hn.node_uuid AND ha.cluster = hn.cluster_name
		WHERE ha.cluster = ? AND ha.domain = ? AND ha.tasklist = ?
		ORDER BY ha.node_id`, cluster, domain, tasklist)
	if err != nil {
		log.Fatalf("Failed to query allocations: %v", err)
	}
	defer rows.Close()

	// Track all partitions we find
	allPartitions := make(map[string]PartitionInfo)
	nodeAllocations := make(map[string][]PartitionInfo)

	fmt.Printf("📋 ALLOCATION RECORDS:\n")
	fmt.Printf("───────────────────────────────────────────────────────────────────────────\n")

	for rows.Next() {
		var nodeID, partitionInfo string
		var allocationStatus int
		var nodeStatus sql.NullInt32

		if err := rows.Scan(&nodeID, &allocationStatus, &partitionInfo, &nodeStatus); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Skip inactive allocations as requested
		if allocationStatus == 0 {
			continue
		}

		nodeStatusStr := "INACTIVE"
		if nodeStatus.Valid && nodeStatus.Int32 == 1 {
			nodeStatusStr = "ACTIVE"
		}

		fmt.Printf("Node: %s (node_status=%s, alloc_status=%d)\n", nodeID[:8], nodeStatusStr, allocationStatus)

		// Parse partition info JSON
		var allocInfo AllocationInfo
		if err := json.Unmarshal([]byte(partitionInfo), &allocInfo); err != nil {
			fmt.Printf("  ❌ Failed to parse JSON: %v\n", err)
			continue
		}

		// Process each partition
		for _, partInfo := range allocInfo.PartitionAllocationInfos {
			partitionInfo := PartitionInfo{
				PartitionID: partInfo.PartitionId,
				NodeID:      nodeID,
				Status:      partInfo.AllocationStatus,
				NodeActive:  nodeStatus.Valid && nodeStatus.Int32 == 1,
			}

			fmt.Printf("  📦 Partition %s: %s\n", partInfo.PartitionId, partInfo.AllocationStatus)

			// Track in global partition map
			if existing, exists := allPartitions[partInfo.PartitionId]; exists {
				fmt.Printf("     ⚠️  DUPLICATE! Already assigned to node %s\n", existing.NodeID[:8])
			}
			allPartitions[partInfo.PartitionId] = partitionInfo

			// Track per node
			nodeAllocations[nodeID] = append(nodeAllocations[nodeID], partitionInfo)
		}
		fmt.Printf("\n")
	}

	// Analysis Summary
	fmt.Printf("🔍 ANALYSIS SUMMARY:\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("Expected Partitions: %d\n", expectedPartitions)
	fmt.Printf("Found Partitions: %d\n", len(allPartitions))
	fmt.Printf("Active Allocations: %d records\n", len(nodeAllocations))

	// Check for missing partitions
	missingPartitions := make([]string, 0)
	for i := 0; i < expectedPartitions; i++ {
		partitionID := strconv.Itoa(i)
		if _, exists := allPartitions[partitionID]; !exists {
			missingPartitions = append(missingPartitions, partitionID)
		}
	}

	// Categorize partitions by status
	statusCounts := make(map[managment.PartitionAllocationStatus]int)
	for _, partition := range allPartitions {
		statusCounts[partition.Status]++
	}

	fmt.Printf("\n📊 PARTITION STATUS BREAKDOWN:\n")
	for status, count := range statusCounts {
		fmt.Printf("  %s: %d partitions\n", status, count)
	}

	if len(missingPartitions) > 0 {
		fmt.Printf("\n❌ MISSING PARTITIONS (%d):\n", len(missingPartitions))
		sort.Strings(missingPartitions)
		for i, partID := range missingPartitions {
			if i < 20 { // Show first 20
				fmt.Printf("  %s", partID)
			}
			if i == 20 && len(missingPartitions) > 20 {
				fmt.Printf("  ... and %d more", len(missingPartitions)-20)
				break
			}
		}
		fmt.Printf("\n")
	}

	fmt.Printf("\n🏁 DIAGNOSIS:\n")
	if len(missingPartitions) == 0 && len(allPartitions) == expectedPartitions {
		fmt.Printf("✅ All partitions are accounted for!\n")
	} else {
		fmt.Printf("❌ Issues detected:\n")
		if len(missingPartitions) > 0 {
			fmt.Printf("   - %d partitions are completely missing\n", len(missingPartitions))
		}
		if len(allPartitions) != expectedPartitions {
			fmt.Printf("   - Expected %d partitions, found %d\n", expectedPartitions, len(allPartitions))
		}
	}
}

type PartitionInfo struct {
	PartitionID string
	NodeID      string
	Status      managment.PartitionAllocationStatus
	NodeActive  bool
}