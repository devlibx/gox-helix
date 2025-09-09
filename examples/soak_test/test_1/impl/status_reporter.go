package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
)

// TasklistStatus represents the status of a single tasklist
type TasklistStatus struct {
	Cluster          string
	Domain           string
	Tasklist         string
	TotalPartitions  int
	AssignedPartitions int
	UnassignedPartitions int
	AssignedNodes    []string
	LastUpdated      time.Time
}

// ClusterStatus represents the overall status of a cluster
type ClusterStatus struct {
	Name                   string
	TotalTasklists        int
	TotalPartitions       int
	TotalAssignedPartitions int
	TotalUnassignedPartitions int
	ActiveNodes           int
	CoordinatorChanges    int
	LastCoordinator       string
	Tasklists             []TasklistStatus
}

// PartitionAssignment represents a single partition assignment from the database
type PartitionAssignment struct {
	PartitionId      string                              `json:"partition_id"`
	AllocationStatus managment.PartitionAllocationStatus `json:"allocation_status"`
}

// AllocationInfo represents the structure of allocation data in the database
type AllocationInfo struct {
	PartitionAllocationInfos []PartitionAssignment `json:"partition_allocation_infos"`
}

// StatusReporter handles real-time monitoring and reporting of cluster status
type StatusReporter struct {
	queries     *helixClusterMysql.Queries
	nodeManager *NodeManager
	
	// Track coordinator changes
	lastCoordinators     map[string]string
	coordinatorChanges   map[string]int
}

// NewStatusReporter creates a new status reporter
func NewStatusReporter(queries *helixClusterMysql.Queries, nodeManager *NodeManager) *StatusReporter {
	return &StatusReporter{
		queries:              queries,
		nodeManager:          nodeManager,
		lastCoordinators:     make(map[string]string),
		coordinatorChanges:   make(map[string]int),
	}
}

// StartPeriodicReporting starts periodic status reporting
func (sr *StatusReporter) StartPeriodicReporting(ctx context.Context, interval time.Duration, clusterNames []string) {
	fmt.Printf("📊 Starting status reporting every %v...\n", interval)
	
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	reportCount := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("🛑 Status reporting stopped\n")
			return
			
		case <-ticker.C:
			reportCount++
			sr.generateAndPrintStatusReport(ctx, clusterNames, reportCount)
		}
	}
}

// generateAndPrintStatusReport generates and prints a comprehensive status report
func (sr *StatusReporter) generateAndPrintStatusReport(ctx context.Context, clusterNames []string, reportNumber int) {
	fmt.Printf("\n" + strings.Repeat("═", 80) + "\n")
	fmt.Printf("📊 STATUS REPORT #%d - %s\n", reportNumber, time.Now().Format("15:04:05"))
	fmt.Printf(strings.Repeat("═", 80) + "\n")
	
	totalClusters := len(clusterNames)
	grandTotalTasklists := 0
	grandTotalPartitions := 0
	grandTotalAssigned := 0
	grandTotalUnassigned := 0
	grandTotalNodes := 0
	
	for i, clusterName := range clusterNames {
		clusterStatus, err := sr.getClusterStatus(ctx, clusterName)
		if err != nil {
			fmt.Printf("❌ Error getting status for cluster %s: %v\n", clusterName, err)
			continue
		}
		
		// Print cluster summary
		sr.printClusterSummary(clusterStatus, i+1, totalClusters)
		
		// Print detailed tasklist breakdown if requested
		if len(clusterStatus.Tasklists) <= 10 { // Only show details for reasonable number
			sr.printTasklistDetails(clusterStatus)
		}
		
		// Accumulate totals
		grandTotalTasklists += clusterStatus.TotalTasklists
		grandTotalPartitions += clusterStatus.TotalPartitions
		grandTotalAssigned += clusterStatus.TotalAssignedPartitions
		grandTotalUnassigned += clusterStatus.TotalUnassignedPartitions
		grandTotalNodes += clusterStatus.ActiveNodes
	}
	
	// Print grand totals
	sr.printGrandTotals(grandTotalTasklists, grandTotalPartitions, grandTotalAssigned, grandTotalUnassigned, grandTotalNodes)
	fmt.Printf(strings.Repeat("═", 80) + "\n\n")
}

// getClusterStatus gets the current status for a specific cluster
func (sr *StatusReporter) getClusterStatus(ctx context.Context, clusterName string) (*ClusterStatus, error) {
	// Get all tasklists for this cluster
	domainsAndTasks, err := sr.queries.GetAllDomainsAndTaskListsByClusterCname(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to get domains and tasks: %w", err)
	}
	
	clusterStatus := &ClusterStatus{
		Name:                 clusterName,
		TotalTasklists:      len(domainsAndTasks),
		Tasklists:           make([]TasklistStatus, 0, len(domainsAndTasks)),
	}
	
	// Get node count
	nodeStats := sr.nodeManager.GetClusterStats()
	clusterStatus.ActiveNodes = nodeStats[clusterName]
	
	// Process each tasklist
	for _, dt := range domainsAndTasks {
		tasklistStatus, err := sr.getTasklistStatus(ctx, clusterName, dt.Domain, dt.Tasklist, int(dt.PartitionCount))
		if err != nil {
			// Continue with other tasklists even if one fails
			continue
		}
		
		clusterStatus.Tasklists = append(clusterStatus.Tasklists, *tasklistStatus)
		clusterStatus.TotalPartitions += tasklistStatus.TotalPartitions
		clusterStatus.TotalAssignedPartitions += tasklistStatus.AssignedPartitions
		clusterStatus.TotalUnassignedPartitions += tasklistStatus.UnassignedPartitions
	}
	
	return clusterStatus, nil
}

// getTasklistStatus gets the status for a specific tasklist
func (sr *StatusReporter) getTasklistStatus(ctx context.Context, cluster, domain, tasklist string, totalPartitions int) (*TasklistStatus, error) {
	// Get current allocations for this tasklist
	allocations, err := sr.queries.GetAllocationsForTasklist(ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  cluster,
		Domain:   domain,
		Tasklist: tasklist,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get allocations: %w", err)
	}
	
	// Count assigned partitions and track assigned nodes
	assignedPartitions := 0
	assignedNodesMap := make(map[string]bool)
	
	for _, allocation := range allocations {
		// Parse partition info to count actual assigned partitions
		assignedCount := sr.countAssignedPartitions(allocation.PartitionInfo)
		assignedPartitions += assignedCount
		if assignedCount > 0 {
			assignedNodesMap[allocation.NodeID] = true
		}
	}
	
	// Convert assigned nodes to slice
	assignedNodes := make([]string, 0, len(assignedNodesMap))
	for nodeID := range assignedNodesMap {
		assignedNodes = append(assignedNodes, nodeID[:8]) // Show only first 8 chars
	}
	sort.Strings(assignedNodes)
	
	return &TasklistStatus{
		Cluster:               cluster,
		Domain:                domain,
		Tasklist:              tasklist,
		TotalPartitions:       totalPartitions,
		AssignedPartitions:    assignedPartitions,
		UnassignedPartitions:  totalPartitions - assignedPartitions,
		AssignedNodes:         assignedNodes,
		LastUpdated:           time.Now(),
	}, nil
}

// countAssignedPartitions counts only partitions with "assigned" status
func (sr *StatusReporter) countAssignedPartitions(partitionInfo string) int {
	// Parse partition allocation info
	var allocationInfo AllocationInfo
	if err := json.Unmarshal([]byte(partitionInfo), &allocationInfo); err != nil {
		// If JSON parsing fails, fall back to string search (old behavior)
		return sr.fallbackPartitionCount(partitionInfo)
	}
	
	// Count only partitions with "assigned" status
	assignedCount := 0
	for _, partitionAssignment := range allocationInfo.PartitionAllocationInfos {
		if partitionAssignment.AllocationStatus == managment.PartitionAllocationAssigned {
			assignedCount++
		}
	}
	
	return assignedCount
}

// fallbackPartitionCount provides fallback counting when JSON parsing fails
func (sr *StatusReporter) fallbackPartitionCount(partitionInfo string) int {
	// Simple heuristic: count occurrences of "partition_id" in the JSON string
	count := 0
	searchStr := "partition_id"
	start := 0
	
	for {
		index := findSubstring(partitionInfo[start:], searchStr)
		if index == -1 {
			break
		}
		count++
		start += index + len(searchStr)
	}
	
	return count
}

// findSubstring finds the first occurrence of substr in str, returns -1 if not found
func findSubstring(str, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(substr) > len(str) {
		return -1
	}
	
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// printClusterSummary prints a summary for a single cluster
func (sr *StatusReporter) printClusterSummary(status *ClusterStatus, clusterIndex, totalClusters int) {
	assignedPercent := 0.0
	if status.TotalPartitions > 0 {
		assignedPercent = float64(status.TotalAssignedPartitions) / float64(status.TotalPartitions) * 100
	}
	
	statusIcon := "🔴" // Red for incomplete
	if status.TotalUnassignedPartitions == 0 {
		statusIcon = "🟢" // Green for complete
	} else if assignedPercent >= 80 {
		statusIcon = "🟡" // Yellow for mostly complete
	}
	
	fmt.Printf("%s Cluster %d/%d: %s\n", statusIcon, clusterIndex, totalClusters, status.Name)
	fmt.Printf("   📋 Tasklists: %d | 🧩 Partitions: %d total, %d assigned (%.1f%%), %d unassigned | 🖥️  Nodes: %d\n",
		status.TotalTasklists,
		status.TotalPartitions,
		status.TotalAssignedPartitions,
		assignedPercent,
		status.TotalUnassignedPartitions,
		status.ActiveNodes)
}

// printTasklistDetails prints detailed breakdown of tasklist statuses
func (sr *StatusReporter) printTasklistDetails(status *ClusterStatus) {
	if len(status.Tasklists) == 0 {
		return
	}
	
	fmt.Printf("   📊 Tasklist Details:\n")
	for _, tasklist := range status.Tasklists {
		assignedPercent := 0.0
		if tasklist.TotalPartitions > 0 {
			assignedPercent = float64(tasklist.AssignedPartitions) / float64(tasklist.TotalPartitions) * 100
		}
		
		statusIcon := "🔴"
		if tasklist.UnassignedPartitions == 0 {
			statusIcon = "🟢"
		} else if assignedPercent >= 80 {
			statusIcon = "🟡"
		}
		
		nodeList := "none"
		if len(tasklist.AssignedNodes) > 0 {
			if len(tasklist.AssignedNodes) <= 3 {
				nodeList = fmt.Sprintf("[%s]", strings.Join(tasklist.AssignedNodes, ","))
			} else {
				nodeList = fmt.Sprintf("[%s...+%d]", strings.Join(tasklist.AssignedNodes[:3], ","), len(tasklist.AssignedNodes)-3)
			}
		}
		
		fmt.Printf("      %s %s/%s: %d/%d assigned (%.0f%%) → %s\n",
			statusIcon,
			tasklist.Domain,
			tasklist.Tasklist,
			tasklist.AssignedPartitions,
			tasklist.TotalPartitions,
			assignedPercent,
			nodeList)
	}
}

// printGrandTotals prints overall statistics across all clusters
func (sr *StatusReporter) printGrandTotals(totalTasklists, totalPartitions, totalAssigned, totalUnassigned, totalNodes int) {
	overallPercent := 0.0
	if totalPartitions > 0 {
		overallPercent = float64(totalAssigned) / float64(totalPartitions) * 100
	}
	
	statusIcon := "🔴"
	if totalUnassigned == 0 {
		statusIcon = "🟢"
	} else if overallPercent >= 80 {
		statusIcon = "🟡"
	}
	
	fmt.Printf("\n%s OVERALL TOTALS:\n", statusIcon)
	fmt.Printf("   📋 Total Tasklists: %d\n", totalTasklists)
	fmt.Printf("   🧩 Total Partitions: %d\n", totalPartitions)
	fmt.Printf("   ✅ Assigned: %d (%.1f%%)\n", totalAssigned, overallPercent)
	fmt.Printf("   ❌ Unassigned: %d\n", totalUnassigned)
	fmt.Printf("   🖥️  Total Nodes: %d\n", totalNodes)
	
	if totalUnassigned == 0 {
		fmt.Printf("   🎉 ALL PARTITIONS ASSIGNED! 🎉\n")
	}
}

// GenerateFinalReport generates a comprehensive final report after test completion
func (sr *StatusReporter) GenerateFinalReport(ctx context.Context, clusterNames []string) error {
	fmt.Printf("\n" + strings.Repeat("█", 80) + "\n")
	fmt.Printf("📋 FINAL SOAK TEST REPORT - %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf(strings.Repeat("█", 80) + "\n")
	
	allComplete := true
	
	for _, clusterName := range clusterNames {
		clusterStatus, err := sr.getClusterStatus(ctx, clusterName)
		if err != nil {
			fmt.Printf("❌ Error getting final status for cluster %s: %v\n", clusterName, err)
			allComplete = false
			continue
		}
		
		sr.printClusterSummary(clusterStatus, 0, 0)
		
		if clusterStatus.TotalUnassignedPartitions > 0 {
			allComplete = false
			fmt.Printf("   ⚠️  WARNING: %d partitions remain unassigned!\n", clusterStatus.TotalUnassignedPartitions)
		}
	}
	
	fmt.Printf("\n🏁 SOAK TEST RESULT: ")
	if allComplete {
		fmt.Printf("✅ SUCCESS - All partitions assigned to active nodes!\n")
	} else {
		fmt.Printf("❌ INCOMPLETE - Some partitions remain unassigned!\n")
	}
	
	fmt.Printf(strings.Repeat("█", 80) + "\n")
	return nil
}