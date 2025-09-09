package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
)

// TasklistDataObject contains all data for a single tasklist's partition allocation
type TasklistDataObject struct {
	Cluster              string
	Domain               string
	Tasklist             string
	TotalPartitions      int
	AssignedPartitions   int
	UnassignedPartitions int
	RequestedPartitions  int // partitions marked for release request
	PendingPartitions    int // partitions marked for pending release
	AssignmentPercentage float64
	AssignedNodes        []string // list of node IDs that have assignments
	LastUpdated          time.Time
}

// ClusterDataObject contains aggregated data for all tasklists in a cluster
type ClusterDataObject struct {
	ClusterName          string
	TotalTasklists       int
	TotalPartitions      int
	AssignedPartitions   int
	UnassignedPartitions int
	RequestedPartitions  int
	PendingPartitions    int
	AssignmentPercentage float64
	ActiveNodes          int
	TasklistData         []TasklistDataObject
	LastUpdated          time.Time
}

// TasklistReporter interface for reporting single tasklist status
type TasklistReporter interface {
	ReportTasklist(data TasklistDataObject) string
}

// ClusterReporter interface for reporting unified cluster data
type ClusterReporter interface {
	ReportCluster(data ClusterDataObject) string
	ReportSummary(clusters []ClusterDataObject) string
}

// ConsoleTasklistReporter provides human-readable console output for tasklist data
type ConsoleTasklistReporter struct{}

// NewConsoleTasklistReporter creates a new console-based tasklist reporter
func NewConsoleTasklistReporter() TasklistReporter {
	return &ConsoleTasklistReporter{}
}

// ReportTasklist generates a human-readable report for a single tasklist
func (r *ConsoleTasklistReporter) ReportTasklist(data TasklistDataObject) string {
	var builder strings.Builder

	statusIcon := "🔴" // Red for incomplete
	if data.UnassignedPartitions == 0 {
		statusIcon = "🟢" // Green for complete
	} else if data.AssignmentPercentage >= 80 {
		statusIcon = "🟡" // Yellow for mostly complete
	}

	builder.WriteString(fmt.Sprintf("%s Tasklist: %s/%s\n", statusIcon, data.Domain, data.Tasklist))
	builder.WriteString(fmt.Sprintf("   Total Partitions: %d\n", data.TotalPartitions))
	builder.WriteString(fmt.Sprintf("      ✅ Assigned: %d (%.1f%%)\n", data.AssignedPartitions, data.AssignmentPercentage))
	builder.WriteString(fmt.Sprintf("      ❌ Unassigned: %d\n", data.UnassignedPartitions))

	if data.RequestedPartitions > 0 {
		builder.WriteString(fmt.Sprintf("      🔄 Requested Release: %d\n", data.RequestedPartitions))
	}
	if data.PendingPartitions > 0 {
		builder.WriteString(fmt.Sprintf("      ⏳ Pending Release: %d\n", data.PendingPartitions))
	}

	nodeList := "none"
	if len(data.AssignedNodes) > 0 {
		if len(data.AssignedNodes) <= 3 {
			nodeList = fmt.Sprintf("[%s]", strings.Join(data.AssignedNodes, ","))
		} else {
			nodeList = fmt.Sprintf("[%s...+%d]", strings.Join(data.AssignedNodes[:3], ","), len(data.AssignedNodes)-3)
		}
	}
	builder.WriteString(fmt.Sprintf("      🖥️  Assigned Nodes: %s\n", nodeList))

	return builder.String()
}

// ConsoleClusterReporter provides human-readable console output for cluster data
type ConsoleClusterReporter struct{}

// NewConsoleClusterReporter creates a new console-based cluster reporter
func NewConsoleClusterReporter() ClusterReporter {
	return &ConsoleClusterReporter{}
}

// ReportCluster generates a human-readable report for a single cluster
func (r *ConsoleClusterReporter) ReportCluster(data ClusterDataObject) string {
	var builder strings.Builder

	statusIcon := "🔴" // Red for incomplete
	if data.UnassignedPartitions == 0 {
		statusIcon = "🟢" // Green for complete
	} else if data.AssignmentPercentage >= 80 {
		statusIcon = "🟡" // Yellow for mostly complete
	}

	builder.WriteString(fmt.Sprintf("%s Cluster: %s\n", statusIcon, data.ClusterName))
	builder.WriteString(fmt.Sprintf("   📋 Tasklists: %d\n", data.TotalTasklists))
	builder.WriteString(fmt.Sprintf("   🧩 Total Partitions: %d\n", data.TotalPartitions))
	builder.WriteString(fmt.Sprintf("      ✅ Assigned: %d (%.1f%%)\n", data.AssignedPartitions, data.AssignmentPercentage))
	builder.WriteString(fmt.Sprintf("      ❌ Unassigned: %d\n", data.UnassignedPartitions))

	if data.RequestedPartitions > 0 {
		builder.WriteString(fmt.Sprintf("      🔄 Requested Release: %d\n", data.RequestedPartitions))
	}
	if data.PendingPartitions > 0 {
		builder.WriteString(fmt.Sprintf("      ⏳ Pending Release: %d\n", data.PendingPartitions))
	}

	builder.WriteString(fmt.Sprintf("   🖥️  Active Nodes: %d\n", data.ActiveNodes))

	return builder.String()
}

// ReportSummary generates a unified summary report across multiple clusters
func (r *ConsoleClusterReporter) ReportSummary(clusters []ClusterDataObject) string {
	var builder strings.Builder

	// Calculate totals
	totalClusters := len(clusters)
	grandTotalTasklists := 0
	grandTotalPartitions := 0
	grandTotalAssigned := 0
	grandTotalUnassigned := 0
	grandTotalRequested := 0
	grandTotalPending := 0
	grandTotalNodes := 0

	for _, cluster := range clusters {
		grandTotalTasklists += cluster.TotalTasklists
		grandTotalPartitions += cluster.TotalPartitions
		grandTotalAssigned += cluster.AssignedPartitions
		grandTotalUnassigned += cluster.UnassignedPartitions
		grandTotalRequested += cluster.RequestedPartitions
		grandTotalPending += cluster.PendingPartitions
		grandTotalNodes += cluster.ActiveNodes
	}

	overallPercent := 0.0
	if grandTotalPartitions > 0 {
		overallPercent = float64(grandTotalAssigned) / float64(grandTotalPartitions) * 100
	}

	statusIcon := "🔴"
	if grandTotalUnassigned == 0 {
		statusIcon = "🟢"
	} else if overallPercent >= 80 {
		statusIcon = "🟡"
	}

	builder.WriteString(fmt.Sprintf("\n%s OVERALL SUMMARY:\n", statusIcon))
	builder.WriteString(fmt.Sprintf("   📊 Clusters: %d\n", totalClusters))
	builder.WriteString(fmt.Sprintf("   📋 Total Tasklists: %d\n", grandTotalTasklists))
	builder.WriteString(fmt.Sprintf("   🧩 Total Partitions: %d\n", grandTotalPartitions))
	builder.WriteString(fmt.Sprintf("      ✅ Assigned: %d (%.1f%%)\n", grandTotalAssigned, overallPercent))
	builder.WriteString(fmt.Sprintf("      ❌ Unassigned: %d\n", grandTotalUnassigned))

	if grandTotalRequested > 0 {
		builder.WriteString(fmt.Sprintf("      🔄 Requested Release: %d\n", grandTotalRequested))
	}
	if grandTotalPending > 0 {
		builder.WriteString(fmt.Sprintf("      ⏳ Pending Release: %d\n", grandTotalPending))
	}

	builder.WriteString(fmt.Sprintf("   🖥️  Total Nodes: %d\n", grandTotalNodes))

	if grandTotalUnassigned == 0 {
		builder.WriteString("   🎉 ALL PARTITIONS ASSIGNED! 🎉\n")
	}

	return builder.String()
}

// PartitionAllocationInfo represents a single partition assignment from the database
type PartitionAllocationInfo struct {
	PartitionId      string                              `json:"partition_id"`
	AllocationStatus managment.PartitionAllocationStatus `json:"allocation_status"`
}

// AllocationInfo represents the structure of allocation data in the database
type AllocationInfo struct {
	PartitionAllocationInfos []PartitionAllocationInfo `json:"partition_allocation_infos"`
}

// ReporterDataBuilder builds reporting data objects from database queries
type ReporterDataBuilder struct {
	queries helixClusterMysql.Querier
}

// NewReporterDataBuilder creates a new data builder
func NewReporterDataBuilder(queries helixClusterMysql.Querier) *ReporterDataBuilder {
	return &ReporterDataBuilder{
		queries: queries,
	}
}

// BuildTasklistData builds a TasklistDataObject for a specific tasklist
func (b *ReporterDataBuilder) BuildTasklistData(ctx context.Context, cluster, domain, tasklist string, totalPartitions int) (*TasklistDataObject, error) {
	// Get current allocations for this tasklist
	allocations, err := b.queries.GetAllocationsForTasklist(ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  cluster,
		Domain:   domain,
		Tasklist: tasklist,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get allocations: %w", err)
	}

	data := &TasklistDataObject{
		Cluster:         cluster,
		Domain:          domain,
		Tasklist:        tasklist,
		TotalPartitions: totalPartitions,
		LastUpdated:     time.Now(),
	}

	// Track unique partitions by status to avoid double-counting conflicts
	assignedPartitions := make(map[string]bool)
	requestedPartitions := make(map[string]bool)
	pendingPartitions := make(map[string]bool)
	assignedNodesMap := make(map[string]bool)

	for _, allocation := range allocations {
		nodeId := allocation.NodeID
		partitionStatuses := b.getPartitionStatusMap(allocation.PartitionInfo)

		// Track unique partitions and nodes
		for partitionId, status := range partitionStatuses {
			switch status {
			case managment.PartitionAllocationAssigned:
				assignedPartitions[partitionId] = true
				assignedNodesMap[nodeId] = true
			case managment.PartitionAllocationRequestedRelease:
				requestedPartitions[partitionId] = true
				assignedNodesMap[nodeId] = true
			case managment.PartitionAllocationPendingRelease:
				pendingPartitions[partitionId] = true
				assignedNodesMap[nodeId] = true
			}
		}
	}

	// Convert counts from unique partition maps
	data.AssignedPartitions = len(assignedPartitions)
	data.RequestedPartitions = len(requestedPartitions)
	data.PendingPartitions = len(pendingPartitions)

	// Convert assigned nodes to slice and sort
	data.AssignedNodes = make([]string, 0, len(assignedNodesMap))
	for nodeId := range assignedNodesMap {
		data.AssignedNodes = append(data.AssignedNodes, nodeId[:8]) // Show only first 8 chars
	}
	sort.Strings(data.AssignedNodes)

	// Calculate derived values
	data.UnassignedPartitions = data.TotalPartitions - data.AssignedPartitions
	if data.TotalPartitions > 0 {
		data.AssignmentPercentage = float64(data.AssignedPartitions) / float64(data.TotalPartitions) * 100
	}

	return data, nil
}

// BuildClusterData builds a ClusterDataObject for a specific cluster
func (b *ReporterDataBuilder) BuildClusterData(ctx context.Context, clusterName string, activeNodes int) (*ClusterDataObject, error) {
	// Get all tasklists for this cluster
	domainsAndTasks, err := b.queries.GetAllDomainsAndTaskListsByClusterCname(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to get domains and tasks: %w", err)
	}

	data := &ClusterDataObject{
		ClusterName:    clusterName,
		TotalTasklists: len(domainsAndTasks),
		ActiveNodes:    activeNodes,
		TasklistData:   make([]TasklistDataObject, 0, len(domainsAndTasks)),
		LastUpdated:    time.Now(),
	}

	// Process each tasklist
	for _, dt := range domainsAndTasks {
		tasklistData, err := b.BuildTasklistData(ctx, clusterName, dt.Domain, dt.Tasklist, int(dt.PartitionCount))
		if err != nil {
			// Continue with other tasklists even if one fails
			continue
		}

		data.TasklistData = append(data.TasklistData, *tasklistData)
		data.TotalPartitions += tasklistData.TotalPartitions
		data.AssignedPartitions += tasklistData.AssignedPartitions
		data.UnassignedPartitions += tasklistData.UnassignedPartitions
		data.RequestedPartitions += tasklistData.RequestedPartitions
		data.PendingPartitions += tasklistData.PendingPartitions
	}

	// Calculate assignment percentage
	if data.TotalPartitions > 0 {
		data.AssignmentPercentage = float64(data.AssignedPartitions) / float64(data.TotalPartitions) * 100
	}

	return data, nil
}

// getPartitionStatusMap returns a map of partition ID to status from JSON data
func (b *ReporterDataBuilder) getPartitionStatusMap(partitionInfo string) map[string]managment.PartitionAllocationStatus {
	result := make(map[string]managment.PartitionAllocationStatus)
	
	// Parse partition allocation info
	var allocationInfo AllocationInfo
	if err := json.Unmarshal([]byte(partitionInfo), &allocationInfo); err != nil {
		// If JSON parsing fails, return empty map
		return result
	}

	// Build partition status map
	for _, partitionAllocation := range allocationInfo.PartitionAllocationInfos {
		result[partitionAllocation.PartitionId] = partitionAllocation.AllocationStatus
	}

	return result
}

// countPartitionsByStatus counts partitions by their allocation status (kept for compatibility)
func (b *ReporterDataBuilder) countPartitionsByStatus(partitionInfo string) (assigned, requested, pending int) {
	statusMap := b.getPartitionStatusMap(partitionInfo)
	
	// Count partitions by status
	for _, status := range statusMap {
		switch status {
		case managment.PartitionAllocationAssigned:
			assigned++
		case managment.PartitionAllocationRequestedRelease:
			requested++
		case managment.PartitionAllocationPendingRelease:
			pending++
		}
	}

	return assigned, requested, pending
}

// fallbackPartitionCount provides fallback counting when JSON parsing fails
func (b *ReporterDataBuilder) fallbackPartitionCount(partitionInfo string) int {
	// Simple heuristic: count occurrences of "partition_id" in the JSON string
	count := 0
	searchStr := "partition_id"
	start := 0

	for {
		index := b.findSubstring(partitionInfo[start:], searchStr)
		if index == -1 {
			break
		}
		count++
		start += index + len(searchStr)
	}

	return count
}

// findSubstring finds the first occurrence of substr in str, returns -1 if not found
func (b *ReporterDataBuilder) findSubstring(str, substr string) int {
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
