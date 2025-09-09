package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
)

// ValidationResult represents the result of partition allocation validation
type ValidationResult struct {
	Success              bool
	TotalClusters        int
	TotalTasklists       int
	TotalPartitions      int
	AssignedPartitions   int
	UnassignedPartitions int
	InvalidAssignments   int
	ClusterResults       map[string]*ClusterValidationResult
	ValidationErrors     []string
}

// ClusterValidationResult represents validation results for a single cluster
type ClusterValidationResult struct {
	ClusterName          string
	Success              bool
	TotalTasklists       int
	TotalPartitions      int
	AssignedPartitions   int
	UnassignedPartitions int
	InvalidAssignments   int
	ActiveNodes          []string
	TasklistResults      map[string]*TasklistValidationResult
	ValidationErrors     []string
}

// TasklistValidationResult represents validation results for a single tasklist
type TasklistValidationResult struct {
	Cluster              string
	Domain               string
	Tasklist             string
	TotalPartitions      int
	AssignedPartitions   int
	UnassignedPartitions int
	InvalidAssignments   int
	UnassignedPartitionIds []string
	InvalidAssignmentDetails []string
	PartitionAssignments map[string]string // partitionId -> nodeId
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

// Validator handles comprehensive validation of partition allocation completeness
type Validator struct {
	queries     *helixClusterMysql.Queries
	nodeManager *NodeManager
}

// NewValidator creates a new validator instance
func NewValidator(queries *helixClusterMysql.Queries, nodeManager *NodeManager) *Validator {
	return &Validator{
		queries:     queries,
		nodeManager: nodeManager,
	}
}

// ValidatePartitionAllocation performs comprehensive validation of partition allocation
func (v *Validator) ValidatePartitionAllocation(ctx context.Context, clusterNames []string) (*ValidationResult, error) {
	fmt.Printf("🔍 Starting comprehensive partition allocation validation...\n")
	
	result := &ValidationResult{
		Success:        true,
		ClusterResults: make(map[string]*ClusterValidationResult),
	}
	
	// Validate each cluster
	for _, clusterName := range clusterNames {
		clusterResult, err := v.validateCluster(ctx, clusterName)
		if err != nil {
			result.ValidationErrors = append(result.ValidationErrors, 
				fmt.Sprintf("Cluster %s validation failed: %v", clusterName, err))
			result.Success = false
			continue
		}
		
		result.ClusterResults[clusterName] = clusterResult
		result.TotalClusters++
		result.TotalTasklists += clusterResult.TotalTasklists
		result.TotalPartitions += clusterResult.TotalPartitions
		result.AssignedPartitions += clusterResult.AssignedPartitions
		result.UnassignedPartitions += clusterResult.UnassignedPartitions
		result.InvalidAssignments += clusterResult.InvalidAssignments
		
		if !clusterResult.Success {
			result.Success = false
		}
	}
	
	// Print validation results
	v.printValidationResults(result)
	
	return result, nil
}

// validateCluster validates partition allocation for a single cluster
func (v *Validator) validateCluster(ctx context.Context, clusterName string) (*ClusterValidationResult, error) {
	result := &ClusterValidationResult{
		ClusterName:     clusterName,
		Success:         true,
		TasklistResults: make(map[string]*TasklistValidationResult),
	}
	
	// Get all tasklists for this cluster
	domainsAndTasks, err := v.queries.GetAllDomainsAndTaskListsByClusterCname(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to get domains and tasks: %w", err)
	}
	
	// Get active nodes for this cluster
	activeNodes, err := v.queries.GetActiveNodes(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to get active nodes: %w", err)
	}
	
	// Create map of active node IDs
	activeNodeIds := make(map[string]bool)
	for _, node := range activeNodes {
		activeNodeIds[node.NodeUuid] = true
		result.ActiveNodes = append(result.ActiveNodes, node.NodeUuid[:8]) // Short ID for display
	}
	sort.Strings(result.ActiveNodes)
	
	result.TotalTasklists = len(domainsAndTasks)
	
	// Validate each tasklist
	for _, dt := range domainsAndTasks {
		tasklistKey := fmt.Sprintf("%s/%s", dt.Domain, dt.Tasklist)
		tasklistResult, err := v.validateTasklist(ctx, clusterName, dt.Domain, dt.Tasklist, 
			int(dt.PartitionCount), activeNodeIds)
		if err != nil {
			result.ValidationErrors = append(result.ValidationErrors, 
				fmt.Sprintf("Tasklist %s validation failed: %v", tasklistKey, err))
			result.Success = false
			continue
		}
		
		result.TasklistResults[tasklistKey] = tasklistResult
		result.TotalPartitions += tasklistResult.TotalPartitions
		result.AssignedPartitions += tasklistResult.AssignedPartitions
		result.UnassignedPartitions += tasklistResult.UnassignedPartitions
		result.InvalidAssignments += tasklistResult.InvalidAssignments
		
		if tasklistResult.UnassignedPartitions > 0 || tasklistResult.InvalidAssignments > 0 {
			result.Success = false
		}
	}
	
	return result, nil
}

// validateTasklist validates partition allocation for a single tasklist
func (v *Validator) validateTasklist(ctx context.Context, cluster, domain, tasklist string, 
	totalPartitions int, activeNodeIds map[string]bool) (*TasklistValidationResult, error) {
	
	result := &TasklistValidationResult{
		Cluster:                  cluster,
		Domain:                   domain,
		Tasklist:                 tasklist,
		TotalPartitions:          totalPartitions,
		PartitionAssignments:     make(map[string]string),
		UnassignedPartitionIds:   make([]string, 0),
		InvalidAssignmentDetails: make([]string, 0),
	}
	
	// Get all allocations for this tasklist
	allocations, err := v.queries.GetAllocationsForTasklist(ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  cluster,
		Domain:   domain,
		Tasklist: tasklist,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get allocations: %w", err)
	}
	
	// Parse all allocations to build partition assignment map
	for _, allocation := range allocations {
		nodeId := allocation.NodeID
		
		// Check if assigned node is active
		if !activeNodeIds[nodeId] {
			result.InvalidAssignmentDetails = append(result.InvalidAssignmentDetails,
				fmt.Sprintf("Node %s is inactive but has assignments", nodeId[:8]))
		}
		
		// Parse partition allocation info
		var allocationInfo AllocationInfo
		if err := json.Unmarshal([]byte(allocation.PartitionInfo), &allocationInfo); err != nil {
			// If JSON parsing fails, try to extract partition IDs manually
			partitionIds := v.extractPartitionIds(allocation.PartitionInfo)
			for _, partitionId := range partitionIds {
				if existing, exists := result.PartitionAssignments[partitionId]; exists {
					result.InvalidAssignmentDetails = append(result.InvalidAssignmentDetails,
						fmt.Sprintf("Partition %s assigned to multiple nodes: %s and %s", 
							partitionId, existing[:8], nodeId[:8]))
					result.InvalidAssignments++
				} else {
					result.PartitionAssignments[partitionId] = nodeId
				}
			}
			continue
		}
		
		// Process parsed partition assignments
		for _, partitionAssignment := range allocationInfo.PartitionAllocationInfos {
			partitionId := partitionAssignment.PartitionId
			
			// Only count assignments that are in "assigned" status
			if partitionAssignment.AllocationStatus == managment.PartitionAllocationAssigned {
				if existing, exists := result.PartitionAssignments[partitionId]; exists {
					result.InvalidAssignmentDetails = append(result.InvalidAssignmentDetails,
						fmt.Sprintf("Partition %s assigned to multiple nodes: %s and %s", 
							partitionId, existing[:8], nodeId[:8]))
					result.InvalidAssignments++
				} else {
					result.PartitionAssignments[partitionId] = nodeId
				}
			}
		}
	}
	
	// Check for unassigned partitions
	for i := 0; i < totalPartitions; i++ {
		partitionId := strconv.Itoa(i)
		if _, assigned := result.PartitionAssignments[partitionId]; !assigned {
			result.UnassignedPartitionIds = append(result.UnassignedPartitionIds, partitionId)
		}
	}
	
	// Calculate final counts
	result.AssignedPartitions = len(result.PartitionAssignments) - result.InvalidAssignments
	result.UnassignedPartitions = len(result.UnassignedPartitionIds)
	
	return result, nil
}

// extractPartitionIds extracts partition IDs from allocation JSON string using simple parsing
func (v *Validator) extractPartitionIds(partitionInfo string) []string {
	partitionIds := make([]string, 0)
	
	// Simple extraction: look for "partition_id":"<value>" patterns
	parts := strings.Split(partitionInfo, "\"partition_id\":")
	for i := 1; i < len(parts); i++ {
		part := strings.TrimSpace(parts[i])
		if len(part) > 2 && part[0] == '"' {
			endQuote := strings.Index(part[1:], "\"")
			if endQuote > 0 {
				partitionId := part[1 : endQuote+1]
				partitionIds = append(partitionIds, partitionId)
			}
		}
	}
	
	return partitionIds
}

// printValidationResults prints comprehensive validation results
func (v *Validator) printValidationResults(result *ValidationResult) {
	fmt.Printf("\n" + strings.Repeat("█", 80) + "\n")
	fmt.Printf("🔍 PARTITION ALLOCATION VALIDATION RESULTS\n")
	fmt.Printf(strings.Repeat("█", 80) + "\n")
	
	// Overall summary
	overallStatus := "✅ SUCCESS"
	if !result.Success {
		overallStatus = "❌ FAILED"
	}
	
	fmt.Printf("%s OVERALL VALIDATION: %s\n", overallStatus, overallStatus)
	fmt.Printf("📊 Summary:\n")
	fmt.Printf("   - Clusters: %d\n", result.TotalClusters)
	fmt.Printf("   - Tasklists: %d\n", result.TotalTasklists)
	fmt.Printf("   - Total Partitions: %d\n", result.TotalPartitions)
	fmt.Printf("   - Assigned Partitions: %d\n", result.AssignedPartitions)
	fmt.Printf("   - Unassigned Partitions: %d\n", result.UnassignedPartitions)
	fmt.Printf("   - Invalid Assignments: %d\n", result.InvalidAssignments)
	
	if result.TotalPartitions > 0 {
		assignedPercent := float64(result.AssignedPartitions) / float64(result.TotalPartitions) * 100
		fmt.Printf("   - Assignment Rate: %.2f%%\n", assignedPercent)
	}
	
	// Detailed cluster results
	fmt.Printf("\n📋 DETAILED RESULTS BY CLUSTER:\n")
	for _, clusterResult := range result.ClusterResults {
		v.printClusterValidationResult(clusterResult)
	}
	
	// Print validation errors
	if len(result.ValidationErrors) > 0 {
		fmt.Printf("\n❌ VALIDATION ERRORS:\n")
		for i, err := range result.ValidationErrors {
			fmt.Printf("   %d. %s\n", i+1, err)
		}
	}
	
	// Final verdict
	fmt.Printf("\n🏁 FINAL VERDICT: ")
	if result.Success {
		fmt.Printf("🎉 ALL PARTITIONS SUCCESSFULLY ASSIGNED! 🎉\n")
		fmt.Printf("✅ Cluster has converged to stable state with complete partition allocation.\n")
	} else {
		fmt.Printf("💥 PARTITION ALLOCATION INCOMPLETE!\n")
		fmt.Printf("❌ %d partitions remain unassigned or have invalid assignments.\n", 
			result.UnassignedPartitions + result.InvalidAssignments)
		fmt.Printf("⚠️  Cluster has NOT converged to stable state.\n")
	}
	
	fmt.Printf(strings.Repeat("█", 80) + "\n")
}

// printClusterValidationResult prints detailed results for a single cluster
func (v *Validator) printClusterValidationResult(result *ClusterValidationResult) {
	status := "✅"
	if !result.Success {
		status = "❌"
	}
	
	fmt.Printf("%s Cluster: %s\n", status, result.ClusterName)
	fmt.Printf("   📊 Stats: %d tasklists, %d partitions, %d assigned, %d unassigned, %d invalid\n",
		result.TotalTasklists, result.TotalPartitions, result.AssignedPartitions, 
		result.UnassignedPartitions, result.InvalidAssignments)
	fmt.Printf("   🖥️  Active Nodes (%d): [%s]\n", len(result.ActiveNodes), strings.Join(result.ActiveNodes, ", "))
	
	// Show problematic tasklists
	for tasklistKey, tasklistResult := range result.TasklistResults {
		if tasklistResult.UnassignedPartitions > 0 || tasklistResult.InvalidAssignments > 0 {
			fmt.Printf("   ⚠️  %s: %d unassigned, %d invalid assignments\n", 
				tasklistKey, tasklistResult.UnassignedPartitions, tasklistResult.InvalidAssignments)
			
			if len(tasklistResult.UnassignedPartitionIds) > 0 {
				fmt.Printf("      🔸 Unassigned partitions: [%s]\n", 
					strings.Join(tasklistResult.UnassignedPartitionIds, ", "))
			}
			
			for _, detail := range tasklistResult.InvalidAssignmentDetails {
				fmt.Printf("      🔸 %s\n", detail)
			}
		}
	}
}