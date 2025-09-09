package impl

import (
	"context"
	"fmt"
	"strings"
	"time"

	coordinator "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
)

// StatusReporter handles real-time monitoring and reporting of cluster status using the new reporter system
type StatusReporter struct {
	dataBuilder      *coordinator.ReporterDataBuilder
	clusterReporter  coordinator.ClusterReporter
	tasklistReporter coordinator.TasklistReporter
	nodeManager      *NodeManager
}

// NewStatusReporter creates a new status reporter
func NewStatusReporter(queries *helixClusterMysql.Queries, nodeManager *NodeManager) *StatusReporter {
	return &StatusReporter{
		dataBuilder:      coordinator.NewReporterDataBuilder(queries),
		clusterReporter:  coordinator.NewConsoleClusterReporter(),
		tasklistReporter: coordinator.NewConsoleTasklistReporter(),
		nodeManager:      nodeManager,
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
	
	var clusters []coordinator.ClusterDataObject
	
	for _, clusterName := range clusterNames {
		nodeStats := sr.nodeManager.GetClusterStats()
		activeNodes := nodeStats[clusterName]
		
		clusterData, err := sr.dataBuilder.BuildClusterData(ctx, clusterName, activeNodes)
		if err != nil {
			fmt.Printf("❌ Error getting status for cluster %s: %v\n", clusterName, err)
			continue
		}
		
		// Print cluster report
		fmt.Print(sr.clusterReporter.ReportCluster(*clusterData))
		
		// Print detailed tasklist breakdown if reasonable number
		if len(clusterData.TasklistData) <= 10 {
			for _, tasklistData := range clusterData.TasklistData {
				fmt.Print("   ")
				fmt.Print(strings.Replace(sr.tasklistReporter.ReportTasklist(tasklistData), "\n", "\n   ", -1))
			}
		}
		
		clusters = append(clusters, *clusterData)
	}
	
	// Print grand totals summary
	fmt.Print(sr.clusterReporter.ReportSummary(clusters))
	fmt.Printf(strings.Repeat("═", 80) + "\n\n")
}

// GenerateFinalReport generates a comprehensive final report after test completion
func (sr *StatusReporter) GenerateFinalReport(ctx context.Context, clusterNames []string) error {
	fmt.Printf("\n" + strings.Repeat("█", 80) + "\n")
	fmt.Printf("📋 FINAL SOAK TEST REPORT - %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf(strings.Repeat("█", 80) + "\n")
	
	var clusters []coordinator.ClusterDataObject
	allComplete := true
	
	for _, clusterName := range clusterNames {
		nodeStats := sr.nodeManager.GetClusterStats()
		activeNodes := nodeStats[clusterName]
		
		clusterData, err := sr.dataBuilder.BuildClusterData(ctx, clusterName, activeNodes)
		if err != nil {
			fmt.Printf("❌ Error getting final status for cluster %s: %v\n", clusterName, err)
			allComplete = false
			continue
		}
		
		// Print cluster report
		fmt.Print(sr.clusterReporter.ReportCluster(*clusterData))
		
		if clusterData.UnassignedPartitions > 0 {
			allComplete = false
			fmt.Printf("   ⚠️  WARNING: %d partitions remain unassigned!\n", clusterData.UnassignedPartitions)
		}
		
		clusters = append(clusters, *clusterData)
	}
	
	// Print overall summary
	fmt.Print(sr.clusterReporter.ReportSummary(clusters))
	
	fmt.Printf("\n🏁 SOAK TEST RESULT: ")
	if allComplete {
		fmt.Printf("✅ SUCCESS - All partitions assigned to active nodes!\n")
	} else {
		fmt.Printf("❌ INCOMPLETE - Some partitions remain unassigned!\n")
	}
	
	fmt.Printf(strings.Repeat("█", 80) + "\n")
	return nil
}