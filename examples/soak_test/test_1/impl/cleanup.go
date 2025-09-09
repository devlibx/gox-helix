package impl

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// CleanupManager handles database cleanup operations for soak tests
type CleanupManager struct {
	sqlDb *sql.DB
}

// NewCleanupManager creates a new cleanup manager
func NewCleanupManager(sqlDb *sql.DB) *CleanupManager {
	return &CleanupManager{
		sqlDb: sqlDb,
	}
}

// CleanupTestData removes all test data for the specified clusters
func (cm *CleanupManager) CleanupTestData(ctx context.Context, clusterNames []string) error {
	if len(clusterNames) == 0 {
		return nil
	}
	
	fmt.Printf("🧹 Starting cleanup of test data for %d clusters...\n", len(clusterNames))
	
	totalCleaned := 0
	for i, clusterName := range clusterNames {
		cleaned, err := cm.cleanupSingleCluster(ctx, clusterName)
		if err != nil {
			fmt.Printf("⚠️  Warning: Failed to cleanup cluster %s: %v\n", clusterName, err)
			continue
		}
		
		totalCleaned += cleaned
		fmt.Printf("✅ Cleaned cluster %d/%d: %s (%d records)\n", i+1, len(clusterNames), clusterName, cleaned)
	}
	
	fmt.Printf("🎯 Cleanup completed: %d total records removed\n", totalCleaned)
	return nil
}

// cleanupSingleCluster removes all data for a single cluster
func (cm *CleanupManager) cleanupSingleCluster(ctx context.Context, clusterName string) (int, error) {
	totalCleaned := 0
	
	// Cleanup in reverse dependency order to avoid foreign key issues
	
	// 1. Clean up allocations (references nodes and cluster)
	allocationsCount, err := cm.cleanupAllocations(ctx, clusterName)
	if err != nil {
		return totalCleaned, fmt.Errorf("failed to cleanup allocations: %w", err)
	}
	totalCleaned += allocationsCount
	
	// 2. Clean up nodes
	nodesCount, err := cm.cleanupNodes(ctx, clusterName)
	if err != nil {
		return totalCleaned, fmt.Errorf("failed to cleanup nodes: %w", err)
	}
	totalCleaned += nodesCount
	
	// 3. Clean up cluster definitions (domains/tasklists)
	clusterCount, err := cm.cleanupClusterDefinitions(ctx, clusterName)
	if err != nil {
		return totalCleaned, fmt.Errorf("failed to cleanup cluster definitions: %w", err)
	}
	totalCleaned += clusterCount
	
	// 4. Clean up distributed locks for this cluster
	locksCount, err := cm.cleanupDistributedLocks(ctx, clusterName)
	if err != nil {
		// Don't fail cleanup if locks cleanup fails - just warn
		fmt.Printf("⚠️  Warning: Failed to cleanup locks for cluster %s: %v\n", clusterName, err)
	} else {
		totalCleaned += locksCount
	}
	
	return totalCleaned, nil
}

// cleanupAllocations removes all allocations for a cluster
func (cm *CleanupManager) cleanupAllocations(ctx context.Context, clusterName string) (int, error) {
	query := "DELETE FROM helix_allocation WHERE cluster = ?"
	result, err := cm.sqlDb.ExecContext(ctx, query, clusterName)
	if err != nil {
		// If table doesn't exist, that's fine - return 0
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// cleanupNodes removes all nodes for a cluster
func (cm *CleanupManager) cleanupNodes(ctx context.Context, clusterName string) (int, error) {
	query := "DELETE FROM helix_nodes WHERE cluster_name = ?"
	result, err := cm.sqlDb.ExecContext(ctx, query, clusterName)
	if err != nil {
		// If table doesn't exist, that's fine - return 0
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// cleanupClusterDefinitions removes cluster/domain/tasklist definitions
func (cm *CleanupManager) cleanupClusterDefinitions(ctx context.Context, clusterName string) (int, error) {
	query := "DELETE FROM helix_cluster WHERE cluster = ?"
	result, err := cm.sqlDb.ExecContext(ctx, query, clusterName)
	if err != nil {
		// If table doesn't exist, that's fine - return 0
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// cleanupDistributedLocks removes distributed locks related to the cluster
func (cm *CleanupManager) cleanupDistributedLocks(ctx context.Context, clusterName string) (int, error) {
	// Clean up coordinator locks for this cluster
	lockKey := fmt.Sprintf("cluster-controller-%s", clusterName)
	query := "DELETE FROM helix_lock WHERE lock_key = ?"
	result, err := cm.sqlDb.ExecContext(ctx, query, lockKey)
	if err != nil {
		// If table doesn't exist, that's fine - return 0
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// CleanupAllTestData removes all test data from the database (nuclear option)
func (cm *CleanupManager) CleanupAllTestData(ctx context.Context) error {
	fmt.Printf("💥 NUCLEAR CLEANUP: Removing all test data from database...\n")
	
	tables := []string{
		"helix_allocation",
		"helix_nodes", 
		"helix_cluster",
		"helix_lock",
	}
	
	totalCleaned := 0
	for _, table := range tables {
		count, err := cm.cleanupTable(ctx, table)
		if err != nil {
			fmt.Printf("⚠️  Warning: Failed to cleanup table %s: %v\n", table, err)
			continue
		}
		
		totalCleaned += count
		fmt.Printf("🗑️  Cleaned table %s: %d records\n", table, count)
	}
	
	fmt.Printf("💥 Nuclear cleanup completed: %d total records removed\n", totalCleaned)
	return nil
}

// cleanupTable removes all records from a table
func (cm *CleanupManager) cleanupTable(ctx context.Context, tableName string) (int, error) {
	query := fmt.Sprintf("DELETE FROM %s", tableName)
	result, err := cm.sqlDb.ExecContext(ctx, query)
	if err != nil {
		// If table doesn't exist, that's fine - return 0
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// CleanupOldTestData removes test data older than the specified duration
func (cm *CleanupManager) CleanupOldTestData(ctx context.Context, olderThan time.Duration) error {
	cutoffTime := time.Now().Add(-olderThan)
	fmt.Printf("🕰️  Cleaning up test data older than %v (before %s)...\n", 
		olderThan, cutoffTime.Format("2006-01-02 15:04:05"))
	
	totalCleaned := 0
	
	// Cleanup old allocations
	count, err := cm.cleanupOldAllocations(ctx, cutoffTime)
	if err != nil {
		fmt.Printf("⚠️  Warning: Failed to cleanup old allocations: %v\n", err)
	} else {
		totalCleaned += count
		fmt.Printf("🗑️  Cleaned old allocations: %d records\n", count)
	}
	
	// Cleanup old nodes
	count, err = cm.cleanupOldNodes(ctx, cutoffTime)
	if err != nil {
		fmt.Printf("⚠️  Warning: Failed to cleanup old nodes: %v\n", err)
	} else {
		totalCleaned += count
		fmt.Printf("🗑️  Cleaned old nodes: %d records\n", count)
	}
	
	// Cleanup old cluster definitions
	count, err = cm.cleanupOldClusterDefinitions(ctx, cutoffTime)
	if err != nil {
		fmt.Printf("⚠️  Warning: Failed to cleanup old cluster definitions: %v\n", err)
	} else {
		totalCleaned += count
		fmt.Printf("🗑️  Cleaned old cluster definitions: %d records\n", count)
	}
	
	// Cleanup old locks
	count, err = cm.cleanupOldLocks(ctx, cutoffTime)
	if err != nil {
		fmt.Printf("⚠️  Warning: Failed to cleanup old locks: %v\n", err)
	} else {
		totalCleaned += count
		fmt.Printf("🗑️  Cleaned old locks: %d records\n", count)
	}
	
	fmt.Printf("🕰️  Time-based cleanup completed: %d total records removed\n", totalCleaned)
	return nil
}

// cleanupOldAllocations removes allocations older than cutoff time
func (cm *CleanupManager) cleanupOldAllocations(ctx context.Context, cutoffTime time.Time) (int, error) {
	query := "DELETE FROM helix_allocation WHERE created_at < ?"
	result, err := cm.sqlDb.ExecContext(ctx, query, cutoffTime)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// cleanupOldNodes removes nodes older than cutoff time
func (cm *CleanupManager) cleanupOldNodes(ctx context.Context, cutoffTime time.Time) (int, error) {
	query := "DELETE FROM helix_nodes WHERE created_at < ?"
	result, err := cm.sqlDb.ExecContext(ctx, query, cutoffTime)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// cleanupOldClusterDefinitions removes cluster definitions older than cutoff time
func (cm *CleanupManager) cleanupOldClusterDefinitions(ctx context.Context, cutoffTime time.Time) (int, error) {
	query := "DELETE FROM helix_cluster WHERE created_at < ?"
	result, err := cm.sqlDb.ExecContext(ctx, query, cutoffTime)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// cleanupOldLocks removes locks older than cutoff time
func (cm *CleanupManager) cleanupOldLocks(ctx context.Context, cutoffTime time.Time) (int, error) {
	query := "DELETE FROM helix_lock WHERE created_at < ?"
	result, err := cm.sqlDb.ExecContext(ctx, query, cutoffTime)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	
	return int(count), nil
}

// PerformPreTestCleanup performs cleanup before starting a test
func (cm *CleanupManager) PerformPreTestCleanup(ctx context.Context) error {
	fmt.Printf("🧹 Performing pre-test cleanup...\n")
	
	// Clean up data older than 24 hours to avoid interfering with concurrent tests
	if err := cm.CleanupOldTestData(ctx, 24*time.Hour); err != nil {
		return fmt.Errorf("pre-test cleanup failed: %w", err)
	}
	
	return nil
}

// PrintCleanupSummary prints a summary of what would be cleaned up (dry run)
func (cm *CleanupManager) PrintCleanupSummary(ctx context.Context, clusterNames []string) error {
	fmt.Printf("🔍 CLEANUP SUMMARY (DRY RUN)\n")
	fmt.Printf("═══════════════════════════\n")
	
	totalEstimated := 0
	for _, clusterName := range clusterNames {
		count, err := cm.countClusterRecords(ctx, clusterName)
		if err != nil {
			fmt.Printf("⚠️  Could not count records for cluster %s: %v\n", clusterName, err)
			continue
		}
		
		fmt.Printf("Cluster %s: %d records\n", clusterName, count)
		totalEstimated += count
	}
	
	fmt.Printf("═══════════════════════════\n")
	fmt.Printf("Total estimated cleanup: %d records\n", totalEstimated)
	return nil
}

// countClusterRecords counts total records for a cluster (for dry run)
func (cm *CleanupManager) countClusterRecords(ctx context.Context, clusterName string) (int, error) {
	total := 0
	
	// Count allocations
	query := "SELECT COUNT(*) FROM helix_allocation WHERE cluster = ?"
	var count int
	err := cm.sqlDb.QueryRowContext(ctx, query, clusterName).Scan(&count)
	if err == nil {
		total += count
	}
	
	// Count nodes
	query = "SELECT COUNT(*) FROM helix_nodes WHERE cluster_name = ?"
	err = cm.sqlDb.QueryRowContext(ctx, query, clusterName).Scan(&count)
	if err == nil {
		total += count
	}
	
	// Count cluster definitions
	query = "SELECT COUNT(*) FROM helix_cluster WHERE cluster = ?"
	err = cm.sqlDb.QueryRowContext(ctx, query, clusterName).Scan(&count)
	if err == nil {
		total += count
	}
	
	return total, nil
}