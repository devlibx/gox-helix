package impl

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"

	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	helixLock "github.com/devlibx/gox-helix/pkg/common/lock/mysql"
	"github.com/devlibx/gox-helix/pkg/util"
	"github.com/google/uuid"

	_ "github.com/go-sql-driver/mysql"
)

// ClusterConfig defines the test cluster configuration
type ClusterConfig struct {
	Name            string
	DomainCount     int
	TasklistsPerDomain int
	MinPartitions   int
	MaxPartitions   int
	InitialNodes    int
}

// DatabaseConfig holds database connection information
type DatabaseConfig struct {
	Host     string
	User     string
	Password string
	Database string
	Port     int
}

// ClusterSetup manages database setup and cluster data creation
type ClusterSetup struct {
	dbConfig *DatabaseConfig
	sqlDb    *sql.DB
	queries  *helixClusterMysql.Queries
}

// NewClusterSetup creates a new cluster setup manager
func NewClusterSetup() (*ClusterSetup, error) {
	// Load environment variables
	if err := util.LoadDevEnv(); err != nil {
		return nil, fmt.Errorf("failed to load dev environment: %w", err)
	}

	// Create database configuration
	dbConfig := &DatabaseConfig{
		Host:     getEnvOrDefault("MYSQL_HOST", "localhost"),
		User:     getEnvOrDefault("MYSQL_USER", "root"),
		Password: getEnvOrDefault("MYSQL_PASSWORD", ""),
		Database: getEnvOrDefault("MYSQL_DB", "helix_test"),
		Port:     3306,
	}

	// Create database connection
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)
	
	sqlDb, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test connection
	if err := sqlDb.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create queries
	queries := helixClusterMysql.New(sqlDb)

	return &ClusterSetup{
		dbConfig: dbConfig,
		sqlDb:    sqlDb,
		queries:  queries,
	}, nil
}

// GetDatabaseConfig returns the database configuration
func (cs *ClusterSetup) GetDatabaseConfig() *helixLock.MySqlConfig {
	return &helixLock.MySqlConfig{
		Host:     cs.dbConfig.Host,
		User:     cs.dbConfig.User,
		Password: cs.dbConfig.Password,
		Database: cs.dbConfig.Database,
		Port:     cs.dbConfig.Port,
	}
}

// GetSQLDB returns the SQL database connection
func (cs *ClusterSetup) GetSQLDB() *sql.DB {
	return cs.sqlDb
}

// GetQueries returns the database queries interface
func (cs *ClusterSetup) GetQueries() *helixClusterMysql.Queries {
	return cs.queries
}

// CreateTestClusters creates test clusters with domains and tasklists
func (cs *ClusterSetup) CreateTestClusters(ctx context.Context, configs []ClusterConfig) error {
	fmt.Printf("🔧 Setting up test clusters...\n")
	
	for _, config := range configs {
		if err := cs.createSingleCluster(ctx, config); err != nil {
			return fmt.Errorf("failed to create cluster %s: %w", config.Name, err)
		}
		fmt.Printf("✅ Created cluster: %s (%d domains, %d tasklists each)\n", 
			config.Name, config.DomainCount, config.TasklistsPerDomain)
	}
	
	return nil
}

// createSingleCluster creates a single cluster with its domains and tasklists
func (cs *ClusterSetup) createSingleCluster(ctx context.Context, config ClusterConfig) error {
	for domainIdx := 0; domainIdx < config.DomainCount; domainIdx++ {
		domain := fmt.Sprintf("domain-%d-%s", domainIdx, uuid.NewString()[:8])
		
		for tasklistIdx := 0; tasklistIdx < config.TasklistsPerDomain; tasklistIdx++ {
			tasklist := fmt.Sprintf("tasklist-%d-%s", tasklistIdx, uuid.NewString()[:8])
			
			// Random partition count between min and max
			partitionCount := config.MinPartitions + rand.Intn(config.MaxPartitions-config.MinPartitions+1)
			
			// Insert cluster data
			err := cs.queries.UpsertCluster(ctx, helixClusterMysql.UpsertClusterParams{
				Cluster:        config.Name,
				Domain:         domain,
				Tasklist:       tasklist,
				PartitionCount: uint32(partitionCount),
				Metadata:       sql.NullString{Valid: true, String: "{}"},
			})
			if err != nil {
				return fmt.Errorf("failed to upsert cluster data for %s/%s/%s: %w", 
					config.Name, domain, tasklist, err)
			}
		}
	}
	
	return nil
}

// PrintClusterSummary prints a summary of created cluster data
func (cs *ClusterSetup) PrintClusterSummary(ctx context.Context, clusterNames []string) error {
	fmt.Printf("\n📊 Cluster Summary:\n")
	fmt.Printf("═══════════════════\n")
	
	totalClusters := 0
	totalTasklists := 0
	totalPartitions := 0
	
	for _, clusterName := range clusterNames {
		domainsAndTasks, err := cs.queries.GetAllDomainsAndTaskListsByClusterCname(ctx, clusterName)
		if err != nil {
			return fmt.Errorf("failed to get domains and tasks for cluster %s: %w", clusterName, err)
		}
		
		clusterPartitions := 0
		for _, dt := range domainsAndTasks {
			clusterPartitions += int(dt.PartitionCount)
		}
		
		fmt.Printf("Cluster: %s\n", clusterName)
		fmt.Printf("  - Tasklists: %d\n", len(domainsAndTasks))
		fmt.Printf("  - Total Partitions: %d\n", clusterPartitions)
		
		totalClusters++
		totalTasklists += len(domainsAndTasks)
		totalPartitions += clusterPartitions
	}
	
	fmt.Printf("\n📈 Totals:\n")
	fmt.Printf("  - Clusters: %d\n", totalClusters)
	fmt.Printf("  - Tasklists: %d\n", totalTasklists)
	fmt.Printf("  - Partitions: %d\n", totalPartitions)
	fmt.Printf("═══════════════════\n\n")
	
	return nil
}

// Close closes the database connection
func (cs *ClusterSetup) Close() error {
	if cs.sqlDb != nil {
		return cs.sqlDb.Close()
	}
	return nil
}

// getEnvOrDefault returns environment variable value or default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// GetDefaultClusterConfigs returns the default cluster configurations for the soak test
func GetDefaultClusterConfigs() []ClusterConfig {
	// Use UUID to ensure unique cluster names across test runs
	testRunId := uuid.NewString()[:8]
	
	return []ClusterConfig{
		{
			Name:               fmt.Sprintf("soak-test-cluster-1-%s", testRunId),
			DomainCount:        3,
			TasklistsPerDomain: 10,
			MinPartitions:      50,
			MaxPartitions:      100,
			InitialNodes:       40,
		},
		{
			Name:               fmt.Sprintf("soak-test-cluster-2-%s", testRunId),
			DomainCount:        3,
			TasklistsPerDomain: 10,
			MinPartitions:      50,
			MaxPartitions:      100,
			InitialNodes:       40,
		},
	}
}