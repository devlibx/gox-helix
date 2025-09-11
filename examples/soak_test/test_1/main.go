package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	"github.com/devlibx/gox-helix/pkg/cluster/mgmt/allocation"
	coordinator "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"github.com/devlibx/gox-helix/pkg/common/database"
	"github.com/devlibx/gox-helix/pkg/common/lock"
	helixLock "github.com/devlibx/gox-helix/pkg/common/lock/mysql"
	"github.com/devlibx/gox-helix/pkg/util"
	"github.com/devlibx/gox-helix/examples/soak_test/test_1/impl"
	"go.uber.org/fx"

	_ "github.com/go-sql-driver/mysql"
)

// SoakTestApp represents the complete soak test application
type SoakTestApp struct {
	// Core components
	clusterSetup    *impl.ClusterSetup
	nodeManager     *impl.NodeManager
	chaosController *impl.ChaosController
	statusReporter  *impl.StatusReporter
	validator       *impl.Validator
	cleanupManager  *impl.CleanupManager
	
	// Configuration
	clusterConfigs []impl.ClusterConfig
	mockCf         *util.MockCrossFunction
	
	// Cluster management
	clusterManagers map[string]managment.ClusterManager
	coordinators    map[string]coordinator.Coordinator
	
	// Lifecycle
	cancelFunctions []context.CancelFunc
}

func main() {
	fmt.Printf("🚀 Starting Gox-Helix Soak Test\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	
	app, err := NewSoakTestApp()
	if err != nil {
		fmt.Printf("❌ Failed to create soak test app: %v\n", err)
		os.Exit(1)
	}
	defer app.Shutdown()
	
	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		fmt.Printf("\n🛑 Shutdown signal received, cleaning up...\n")
		cancel()
	}()
	
	// Run the soak test
	if err := app.RunSoakTest(ctx); err != nil {
		fmt.Printf("❌ Soak test failed: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("🎉 Soak test completed successfully!\n")
}

// NewSoakTestApp creates and initializes a new soak test application
func NewSoakTestApp() (*SoakTestApp, error) {
	// Initialize cluster setup
	clusterSetup, err := impl.NewClusterSetup()
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster setup: %w", err)
	}
	
	// Create mock cross function with 10x time acceleration
	mockCf := util.NewMockCrossFunction(time.Now())
	
	// Create other components
	nodeManager := impl.NewNodeManager(mockCf)
	statusReporter := impl.NewStatusReporter(clusterSetup.GetQueries(), nodeManager)
	validator := impl.NewValidator(clusterSetup.GetQueries(), nodeManager)
	cleanupManager := impl.NewCleanupManager(clusterSetup.GetSQLDB())
	
	// Get cluster configurations
	clusterConfigs := impl.GetDefaultClusterConfigs()
	
	// Create chaos controller
	chaosConfig := impl.GetDefaultChaosConfig()
	chaosController := impl.NewChaosController(chaosConfig, nodeManager, mockCf)
	
	app := &SoakTestApp{
		clusterSetup:    clusterSetup,
		nodeManager:     nodeManager,
		chaosController: chaosController,
		statusReporter:  statusReporter,
		validator:       validator,
		cleanupManager:  cleanupManager,
		clusterConfigs:  clusterConfigs,
		mockCf:          mockCf,
		clusterManagers: make(map[string]managment.ClusterManager),
		coordinators:    make(map[string]coordinator.Coordinator),
		cancelFunctions: make([]context.CancelFunc, 0),
	}
	
	return app, nil
}

// RunSoakTest executes the complete soak test sequence
func (app *SoakTestApp) RunSoakTest(ctx context.Context) error {
	fmt.Printf("🔧 Test Configuration:\n")
	fmt.Printf("   - Clusters: %d\n", len(app.clusterConfigs))
	fmt.Printf("   - Domains per cluster: %d\n", app.clusterConfigs[0].DomainCount)
	fmt.Printf("   - Tasklists per domain: %d\n", app.clusterConfigs[0].TasklistsPerDomain)
	fmt.Printf("   - Initial nodes per cluster: %d\n", app.clusterConfigs[0].InitialNodes)
	fmt.Printf("   - Chaos duration: 1 minute, Stabilization: 30 seconds\n")
	fmt.Printf("   - Time acceleration: 10x (1s = 100ms)\n")
	fmt.Printf("   - Coordinator TTL: 2s (fast coordinator churn)\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n\n")
	
	// Phase 1: Setup
	if err := app.setupPhase(ctx); err != nil {
		return fmt.Errorf("setup phase failed: %w", err)
	}
	
	// Phase 2: Chaos testing
	if err := app.chaosPhase(ctx); err != nil {
		return fmt.Errorf("chaos phase failed: %w", err)
	}
	
	// Phase 3: Stabilization
	if err := app.stabilizationPhase(ctx); err != nil {
		return fmt.Errorf("stabilization phase failed: %w", err)
	}
	
	// Phase 4: Validation
	if err := app.validationPhase(ctx); err != nil {
		return fmt.Errorf("validation phase failed: %w", err)
	}
	
	// Phase 5: Cleanup
	if err := app.cleanupPhase(ctx); err != nil {
		// Don't fail the test if cleanup fails
		fmt.Printf("⚠️  Warning: Cleanup phase had issues: %v\n", err)
	}
	
	return nil
}

// setupPhase performs initial setup of clusters, nodes, and coordinators
func (app *SoakTestApp) setupPhase(ctx context.Context) error {
	fmt.Printf("🏗️  SETUP PHASE\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	
	// Pre-test cleanup
	if err := app.cleanupManager.PerformPreTestCleanup(ctx); err != nil {
		return fmt.Errorf("pre-test cleanup failed: %w", err)
	}
	
	// Create test clusters
	clusterNames := make([]string, len(app.clusterConfigs))
	for i, config := range app.clusterConfigs {
		clusterNames[i] = config.Name
	}
	
	if err := app.clusterSetup.CreateTestClusters(ctx, app.clusterConfigs); err != nil {
		return fmt.Errorf("failed to create test clusters: %w", err)
	}
	
	// Print cluster summary
	if err := app.clusterSetup.PrintClusterSummary(ctx, clusterNames); err != nil {
		return fmt.Errorf("failed to print cluster summary: %w", err)
	}
	
	// Create cluster managers and coordinators
	if err := app.createClusterComponents(ctx); err != nil {
		return fmt.Errorf("failed to create cluster components: %w", err)
	}
	
	// Register initial nodes
	if err := app.nodeManager.RegisterInitialNodes(ctx, app.clusterConfigs[0].InitialNodes); err != nil {
		return fmt.Errorf("failed to register initial nodes: %w", err)
	}
	
	// Start coordinators
	if err := app.startCoordinators(ctx); err != nil {
		return fmt.Errorf("failed to start coordinators: %w", err)
	}
	
	fmt.Printf("✅ Setup phase completed successfully\n\n")
	return nil
}

// createClusterComponents creates cluster managers and coordinators for all clusters
func (app *SoakTestApp) createClusterComponents(ctx context.Context) error {
	fmt.Printf("🔧 Creating cluster managers and coordinators...\n")
	
	for _, config := range app.clusterConfigs {
		// Create cluster components using fx
		clusterManager, coordinator, err := app.createClusterComponentsWithFx(config.Name)
		if err != nil {
			return fmt.Errorf("failed to create components for cluster %s: %w", config.Name, err)
		}
		
		app.clusterManagers[config.Name] = clusterManager
		app.coordinators[config.Name] = coordinator
		
		// Register cluster with node manager
		app.nodeManager.RegisterCluster(config.Name, clusterManager)
		
		fmt.Printf("✅ Created components for cluster: %s\n", config.Name)
	}
	
	return nil
}

// createClusterComponentsWithFx creates cluster components using fx dependency injection
func (app *SoakTestApp) createClusterComponentsWithFx(clusterName string) (managment.ClusterManager, coordinator.Coordinator, error) {
	var clusterManager managment.ClusterManager
	var coord coordinator.Coordinator
	
	fxApp := fx.New(
		// Supply dependencies
		fx.Supply(app.clusterSetup.GetDatabaseConfig()),
		fx.Provide(func() gox.CrossFunction {
			return app.mockCf
		}),
		
		// Database connections
		fx.Provide(func(config *helixLock.MySqlConfig) (*sql.DB, error) {
			config.SetupDefault()
			connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
				config.User, config.Password, config.Host, config.Port, config.Database)
			return sql.Open("mysql", connectionString)
		}),
		fx.Provide(func(sqlDb *sql.DB) *helixClusterMysql.Queries {
			return helixClusterMysql.New(sqlDb)
		}),
		
		// Database connection holder for V3 algorithm
		fx.Provide(func(sqlDb *sql.DB) database.ConnectionHolder {
			return database.NewConnectionHolder(sqlDb)
		}),
		
		// Locker service
		fx.Provide(func(cf gox.CrossFunction, config *helixLock.MySqlConfig) (lock.Locker, error) {
			return helixLock.NewHelixLockMySQLService(cf, config)
		}),
		
		// Cluster manager with fast coordinator TTL for chaos
		fx.Provide(func(
			cf gox.CrossFunction,
			db *helixClusterMysql.Queries,
			locker lock.Locker,
		) (managment.ClusterManager, error) {
			config := &managment.ClusterManagerConfig{
				Name:                  clusterName,
				NodeHeartbeatInterval: 3 * time.Second,
				ControllerTtl:         2 * time.Second, // Short TTL for coordinator churn!
			}
			return managment.NewClusterManager(cf, config, db, locker)
		}),
		
		// Allocation manager (using V3 Algorithm - stable cluster with cross-node placeholders)
		fx.Provide(func(
			cf gox.CrossFunction,
			db *helixClusterMysql.Queries,
			connHolder database.ConnectionHolder,
		) (managment.AllocationManager, error) {
			algorithmConfig := &managment.AlgorithmConfig{
				TimeToWaitForPartitionReleaseBeforeForceRelease: 10 * time.Second,
			}
			return allocation.NewAllocationAlgorithmV1(cf, db, db, algorithmConfig, connHolder)
		}),
		
		// Coordinator
		fx.Provide(func(
			cf gox.CrossFunction,
			clusterManager managment.ClusterManager,
			allocationManager managment.AllocationManager,
			db *helixClusterMysql.Queries,
			locker lock.Locker,
		) (coordinator.Coordinator, error) {
			return coordinator.NewCoordinator(cf, clusterManager, allocationManager, db, locker)
		}),
		
		// Populate the components we need
		fx.Populate(&clusterManager, &coord),
		fx.NopLogger, // Disable fx logging
	)
	
	// Start the fx app
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := fxApp.Start(startCtx); err != nil {
		return nil, nil, fmt.Errorf("failed to start fx app: %w", err)
	}
	
	// Store cancel function for cleanup
	app.cancelFunctions = append(app.cancelFunctions, func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		fxApp.Stop(stopCtx)
	})
	
	return clusterManager, coord, nil
}

// startCoordinators starts all coordinators in background goroutines
func (app *SoakTestApp) startCoordinators(ctx context.Context) error {
	fmt.Printf("🚀 Starting coordinators for all clusters...\n")
	
	for clusterName, coord := range app.coordinators {
		// Start coordinator in background
		coordinatorCtx, cancel := context.WithCancel(ctx)
		app.cancelFunctions = append(app.cancelFunctions, cancel)
		
		go func(name string, c coordinator.Coordinator) {
			if err := c.Start(coordinatorCtx); err != nil && coordinatorCtx.Err() == nil {
				fmt.Printf("⚠️  Warning: Coordinator for cluster %s stopped with error: %v\n", name, err)
			}
		}(clusterName, coord)
		
		fmt.Printf("✅ Started coordinator for cluster: %s\n", clusterName)
	}
	
	// Give coordinators time to initialize
	time.Sleep(2 * time.Second)
	return nil
}

// chaosPhase runs the intensive chaos testing with status reporting
func (app *SoakTestApp) chaosPhase(ctx context.Context) error {
	fmt.Printf("🔥 CHAOS PHASE\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	
	clusterNames := make([]string, len(app.clusterConfigs))
	for i, config := range app.clusterConfigs {
		clusterNames[i] = config.Name
	}
	
	// Start status reporting in background with separate context
	statusCtx, statusCancel := context.WithCancel(context.Background())
	app.cancelFunctions = append(app.cancelFunctions, statusCancel)
	
	go app.statusReporter.StartPeriodicReporting(statusCtx, 10*time.Second, clusterNames)
	
	// Run chaos phase
	if err := app.chaosController.StartChaos(ctx, app.statusReporter); err != nil {
		return fmt.Errorf("chaos phase failed: %w", err)
	}
	
	fmt.Printf("✅ Chaos phase completed\n\n")
	return nil
}

// stabilizationPhase allows clusters to stabilize after chaos
func (app *SoakTestApp) stabilizationPhase(ctx context.Context) error {
	fmt.Printf("🔄 STABILIZATION PHASE\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	
	if err := app.chaosController.StabilizationPhase(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("stabilization phase failed: %w", err)
	}
	
	fmt.Printf("✅ Stabilization phase completed\n\n")
	return nil
}

// validationPhase validates that all partitions are assigned correctly
func (app *SoakTestApp) validationPhase(ctx context.Context) error {
	fmt.Printf("🔍 VALIDATION PHASE\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	
	clusterNames := make([]string, len(app.clusterConfigs))
	for i, config := range app.clusterConfigs {
		clusterNames[i] = config.Name
	}
	
	// Perform comprehensive validation
	validationResult, err := app.validator.ValidatePartitionAllocation(ctx, clusterNames)
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	
	// Generate final report
	if err := app.statusReporter.GenerateFinalReport(ctx, clusterNames); err != nil {
		fmt.Printf("⚠️  Warning: Failed to generate final report: %v\n", err)
	}
	
	if !validationResult.Success {
		return fmt.Errorf("VALIDATION FAILED: %d partitions unassigned, %d invalid assignments", 
			validationResult.UnassignedPartitions, validationResult.InvalidAssignments)
	}
	
	fmt.Printf("✅ Validation phase completed successfully\n\n")
	return nil
}

// cleanupPhase performs cleanup of test data
func (app *SoakTestApp) cleanupPhase(ctx context.Context) error {
	fmt.Printf("🧹 CLEANUP PHASE\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	
	clusterNames := make([]string, len(app.clusterConfigs))
	for i, config := range app.clusterConfigs {
		clusterNames[i] = config.Name
	}
	
	if err := app.cleanupManager.CleanupTestData(ctx, clusterNames); err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}
	
	fmt.Printf("✅ Cleanup phase completed\n\n")
	return nil
}

// Shutdown gracefully shuts down the soak test application
func (app *SoakTestApp) Shutdown() {
	fmt.Printf("🛑 Shutting down soak test application...\n")
	
	// Stop node manager
	if app.nodeManager != nil {
		app.nodeManager.Shutdown()
	}
	
	// Cancel all contexts
	for i, cancel := range app.cancelFunctions {
		fmt.Printf("🔌 Stopping component %d/%d...\n", i+1, len(app.cancelFunctions))
		cancel()
	}
	
	// Close cluster setup
	if app.clusterSetup != nil {
		if err := app.clusterSetup.Close(); err != nil {
			fmt.Printf("⚠️  Warning: Failed to close cluster setup: %v\n", err)
		}
	}
	
	fmt.Printf("✅ Shutdown completed\n")
}