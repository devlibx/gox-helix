package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/devlibx/gox-helix/examples/db_queue/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/oklog/ulid/v2"
)

const (
	// Number of concurrent goroutines to insert jobs.
	numProducers = 5
	// Number of jobs each producer will insert.
	jobsPerProducer = 20000000
)

// Configuration for the jobs to be created.
// In a real application, this would be read from a config file.
var jobConfig = map[string]map[string]int{
	"order_processing": {
		"new_orders":    8, // 8 partitions
		"cancellations": 4, // 4 partitions
	},
}

func main() {
	// IMPORTANT: Replace with your actual database connection string.
	// It is recommended to use environment variables or a config file for this.
	// Example DSN: "user:password@tcp(127.0.0.1:3306)/database"
	dsn := "root:credroot@tcp(127.0.0.1:3306)/automation?parseTime=true"

	// Connect to the database.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	log.Println("Database connection successful.")
	queries := database.New(db)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(numProducers)

	log.Printf("Starting %d producers to insert %d jobs each...\n", numProducers, jobsPerProducer)

	// Create a single, thread-safe source for ULIDs.
	var mu sync.Mutex
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)

	// Start the concurrent producers.
	for i := 0; i < numProducers; i++ {
		go func(producerID int) {
			defer wg.Done()

			for j := 0; j < jobsPerProducer; j++ {
				// Generate a new ULID for the job under a lock to ensure uniqueness across goroutines.
				mu.Lock()
				jobID := ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
				mu.Unlock()

				// Randomly select a domain, tasklist, and partition.
				domain := "order_processing"
				tasklist := "new_orders"
				numPartitions := jobConfig[domain][tasklist]
				if rand.Intn(2) == 0 {
					tasklist = "cancellations"
					numPartitions = jobConfig[domain][tasklist]
				}
				partitionID := rand.Intn(numPartitions)

				// Create a sample payload.
				payload, _ := json.Marshal(map[string]interface{}{
					"job_id":      jobID,
					"producer_id": producerID,
					"message":     fmt.Sprintf("This is job %d from producer %d", j, producerID),
				})

				// Create the job in the database.
				err := queries.CreateJob(ctx, database.CreateJobParams{
					ID:          jobID,
					Domain:      domain,
					Tasklist:    tasklist,
					PartitionID: uint32(partitionID),
					Status:      "created",
					Payload:     payload,
				})

				if err != nil {
					log.Printf("[Producer %d] failed to create job: %v\n", producerID, err)
				}

				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	// Wait for all producers to finish.
	wg.Wait()
	log.Printf("Finished inserting all jobs. Total jobs: %d", numProducers*jobsPerProducer)
}
