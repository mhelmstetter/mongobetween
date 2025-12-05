package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	uri := flag.String("uri", "mongodb://localhost:27017", "MongoDB connection URI")
	numWorkers := flag.Int("n", 1, "Number of top-level workers (each creates a MongoClient)")
	numSubWorkers := flag.Int("m", 1, "Number of sub-workers per top-level worker")
	flag.Parse()

	log.Printf("Starting load generator with %d workers, %d sub-workers each", *numWorkers, *numSubWorkers)
	log.Printf("Connecting to: %s", *uri)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	var wg sync.WaitGroup

	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, workerID, *uri, *numSubWorkers)
		}(i)
	}

	wg.Wait()
	log.Println("All workers stopped")
}

func runWorker(ctx context.Context, workerID int, uri string, numSubWorkers int) {
	log.Printf("[Worker %d] Creating MongoClient", workerID)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Printf("[Worker %d] Failed to connect: %v", workerID, err)
		return
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Printf("[Worker %d] Failed to disconnect: %v", workerID, err)
		}
	}()

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		log.Printf("[Worker %d] Failed to ping: %v", workerID, err)
		return
	}
	log.Printf("[Worker %d] Connected successfully", workerID)

	// Check if sharded cluster and setup sharding if needed (only first worker does this)
	if workerID == 0 {
		if err := setupShardingIfNeeded(ctx, client); err != nil {
			log.Printf("[Worker %d] Failed to setup sharding: %v", workerID, err)
			// Continue anyway, collection might already be sharded or it's not a sharded cluster
		}
	}

	collection := client.Database("test").Collection("test")

	var wg sync.WaitGroup
	for i := 0; i < numSubWorkers; i++ {
		wg.Add(1)
		go func(subWorkerID int) {
			defer wg.Done()
			runSubWorker(ctx, workerID, subWorkerID, collection)
		}(i)
	}

	wg.Wait()
	log.Printf("[Worker %d] All sub-workers stopped", workerID)
}

func setupShardingIfNeeded(ctx context.Context, client *mongo.Client) error {
	// Check if this is a sharded cluster using isdbgrid command
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "isdbgrid", Value: 1}}).Decode(&result)
	if err != nil {
		// Not a sharded cluster or command failed
		log.Println("Not a sharded cluster (isdbgrid check failed)")
		return nil
	}

	log.Println("Detected sharded cluster, setting up sharding for test.test")

	// Enable sharding on the test database
	err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "enableSharding", Value: "test"}}).Decode(&result)
	if err != nil {
		log.Printf("enableSharding failed (may already be enabled): %v", err)
		// Continue anyway
	}

	// Shard the collection on {x: 1}
	err = client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "shardCollection", Value: "test.test"},
		{Key: "key", Value: bson.D{{Key: "x", Value: 1}}},
	}).Decode(&result)
	if err != nil {
		log.Printf("shardCollection failed (may already be sharded): %v", err)
		// Continue anyway
	} else {
		log.Println("Successfully sharded test.test on {x: 1}")
	}

	return nil
}

func runSubWorker(ctx context.Context, workerID, subWorkerID int, collection *mongo.Collection) {
	log.Printf("[Worker %d, Sub %d] Starting infinite loop", workerID, subWorkerID)

	insertCount := 0
	findCount := 0

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker %d, Sub %d] Stopping (inserts: %d, finds: %d)", workerID, subWorkerID, insertCount, findCount)
			return
		case <-ticker.C:
			log.Printf("[Worker %d, Sub %d] Stats - inserts: %d, finds: %d", workerID, subWorkerID, insertCount, findCount)
		default:
			// Generate random int with high entropy for insert
			insertVal, err := cryptoRandInt64()
			if err != nil {
				log.Printf("[Worker %d, Sub %d] Failed to generate random int: %v", workerID, subWorkerID, err)
				continue
			}

			// InsertOne
			_, err = collection.InsertOne(ctx, bson.D{
				{Key: "x", Value: insertVal},
				{Key: "data", Value: fmt.Sprintf("worker-%d-sub-%d", workerID, subWorkerID)},
				{Key: "ts", Value: time.Now()},
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[Worker %d, Sub %d] InsertOne failed: %v", workerID, subWorkerID, err)
				continue
			}
			insertCount++

			// Generate another random int for find
			findVal, err := cryptoRandInt64()
			if err != nil {
				log.Printf("[Worker %d, Sub %d] Failed to generate random int: %v", workerID, subWorkerID, err)
				continue
			}

			// FindOne (may or may not find a document)
			var found bson.M
			err = collection.FindOne(ctx, bson.D{{Key: "x", Value: findVal}}).Decode(&found)
			if err != nil && err != mongo.ErrNoDocuments {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[Worker %d, Sub %d] FindOne failed: %v", workerID, subWorkerID, err)
				continue
			}
			findCount++
		}
	}
}

// cryptoRandInt64 generates a random int64 using crypto/rand for high entropy
func cryptoRandInt64() (int64, error) {
	max := big.NewInt(1<<63 - 1)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return 0, err
	}
	return n.Int64(), nil
}
