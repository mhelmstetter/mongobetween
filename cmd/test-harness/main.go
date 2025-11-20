package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	URI            string
	Database       string
	Collection     string
	Duration       time.Duration
	MinPoolSize    uint64
	MaxPoolSize    uint64
	InsertWorkers  int
	QueryWorkers   int
	SlowWorkers    int
	CursorWorkers  int
	BulkSize       int
	AppName        string
	ReportInterval time.Duration
	HoldTime       time.Duration
}

type Stats struct {
	inserts      int64
	queries      int64
	slowQueries  int64
	cursorOps    int64
	errors       int64
}

func main() {
	config := parseFlags()

	fmt.Println("=== Mongobetween Go Test Harness ===")
	fmt.Printf("URI: %s\n", config.URI)
	fmt.Printf("Database: %s\n", config.Database)
	fmt.Printf("Collection: %s\n", config.Collection)
	fmt.Printf("Duration: %s\n", config.Duration)
	fmt.Printf("Min Pool Size: %d\n", config.MinPoolSize)
	fmt.Printf("Max Pool Size: %d\n", config.MaxPoolSize)
	fmt.Printf("Insert Workers: %d\n", config.InsertWorkers)
	fmt.Printf("Query Workers: %d\n", config.QueryWorkers)
	fmt.Printf("Slow Query Workers: %d\n", config.SlowWorkers)
	fmt.Printf("Cursor Workers: %d\n", config.CursorWorkers)
	fmt.Printf("App Name: %s\n", config.AppName)
	fmt.Println("====================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Connect to MongoDB
	client, err := connect(ctx, config)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Printf("Error disconnecting: %v", err)
		}
	}()

	collection := client.Database(config.Database).Collection(config.Collection)

	// Setup collection with bulk data if needed
	if err := setupCollection(ctx, collection, config.BulkSize); err != nil {
		log.Fatalf("Failed to setup collection: %v", err)
	}

	// Create indexes for fast queries
	if err := createIndexes(ctx, collection); err != nil {
		log.Fatalf("Failed to create indexes: %v", err)
	}

	// Run workers
	stats := &Stats{}
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Start timer for duration
	go func() {
		select {
		case <-time.After(config.Duration):
			close(stopChan)
		case <-ctx.Done():
		}
	}()

	// Start progress reporter
	go reportProgress(ctx, stats, config.ReportInterval)

	// Start insert workers
	for i := 0; i < config.InsertWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			insertWorker(ctx, collection, id, stats, stopChan, config.HoldTime)
		}(i)
	}

	// Start fast query workers
	for i := 0; i < config.QueryWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			queryWorker(ctx, collection, id, stats, stopChan, config.HoldTime)
		}(i)
	}

	// Start slow query workers
	for i := 0; i < config.SlowWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			slowQueryWorker(ctx, collection, id, stats, stopChan, config.HoldTime)
		}(i)
	}

	// Start cursor workers
	for i := 0; i < config.CursorWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cursorWorker(ctx, collection, id, stats, stopChan, config.HoldTime)
		}(i)
	}

	wg.Wait()

	fmt.Println("\n=== Test Complete ===")
	fmt.Printf("Total Inserts: %d\n", atomic.LoadInt64(&stats.inserts))
	fmt.Printf("Total Queries: %d\n", atomic.LoadInt64(&stats.queries))
	fmt.Printf("Total Slow Queries: %d\n", atomic.LoadInt64(&stats.slowQueries))
	fmt.Printf("Total Cursor Ops: %d\n", atomic.LoadInt64(&stats.cursorOps))
	fmt.Printf("Total Errors: %d\n", atomic.LoadInt64(&stats.errors))
}

func parseFlags() *Config {
	config := &Config{}

	flag.StringVar(&config.URI, "uri", "mongodb://localhost:27016", "MongoDB URI")
	flag.StringVar(&config.Database, "db", "test_harness", "Database name")
	flag.StringVar(&config.Collection, "collection", "test_data", "Collection name")
	flag.DurationVar(&config.Duration, "duration", 60*time.Second, "Test duration")
	flag.Uint64Var(&config.MinPoolSize, "min-pool", 25, "Minimum pool size")
	flag.Uint64Var(&config.MaxPoolSize, "max-pool", 100, "Maximum pool size")
	flag.IntVar(&config.InsertWorkers, "insert-workers", 10, "Number of insert workers")
	flag.IntVar(&config.QueryWorkers, "query-workers", 20, "Number of fast query workers")
	flag.IntVar(&config.SlowWorkers, "slow-workers", 10, "Number of slow query workers")
	flag.IntVar(&config.CursorWorkers, "cursor-workers", 5, "Number of cursor workers")
	flag.IntVar(&config.BulkSize, "bulk-size", 50000, "Number of documents to insert on setup")
	flag.StringVar(&config.AppName, "app-name", "go-test-harness", "Application name for metrics")
	flag.DurationVar(&config.ReportInterval, "report-interval", 5*time.Second, "Progress report interval")
	flag.DurationVar(&config.HoldTime, "hold-time", 0, "Time to hold connection after each operation (simulates slow backend)")

	flag.Parse()
	return config
}

func connect(ctx context.Context, config *Config) (*mongo.Client, error) {
	clientOpts := options.Client().
		ApplyURI(config.URI).
		SetMinPoolSize(config.MinPoolSize).
		SetMaxPoolSize(config.MaxPoolSize).
		SetAppName(config.AppName)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}

	// Ping to verify connection
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, fmt.Errorf("ping failed: %w", err)
	}

	fmt.Println("Connected successfully")
	return client, nil
}

func setupCollection(ctx context.Context, collection *mongo.Collection, bulkSize int) error {
	count, err := collection.CountDocuments(ctx, bson.D{})
	if err != nil {
		return err
	}

	fmt.Printf("Collection has %d documents\n", count)

	if count < int64(bulkSize) {
		fmt.Printf("Inserting %d documents...\n", bulkSize-int(count))

		batchSize := 1000
		for i := int(count); i < bulkSize; i += batchSize {
			end := i + batchSize
			if end > bulkSize {
				end = bulkSize
			}

			docs := make([]interface{}, 0, end-i)
			for j := i; j < end; j++ {
				docs = append(docs, bson.D{
					{Key: "indexed_field", Value: rand.Intn(10000)},
					{Key: "unindexed_field", Value: rand.Intn(10000)},
					{Key: "data", Value: fmt.Sprintf("document-%d", j)},
					{Key: "timestamp", Value: time.Now()},
				})
			}

			if _, err := collection.InsertMany(ctx, docs); err != nil {
				return err
			}
		}
		fmt.Printf("Bulk insert complete\n")
	}

	return nil
}

func createIndexes(ctx context.Context, collection *mongo.Collection) error {
	_, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "indexed_field", Value: 1}},
	})
	return err
}

func reportProgress(ctx context.Context, stats *Stats, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("[Progress] Inserts: %d, Queries: %d, Slow: %d, Cursors: %d, Errors: %d\n",
				atomic.LoadInt64(&stats.inserts),
				atomic.LoadInt64(&stats.queries),
				atomic.LoadInt64(&stats.slowQueries),
				atomic.LoadInt64(&stats.cursorOps),
				atomic.LoadInt64(&stats.errors))
		}
	}
}

func insertWorker(ctx context.Context, collection *mongo.Collection, id int, stats *Stats, stop <-chan struct{}, holdTime time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		default:
			doc := bson.D{
				{Key: "indexed_field", Value: rand.Intn(10000)},
				{Key: "unindexed_field", Value: rand.Intn(10000)},
				{Key: "data", Value: fmt.Sprintf("insert-worker-%d-%d", id, time.Now().UnixNano())},
				{Key: "timestamp", Value: time.Now()},
			}

			if _, err := collection.InsertOne(ctx, doc); err != nil {
				if ctx.Err() == nil {
					atomic.AddInt64(&stats.errors, 1)
				}
				continue
			}
			if holdTime > 0 {
				time.Sleep(holdTime)
			}
			atomic.AddInt64(&stats.inserts, 1)
		}
	}
}

func queryWorker(ctx context.Context, collection *mongo.Collection, id int, stats *Stats, stop <-chan struct{}, holdTime time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		default:
			// Fast indexed query
			filter := bson.D{
				{Key: "indexed_field", Value: bson.D{
					{Key: "$gte", Value: rand.Intn(5000)},
					{Key: "$lte", Value: rand.Intn(5000) + 5000},
				}},
			}

			cursor, err := collection.Find(ctx, filter, options.Find().SetLimit(100))
			if err != nil {
				if ctx.Err() == nil {
					atomic.AddInt64(&stats.errors, 1)
				}
				continue
			}

			var results []bson.M
			if err := cursor.All(ctx, &results); err != nil {
				cursor.Close(ctx)
				if ctx.Err() == nil {
					atomic.AddInt64(&stats.errors, 1)
				}
				continue
			}
			if holdTime > 0 {
				time.Sleep(holdTime)
			}
			cursor.Close(ctx)
			atomic.AddInt64(&stats.queries, 1)
		}
	}
}

func slowQueryWorker(ctx context.Context, collection *mongo.Collection, id int, stats *Stats, stop <-chan struct{}, holdTime time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		default:
			// Slow unindexed query with aggregation
			pipeline := mongo.Pipeline{
				{{Key: "$match", Value: bson.D{
					{Key: "unindexed_field", Value: bson.D{
						{Key: "$gte", Value: rand.Intn(5000)},
						{Key: "$lte", Value: rand.Intn(5000) + 5000},
					}},
				}}},
				{{Key: "$group", Value: bson.D{
					{Key: "_id", Value: nil},
					{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
					{Key: "avgIndexed", Value: bson.D{{Key: "$avg", Value: "$indexed_field"}}},
				}}},
			}

			cursor, err := collection.Aggregate(ctx, pipeline)
			if err != nil {
				if ctx.Err() == nil {
					atomic.AddInt64(&stats.errors, 1)
				}
				continue
			}

			var results []bson.M
			if err := cursor.All(ctx, &results); err != nil {
				cursor.Close(ctx)
				if ctx.Err() == nil {
					atomic.AddInt64(&stats.errors, 1)
				}
				continue
			}
			if holdTime > 0 {
				time.Sleep(holdTime)
			}
			cursor.Close(ctx)
			atomic.AddInt64(&stats.slowQueries, 1)
		}
	}
}

func cursorWorker(ctx context.Context, collection *mongo.Collection, id int, stats *Stats, stop <-chan struct{}, holdTime time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		default:
			// Create cursor with small batch size to ensure multiple getMore calls
			filter := bson.D{
				{Key: "indexed_field", Value: bson.D{
					{Key: "$gte", Value: rand.Intn(5000)},
				}},
			}

			cursor, err := collection.Find(ctx, filter,
				options.Find().
					SetBatchSize(10).
					SetLimit(100))
			if err != nil {
				if ctx.Err() == nil {
					atomic.AddInt64(&stats.errors, 1)
				}
				continue
			}

			// Iterate through cursor to trigger getMore
			count := 0
			for cursor.Next(ctx) {
				count++
			}
			if holdTime > 0 {
				time.Sleep(holdTime)
			}
			cursor.Close(ctx)

			if cursor.Err() != nil && ctx.Err() == nil {
				atomic.AddInt64(&stats.errors, 1)
				continue
			}
			atomic.AddInt64(&stats.cursorOps, 1)
		}
	}
}
