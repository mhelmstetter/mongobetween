package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

type metricValue struct {
	value     float64
	count     int64
	lastSeen  time.Time
	metricType string
}

func main() {
	addr := flag.String("addr", ":8125", "Address to listen on")
	aggregate := flag.Bool("aggregate", false, "Aggregate metrics and print summary every 10s")
	flag.Parse()

	conn, err := net.ListenPacket("udp", *addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *addr, err)
	}
	defer conn.Close()

	log.Printf("Listening for statsd metrics on %s", *addr)

	if *aggregate {
		runAggregated(conn)
	} else {
		runPassthrough(conn)
	}
}

func runPassthrough(conn net.PacketConn) {
	buf := make([]byte, 65535)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			log.Printf("Read error: %v", err)
			continue
		}

		metrics := strings.Split(string(buf[:n]), "\n")
		for _, metric := range metrics {
			metric = strings.TrimSpace(metric)
			if metric != "" {
				fmt.Println(metric)
			}
		}
	}
}

func runAggregated(conn net.PacketConn) {
	metrics := make(map[string]*metricValue)
	var mu sync.Mutex

	// Print summary every 10 seconds
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			mu.Lock()
			printSummary(metrics)
			// Clear counters but keep gauges
			for _, mv := range metrics {
				if mv.metricType == "c" {
					mv.count = 0
				}
			}
			mu.Unlock()
		}
	}()

	buf := make([]byte, 65535)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			log.Printf("Read error: %v", err)
			continue
		}

		lines := strings.Split(string(buf[:n]), "\n")
		mu.Lock()
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parseAndAggregate(line, metrics)
		}
		mu.Unlock()
	}
}

func parseAndAggregate(line string, metrics map[string]*metricValue) {
	// Format: metric.name:value|type|@sample_rate|#tag1:val1,tag2:val2
	// Examples:
	//   mongobetween.pool_event.connection_created:1|c
	//   mongobetween.pool.open_connections:5|g
	//   mongobetween.request_time:125|ms

	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return
	}

	name := parts[0]
	rest := parts[1]

	// Extract tags if present and append to name
	if idx := strings.Index(rest, "|#"); idx != -1 {
		tags := rest[idx+2:]
		rest = rest[:idx]
		name = fmt.Sprintf("%s [%s]", name, tags)
	}

	valueParts := strings.Split(rest, "|")
	if len(valueParts) < 2 {
		return
	}

	var value float64
	fmt.Sscanf(valueParts[0], "%f", &value)
	metricType := valueParts[1]

	mv, exists := metrics[name]
	if !exists {
		mv = &metricValue{metricType: metricType}
		metrics[name] = mv
	}

	mv.lastSeen = time.Now()

	switch metricType {
	case "c": // counter
		mv.count += int64(value)
	case "g": // gauge
		mv.value = value
	case "ms", "h": // timing/histogram
		// For timing, we'll just track the latest and count
		mv.value = value
		mv.count++
	}
}

func printSummary(metrics map[string]*metricValue) {
	if len(metrics) == 0 {
		log.Println("--- No metrics received ---")
		return
	}

	fmt.Println("\n========== METRICS SUMMARY ==========")
	fmt.Printf("Time: %s\n\n", time.Now().Format(time.RFC3339))

	// Sort metric names
	names := make([]string, 0, len(metrics))
	for name := range metrics {
		names = append(names, name)
	}
	sort.Strings(names)

	// Group by type
	counters := []string{}
	gauges := []string{}
	timings := []string{}

	for _, name := range names {
		mv := metrics[name]
		switch mv.metricType {
		case "c":
			counters = append(counters, name)
		case "g":
			gauges = append(gauges, name)
		case "ms", "h":
			timings = append(timings, name)
		}
	}

	if len(counters) > 0 {
		fmt.Println("COUNTERS:")
		for _, name := range counters {
			mv := metrics[name]
			fmt.Printf("  %-60s %d\n", name, mv.count)
		}
		fmt.Println()
	}

	if len(gauges) > 0 {
		fmt.Println("GAUGES:")
		for _, name := range gauges {
			mv := metrics[name]
			fmt.Printf("  %-60s %.2f\n", name, mv.value)
		}
		fmt.Println()
	}

	if len(timings) > 0 {
		fmt.Println("TIMINGS (last value, count):")
		for _, name := range timings {
			mv := metrics[name]
			fmt.Printf("  %-60s %.2fms (%d samples)\n", name, mv.value, mv.count)
		}
		fmt.Println()
	}

	fmt.Println("======================================")
}
