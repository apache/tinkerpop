/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/apache/tinkerpop/gremlin-go/v3/driver"
)

type ResultSet = gremlingo.ResultSet
type GraphTraversalSource = gremlingo.GraphTraversalSource
type GraphTraversal = gremlingo.GraphTraversal
type DriverRemoteConnection = gremlingo.DriverRemoteConnection
type DriverRemoteConnectionSettings = gremlingo.DriverRemoteConnectionSettings

type performanceResults struct {
	executeDuration   time.Duration
	executeThroughput int
	executeAllocs     uint64
	executeBytes      uint64
}

type performanceStats struct {
	executeDurationArr   []int64
	executeThroughputArr []int
	executeAllocsArr     []uint64
	executeBytesArr      []uint64
}

// test suite constants
const suiteRunCount = 55

var poolSize = []int{2, 4, 8}
var poolQueryCount = []int{250}

// number of projections to generate each traversal with
const poolingTraversal = 10
const smokeTraversal = 10
const fullTraversal = 500

const retrieveOne = "retrieve one"
const retrieveAll = "retrieve all"
const connectionPooling = "connection pooling"

// placeholder variable to assign result value to
var retrievedRes interface{}
var memStats runtime.MemStats

var smokeTests = []string{"smokeTraversal"}
var fullTests = append(smokeTests, "fullTraversal")

var traversalMap = map[string]func(g *GraphTraversalSource) *GraphTraversal{
	// simple queries as smoke test
	"smokeTraversal": func(g *gremlingo.GraphTraversalSource) *gremlingo.GraphTraversal {
		return generateProjectTraversal(g, smokeTraversal)
	},

	// connection pooling test
	"poolingTraversal": func(g *gremlingo.GraphTraversalSource) *gremlingo.GraphTraversal {
		return generateProjectTraversal(g, poolingTraversal)
	},

	// complex queries for full test
	"fullTraversal": func(g *gremlingo.GraphTraversalSource) *gremlingo.GraphTraversal {
		return generateProjectTraversal(g, fullTraversal)
	},
}

func generateProjectTraversal(g *GraphTraversalSource, repeat int) *GraphTraversal {
	var traversal *GraphTraversal
	var args []interface{}
	for i := 0; i < repeat; i++ {
		args = append(args, strconv.Itoa(i))
	}
	traversal = g.V().Project(args...)
	for i := 0; i < repeat; i++ {
		traversal = traversal.By(gremlingo.T__.ValueMap(true))
	}
	return traversal
}

func executeConnectionPooling(testName string, queryCount int, g *GraphTraversalSource) (*performanceResults, error) {
	var allResultSets []*ResultSet
	runtime.GC()
	runtime.ReadMemStats(&memStats)
	startAlloc := memStats.Mallocs
	startBytes := memStats.TotalAlloc
	// run all at the same time
	startTime := time.Now()
	// execute the traversal
	for i := 0; i < queryCount; i++ {
		results, err := getTraversal(testName, g).GetResultSet()
		if err != nil {
			log.Println(err)
			return nil, err
		}
		allResultSets = append(allResultSets, &results)
	}
	for i := 0; i < len(allResultSets); i++ {
		results, err := (*allResultSets[i]).All()
		if err != nil {
			log.Println(err)
			return nil, err
		}
		if results == nil {
			log.Println("result list should not be nil")
			return nil, err
		}
	}
	duration := time.Now().Sub(startTime)
	runtime.ReadMemStats(&memStats)
	return &performanceResults{
		executeDuration:   duration,
		executeThroughput: int(math.Round(float64(queryCount) / duration.Seconds())),
		executeAllocs:     memStats.Mallocs - startAlloc,
		executeBytes:      memStats.TotalAlloc - startBytes,
	}, nil
}

func executeAndRetrieveOne(t *GraphTraversal) (*performanceResults, error) {
	runtime.GC()
	runtime.ReadMemStats(&memStats)
	startAlloc := memStats.Mallocs
	startBytes := memStats.TotalAlloc
	startTime := time.Now()
	// execute the traversal
	result, err := t.Next()
	if err != nil {
		log.Println(err)
		return nil, err
	} else {
		retrievedRes = result.GetInterface()
	}
	duration := time.Now().Sub(startTime)
	runtime.ReadMemStats(&memStats)
	return &performanceResults{
		executeDuration: duration,
		executeAllocs:   memStats.Mallocs - startAlloc,
		executeBytes:    memStats.TotalAlloc - startBytes,
	}, err
}

func executeAndRetrieveAll(t *GraphTraversal) (*performanceResults, error) {
	runtime.GC()
	runtime.ReadMemStats(&memStats)
	execAlloc := memStats.Mallocs
	execBytes := memStats.TotalAlloc
	startExecution := time.Now()
	// execute the traversal
	results, err := t.ToList()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	for _, result := range results {
		retrievedRes = result.GetInterface()
	}
	endExecution := time.Now()
	runtime.ReadMemStats(&memStats)
	return &performanceResults{
		executeDuration: endExecution.Sub(startExecution),
		executeAllocs:   memStats.Mallocs - execAlloc,
		executeBytes:    memStats.TotalAlloc - execBytes,
	}, nil
}

func run(g *GraphTraversalSource, runs int, runLevel string, memProfile bool) {
	if runs < 6 {
		log.Panic("each test must be run greater than 6 times")
		return
	}
	switch runLevel {
	case "smoke":
		fmt.Println("Running smoke test")
		for _, name := range smokeTests {
			fmt.Println("Running test:", name)
			runPerformance(name, g, runs, memProfile)
			fmt.Printf("Test %s completed with %d runs.\n", name, runs)
		}
	case "full":
		fmt.Println("Running full test")
		for _, name := range fullTests {
			fmt.Println("Running test:", name)
			runPerformance(name, g, runs, memProfile)
			fmt.Printf("Test %s completed with %d runs.\n", name, runs)
		}
	}
}

func runPerformance(testName string, g *GraphTraversalSource, runs int, memProf bool) {
	performStats := &performanceStats{
		executeDurationArr: make([]int64, runs),
		executeAllocsArr:   make([]uint64, runs),
		executeBytesArr:    make([]uint64, runs),
	}
	warmUp, err := g.V().Count().Next()
	if err != nil {
		log.Panic("Error during warm up:", err)
		return
	} else {
		count, _ := warmUp.GetInt64()
		fmt.Printf("Warming up. Vertex count of current graph: %d \n", count)
	}
	for i := 0; i < runs; i++ {
		var q *performanceResults
		var traversal = getTraversal(testName, g)
		q, err = executeAndRetrieveOne(traversal)
		if err != nil {
			return
		}
		performStats.executeDurationArr[i] = q.executeDuration.Milliseconds()
		performStats.executeAllocsArr[i] = q.executeAllocs
		performStats.executeBytesArr[i] = q.executeBytes
	}
	handleMetrics(performStats, retrieveOne, runs, memProf)
	for i := 0; i < runs; i++ {
		var q *performanceResults
		var traversal = getTraversal(testName, g)
		q, err = executeAndRetrieveAll(traversal)
		if err != nil {
			return
		}
		performStats.executeDurationArr[i] = q.executeDuration.Milliseconds()
		performStats.executeAllocsArr[i] = q.executeAllocs
		performStats.executeBytesArr[i] = q.executeBytes
	}
	handleMetrics(performStats, retrieveAll, runs, memProf)
}

func runPerformancePooling(testName string, g *GraphTraversalSource, testRun, queryRun int, memProf bool) {
	performStats := &performanceStats{
		executeDurationArr:   make([]int64, testRun),
		executeThroughputArr: make([]int, testRun),
		executeAllocsArr:     make([]uint64, testRun),
		executeBytesArr:      make([]uint64, testRun),
	}
	warmUp, err := g.V().Count().Next()
	if err != nil {
		log.Panic("Error during warm up:", err)
		return
	} else {
		count, _ := warmUp.GetInt64()
		fmt.Printf("Warming up. Vertex count of current graph: %d \n", count)
	}
	for i := 0; i < testRun; i++ {
		var q *performanceResults
		q, err = executeConnectionPooling(testName, queryRun, g)
		if err != nil {
			return
		}
		performStats.executeDurationArr[i] = q.executeDuration.Milliseconds()
		performStats.executeThroughputArr[i] = q.executeThroughput
		performStats.executeAllocsArr[i] = q.executeAllocs
		performStats.executeBytesArr[i] = q.executeBytes
	}
	handleMetrics(performStats, connectionPooling, testRun, memProf)
}

func handleMetrics(stats *performanceStats, testType string, runs int, memProf bool) {
	fmt.Println("================================================")
	fmt.Println("Results for", testType)
	sort.Slice(stats.executeDurationArr, func(i, j int) bool { return stats.executeDurationArr[i] < stats.executeDurationArr[j] })
	// removing the slowest run as outlier
	executeDurationArr := stats.executeDurationArr[:runs-5]
	var totalTime int64
	for i := range executeDurationArr {
		totalTime += executeDurationArr[i]
	}
	printDuration(executeDurationArr, totalTime, runs-5)
	if testType == connectionPooling {
		sort.Ints(stats.executeThroughputArr)
		// removing the lowest throughput (aka slowest) as outlier
		executeThroughputArr := stats.executeThroughputArr[5:]
		var totalThroughput int
		for i := range executeThroughputArr {
			totalThroughput += executeThroughputArr[i]
		}
		printThroughput(executeThroughputArr, totalThroughput, runs-5)
	}

	if memProf {
		sort.Slice(stats.executeAllocsArr, func(i, j int) bool { return stats.executeAllocsArr[i] < stats.executeAllocsArr[j] })
		executeAllocsArr := stats.executeAllocsArr[:runs-5]
		var totalAlloc uint64
		for i := range executeAllocsArr {
			totalAlloc += executeAllocsArr[i]
		}
		printAllocs(executeAllocsArr, totalAlloc, runs-5)

		sort.Slice(stats.executeBytesArr, func(i, j int) bool { return stats.executeBytesArr[i] < stats.executeBytesArr[j] })
		executeBytesArr := stats.executeBytesArr[:runs-5]
		var totalBytes uint64
		for i := range executeBytesArr {
			totalBytes += executeBytesArr[i]
		}
		printBytes(executeBytesArr, totalBytes, runs-5)
	}
}

func printDuration(stats []int64, total int64, runs int) {
	fmt.Printf("\tEXECUTION STATS:\taverage %dms \tmedian %dms \tp90 %dms \tp95 %dms \tmax %dms \tmin %dms\n",
		int64(math.Round(float64(total)/float64(runs))), stats[int(math.Ceil(float64(runs/2)))],
		stats[int(math.Ceil(float64(runs*90/100)))-1], stats[int(math.Ceil(float64(runs*95/100)))-1], stats[runs-1], stats[0])
}

func printThroughput(stats []int, total int, runs int) {
	fmt.Printf("\tTHROUGHPUT STATS:\taverage %d query/s\tmedian %d query/s\tp10 %d query/s\tp5 %d query/s\tmax %d query/s\tmin %d query/s\n",
		int(math.Round(float64(total)/float64(runs))), stats[int(math.Ceil(float64(runs/2)))],
		stats[int(math.Ceil(float64(runs*10/100)))-1], stats[int(math.Ceil(float64(runs*5/100)))-1], stats[runs-1], stats[0])
}

func printAllocs(stats []uint64, total uint64, runs int) {
	fmt.Printf("\tALLOCATION STATS:\taverage %d allocs\tmedian %d allocs\tp90 %d allocs\tp95 %d allocs\tmax %d allocs\tmin %d allocs\n",
		total/uint64(runs), stats[runs/2],
		stats[int(math.Ceil(float64(runs*90/100)))-1], stats[int(math.Ceil(float64(runs*95/100)))-1], stats[runs-1], stats[0])
}

func printBytes(stats []uint64, total uint64, runs int) {
	fmt.Printf("\tMEMORY USE STATS:\taverage %d B\tmedian %d B\tp90 %d B\tp95 %d B\tmax %d B\tmin %d B\n",
		total/uint64(runs), stats[runs/2],
		stats[int(math.Ceil(float64(runs*90/100)))-1], stats[int(math.Ceil(float64(runs*95/100)))-1], stats[runs-1], stats[0])
}

func getTraversal(testName string, g *GraphTraversalSource) *GraphTraversal {
	if traversalFcn, ok := traversalMap[testName]; ok {
		return traversalFcn(g)
	} else {
		return nil
	}
}

// connection setting constants

const Host = "localhost"
const Port = 45940
const GremlinWarning = gremlingo.Warning
const gratefulGraphAlias = "ggrateful"
const threshold = 4 // same as default
const bufferSize = 314572800
const poolingBufferSize = 26214400

//
// createConnection: Creates a connection to a remote endpoint and returns a
// GraphTraversalSource that can be used to submit Gremlin queries.
//
func createConnection(host string, port, poolSize, buffersSize int) (*GraphTraversalSource, *DriverRemoteConnection, error) {
	var g *GraphTraversalSource
	var drc *DriverRemoteConnection
	var err error

	endpoint := fmt.Sprintf("ws://%s:%d/gremlin", host, port)
	log.Println("Attempting to connect to : " + endpoint)

	// Establish a new connection and catch any errors that may occur
	drc, err = gremlingo.NewDriverRemoteConnection(endpoint, func(settings *DriverRemoteConnectionSettings) {
		settings.LogVerbosity = GremlinWarning
		settings.TraversalSource = gratefulGraphAlias
		settings.NewConnectionThreshold = threshold
		settings.MaximumConcurrentConnections = poolSize
		settings.WriteBufferSize = buffersSize
		settings.ReadBufferSize = buffersSize
	})

	if err != nil {
		log.Fatalln(err)
	} else {
		g = gremlingo.Traversal_().With(drc)
	}
	return g, drc, err
}

const (
	usage = `Usage:
Run Gremlin-Go Performance Tests.

The performance test is expected to run on a Gremlin Server loaded with the grateful graph. 

A projection query using ValueMap is executed for each test, g.V().Project(1).By(gremlingo.T__.ValueMap(true)), with 
the number of projection scaled up depending on test suite run level. Smoke tests runs the projection query with 10 
projections, and full test runs the smoke test plus the query with 500 projections. Connection pooling test runs the 
query with 10 projections, executed 250 times per run with a default set of connection pool sizes (2, 4, 8). 

Each test is run 55 times by default, with the slowest 5 runs removed, and execution time is taken for retrieving a 
single result with Next(), and retrieving all results with ToList(). 

Metrics for single query execution include average, median, P90, P50, max, and min. One can optionally enable memory 
profile metrics, which outputs the allocation and byte usage data. Connection pooling test includes throughput metrics, 
which are average, median, P10, P5, max, and min query/s.

Options:
`
)

//
// main: Program entry point. Create the connection, run performance tests, shutdown.
//
func main() {
	hostPtr := flag.String("host", Host, "Server host, the default is localhost.")
	portPtr := flag.Int("port", Port, "Server port, the default is 45940.")
	runCount := flag.Int("runCount", suiteRunCount, "The number of times to run each test, the default is 55, minimum is 6.")
	memProfile := flag.Bool("memProfile", false, "Enables memory profiling.")
	runLevel := flag.String("runLevel", "smoke", "The test suite to run: smoke, full, or pooling, the default is smoke")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), usage)
		flag.PrintDefaults()
	}

	flag.Parse()
	fmt.Println("===== RUNNING PERFORMANCE TEST SUITE ON THE GRATEFUL GRAPH =====")
	if *runLevel == "pooling" {
		for _, size := range poolSize {

			// Create a graph traversal source object, after which we are ready to submit queries.
			g, drc, err := createConnection(*hostPtr, *portPtr, size, poolingBufferSize)

			if err != nil {
				log.Println("Error creating the connection - program terminating", err)
				return
			}

			fmt.Printf("===== Running connection pooling test with %d pools =====\n", size)
			for _, queryCount := range poolQueryCount {
				runPerformancePooling("poolingTraversal", g, *runCount, queryCount, *memProfile)
				fmt.Printf("Connection pooling test completed running %d queries in %d pools with %d run.\n",
					queryCount, size, *runCount)
				fmt.Println("================================================")
			}
			drc.Close()
		}
	} else {
		// Create a graph traversal source object, after which we are ready to submit queries.
		g, drc, err := createConnection(*hostPtr, *portPtr, 2, bufferSize)

		if err != nil {
			log.Println("Error creating the connection - program terminating", err)
			return
		}

		run(g, *runCount, *runLevel, *memProfile)
		fmt.Println("================================================")

		drc.Close()
	}
}
