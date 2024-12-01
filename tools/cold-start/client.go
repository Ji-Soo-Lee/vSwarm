// MIT License
//
// Copyright (c) 2020 Dmitrii Ustiugov and EASE lab
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"github.com/vhive-serverless/vSwarm/tools/benchmarking_eventing/vhivemetadata"
	"github.com/vhive-serverless/vSwarm/tools/endpoint"
	tracing "github.com/vhive-serverless/vSwarm/utils/tracing/go"

	pb "github.com/vhive-serverless/vSwarm/utils/protobuf/helloworld"
)

const TimeseriesDBAddr = "10.96.0.84:90"

var (
	completed         int64
	profSlice         LatencySlice
	latSliceWithFunc  LatencySliceWithFunc
	funcDurEnableFlag *bool
	portFlag          *int
	grpcTimeout       time.Duration
	withTracing       *bool
	workflowIDs       map[*endpoint.Endpoint]string
	traceFile         *string
)

func main() {
	endpointsFile := flag.String("endpointsFile", "endpoints.json", "File with endpoints' metadata")
	traceFile = flag.String("traceFile", "./tools/trace/load_trace.csv", "Trace file to schedule invocations")
	latencyOutputFile := flag.String("latf", "lat.csv", "CSV file for the latency measurements in microseconds")
	funcDurationOutputFile := flag.String("durf", "dur.csv", "CSV file for the function duration measurements in microseconds")
	funcDurEnableFlag = flag.Bool("profile", false, "Enable function duration profiling")
	portFlag = flag.Int("port", 80, "The port that functions listen to")
	withTracing = flag.Bool("trace", false, "Enable tracing in the client")
	zipkin := flag.String("zipkin", "http://localhost:9411/api/v2/spans", "zipkin url")
	debug := flag.Bool("dbg", false, "Enable debug logging")
	grpcTimeout = time.Duration(*flag.Int("grpcTimeout", 30, "Timeout in seconds for gRPC requests")) * time.Second

	flag.Parse()

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: time.RFC3339Nano,
		FullTimestamp:   true,
	})
	log.SetOutput(os.Stdout)
	if *debug {
		log.SetLevel(log.DebugLevel)
		log.Debug("Debug logging is enabled")
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Info("Reading the endpoints from the file: ", *endpointsFile)

	endpoints, err := readEndpoints(*endpointsFile)
	if err != nil {
		log.Fatal("Failed to read the endpoints file: ", err)
	}

	traceData, err := readTrace(*traceFile)
	if err != nil {
		log.Fatal("Failed to read the trace file: ", err)
	}

	workflowIDs = make(map[*endpoint.Endpoint]string)
	for _, ep := range endpoints {
		workflowIDs[ep] = uuid.New().String()
	}

	if *withTracing {
		shutdown, err := tracing.InitBasicTracer(*zipkin, "invoker")
		if err != nil {
			log.Print(err)
		}
		defer shutdown()
	}

	runTraceExperiment(endpoints, traceData, *latencyOutputFile)
	if *funcDurEnableFlag {
		writeFunctionDurations(*funcDurationOutputFile)
	}
}

func readTrace(filePath string) ([]TraceEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var trace []TraceEntry
	for i, record := range records {
		if i == 0 {
			// Skip header
			continue
		}
		// Parse time as float
		timeStamp, err := strconv.ParseFloat(record[0], 64)
		if err != nil {
			return nil, err
		}
		trace = append(trace, TraceEntry{
			Time:   timeStamp,
			Action: record[1],
		})
	}
	return trace, nil
}

func runTraceExperiment(endpoints []*endpoint.Endpoint, trace []TraceEntry, latencyOutputFile string) {
	var wg sync.WaitGroup
	startTime := time.Now()

	// Create the latency output file
	latencyFile, err := os.Create(latencyOutputFile)
	if err != nil {
		log.Fatalf("Error creating latency output file: %v", err)
	}
	defer latencyFile.Close()

	writer := csv.NewWriter(latencyFile)
	defer writer.Flush()

	// Write header to latency file
	if err := writer.Write([]string{"timestamp", "function", "latency_ms"}); err != nil {
		log.Fatalf("Error writing header to latency file: %v", err)
	}

	for _, entry := range trace {
		wg.Add(1)
		go func(entry TraceEntry) {
			defer wg.Done()
			delay := time.Duration(entry.Time * float64(time.Second))
			time.Sleep(delay - time.Since(startTime))

			// Match endpoint directly with the full action name
			for _, endpoint := range endpoints {
				if endpoint.Hostname == entry.Action {
					start := time.Now()
					if endpoint.Eventing {
						invokeEventingFunction(endpoint) // Eventing invocation
					} else {
						invokeServingFunction(endpoint) // Function invocation
					}
					latency := time.Since(start).Milliseconds()

					// Record latency
					record := []string{
						fmt.Sprintf("%d", time.Since(startTime).Milliseconds()),
						entry.Action,
						fmt.Sprintf("%d", latency),
					}
					writer.Write(record)
					writer.Flush()
					break
				}
			}
		}(entry)
	}

	wg.Wait()
	log.Println("Trace-based experiment completed.")

	// Write latencies to file
	writeLatenciesWithFunc(latencyOutputFile)
}

type TraceEntry struct {
	Time   float64
	Action string
}

func readEndpoints(path string) (endpoints []*endpoint.Endpoint, _ error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &endpoints); err != nil {
		return nil, err
	}
	return
}

func SayHello(address, workflowID string) {
	dialOptions := make([]grpc.DialOption, 0)
	dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if *withTracing {
		dialOptions = append(dialOptions, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}
	conn, err := grpc.NewClient(address, dialOptions...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewGreeterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	response, err := c.SayHello(ctx, &pb.HelloRequest{
		Name: "Invoke relay",
		VHiveMetadata: vhivemetadata.MakeVHiveMetadata(
			workflowID,
			uuid.New().String(),
			time.Now().UTC(),
		),
	})
	if err != nil {
		log.Warnf("Failed to invoke %v, err=%v", address, err)
	} else {
		if *funcDurEnableFlag {
			log.Debugf("Inside if\n")
			words := strings.Fields(response.Message)
			lastWord := words[len(words)-1]
			duration, err := strconv.ParseInt(lastWord, 10, 64)
			if err == nil {
				profSlice.Lock()
				profSlice.slice = append(profSlice.slice, duration)
				profSlice.Unlock()
			}
		}
		atomic.AddInt64(&completed, 1)
	}
}

func invokeEventingFunction(endpoint *endpoint.Endpoint) {
	address := fmt.Sprintf("%s:%d", endpoint.Hostname, *portFlag)
	log.Debug("Invoking asynchronously: ", address)

	SayHello(address, workflowIDs[endpoint])
}

func invokeServingFunction(endpoint *endpoint.Endpoint) {
	defer getDurationWithFunc(startMeasurement(endpoint.Hostname)) // measure entire invocation time

	address := fmt.Sprintf("%s:%d", endpoint.Hostname, *portFlag)
	log.Debug("Invoking: ", address)

	SayHello(address, workflowIDs[endpoint])
}

// LatencySlice is a thread-safe slice to hold a slice of latency measurements.
type LatencySlice struct {
	sync.Mutex
	slice []int64
}

type LatencyRecord struct {
	invoked int64
	latency int64
}

type LatencySliceWithFunc struct {
	sync.Mutex
	slice []LatencyRecord
}

func startMeasurement(msg string) (string, time.Time) {
	return msg, time.Now()
}

func getDurationWithFunc(msg string, start time.Time) {
	latency := time.Since(start)
	log.Debugf("Invoked %v in %v usec\n", msg, latency.Microseconds())
	addDurationsWithFunc(start, []string{msg}, []time.Duration{latency})
}

func addDurationsWithFunc(start time.Time, msg []string, ds []time.Duration) {
	log.Debugf("%v : %v\n", msg, ds)
	latSliceWithFunc.Lock()
	for _, d := range ds {
		for _, m := range msg {
			latSliceWithFunc.slice = append(latSliceWithFunc.slice, LatencyRecord{
				invoked: start.UnixMilli(),
				latency: d.Microseconds(),
			})
		}
	}
	latSliceWithFunc.Unlock()
}

func writeLatenciesWithFunc(latencyOutputFile string) {
	latSliceWithFunc.Lock()
	defer latSliceWithFunc.Unlock()

	// Sort slice by invoked time
	sort.Slice(latSliceWithFunc.slice, func(i, j int) bool {
		return latSliceWithFunc.slice[i].invoked < latSliceWithFunc.slice[j].invoked
	})

	fileName := latencyOutputFile
	log.Info("The measured latencies are saved in ", fileName)

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)

	if err != nil {
		log.Fatal("Failed creating file: ", err)
	}

	datawriter := bufio.NewWriter(file)

	for _, latRecord := range latSliceWithFunc.slice {
		line := fmt.Sprintf("%d\n", latRecord.latency)
		_, err := datawriter.WriteString(line)
		if err != nil {
			log.Fatal("Failed to write the latencies to a file ", err)
		}
	}

	datawriter.Flush()
	file.Close()
}

func writeFunctionDurations(funcDurationOutputFile string) {
	profSlice.Lock()
	defer profSlice.Unlock()

	fileName := funcDurationOutputFile
	log.Info("The measured function durations are saved in ", fileName)

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)

	if err != nil {
		log.Fatal("Failed creating file: ", err)
	}

	datawriter := bufio.NewWriter(file)

	for _, dur := range profSlice.slice {
		_, err := datawriter.WriteString(strconv.FormatInt(dur, 10) + "\n")
		if err != nil {
			log.Fatal("Failed to write the function durations to a file ", err)
		}
	}

	datawriter.Flush()
	file.Close()
}
