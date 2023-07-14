package kioperf

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"math"
	"net/url"
	"os"
	"sort"
	"time"
)

// Structures used by tests. 
type CommonConfig struct {
	FileType    string // "disk" or "s3"
	Operation   string // "read" or "write"
	Iterations  int    // Number of operations to run
	Threads     int    // Number of threads to run
	FileSizeMiB int64  // File length in MiB (write)
	Path        string // Containing directory path
	Files       int    // Number of files to write/read
	Debug       bool   // Log extra information if true
	CsvData     bool   // Print results as CVS if true
	DryRun      bool   // If true just parse and exit.
}

type S3Config struct {
	Common   CommonConfig
	S3Url    string // S3 URL
	S3Bucket string // Bucket for S3 operations parsed from S3 URL
}

type DiskConfig struct {
	Common CommonConfig
	Direct bool // If true use direct I/O (only for disk)
	Fsync  bool // If true fsync file at close (only for disk write)
}

// Interface for test implementation.
type Testable interface {
	// Get common config.
	GetCommonConfig() *CommonConfig
	// Initialize.
	Init()
	// Spawn a worker.
	Worker(id int, tasks chan string, results chan Result)
}

type Result struct {
	Operation           string
	WorkerId            int
	Path                string
	Succeeded           bool
	Bytes               int64
	StartTimeMs         float64
	Duration            float64
	FirstBlockArrivalMs float64
}

// Global information.
var DEBUG bool = false
var STARTTIME time.Time
var DATA []byte

// Print and conversion functions.
func debugPrintln(a ...any) {
	if DEBUG {
		fmt.Println(a)
	}
}

func debugPrintf(format string, a ...any) {
	if DEBUG {
		fmt.Printf(format, a)
	}
}

func millis64(d time.Duration) float64 {
	return float64(d) / 1000.0 / 1000.0
}

func DoTest(test Testable) {
	common := test.GetCommonConfig()

	// Prepare worker task and result channels.
	var tasks = make(chan string, common.Iterations)
	var results = make(chan Result, common.Iterations)
	var resultList = make([]Result, common.Iterations)

	// If this is a write operation, create a data buffer to write from.
	if common.Operation == "write" {
		byteCount := common.FileSizeMiB * 1024 * 1024
		DATA = make([]byte, byteCount)
		for i := int64(0); i < byteCount; i++ {
			DATA[i] = byte(i % 256)
		}
	}

	// Print common parameters.
	fmt.Printf("TEST PARAMETERS\n")
	fmt.Printf("    Operation:   %s:%s\n", common.FileType, common.Operation)
	fmt.Printf("    Iterations:  %d\n", common.Iterations)
	fmt.Printf("    Threads:     %d\n", common.Threads)
	fmt.Printf("    Files:       %d\n", common.Files)
	fmt.Printf("    FileSizeMiB: %d\n", common.FileSizeMiB)

	// Initialize the test and print test-specific parameters.
	test.Init()

	// Stop here if this is a dry run.
	if common.DryRun {
		return
	}

	// Generate file paths for each operation and place in task queue.
	for i := 0; i < common.Iterations; i++ {
		index := i % common.Files
		tasks <- fmt.Sprintf("%s/kioperf-file-%d.dat", common.Path, index)
	}

	// Generate and start workers.
	STARTTIME = time.Now()
	fmt.Println("Starting...", STARTTIME)

	for i := 0; i < common.Threads; i++ {
		go test.Worker(i, tasks, results)
	}
	close(tasks)

	// Read results and accumulate.
	for i := 0; i < common.Iterations; i++ {
		result := <-results
		resultList[i] = result
		debugPrintln(resultList[i])
		if !DEBUG {
			fmt.Printf(".")
		}
	}
	if !DEBUG {
		fmt.Printf("\n")
	}
	end := time.Now()
	fmt.Println("Ending...", end)
	duration := end.Sub(STARTTIME)
	fmt.Println("Duration: ", duration)

	// Generate summary aggregates.
	var files int
	var bytes int64
	var succeeded, failed int
	var min, max, sum float64
	min, max, sum = math.MaxFloat64, 0.0, 0.0
	for _, res := range resultList {
		files += 1
		bytes += res.Bytes
		if res.Succeeded {
			succeeded += 1
		} else {
			failed += 1
		}
		if min > res.Duration {
			min = res.Duration
		}
		if max < res.Duration {
			max = res.Duration
		}
		sum += res.Duration
	}

	// Print operation results in CVS if requested.
	if common.CsvData {
		fmt.Printf("CSV1,Operation,WorkerId,Path,Succeeded,Bytes,StartTimeMs,Duration,FirstBlockArrivalMs\n")
		for _, res := range resultList {
			fmt.Printf("CSV1,%s,%d,%s,%t,%d,%.4f,%.4f,%.4f\n", res.Operation, res.WorkerId,
				res.Path, res.Succeeded, res.Bytes, res.StartTimeMs, res.Duration,
				res.FirstBlockArrivalMs)
		}
	}

	// Print summary results.
	avg := sum / float64(common.Iterations)
	bytes_per_second := float64(bytes) / float64(duration.Nanoseconds()) * 1000.0 * 1000.0 * 1000.0
	mibs_per_second := bytes_per_second / 1024 / 1024
	fmt.Printf("    Operation:   %s:%s\n", common.FileType, common.Operation)
	fmt.Printf("STATISTICS\n")
	fmt.Printf("  Throughput: %.4f MiB/sec\n", mibs_per_second)
	fmt.Printf("  Files:      %d\n", files)
	fmt.Printf("  Bytes:      %d\n", bytes)
	fmt.Printf("  Succeeded:  %d\n", succeeded)
	fmt.Printf("  Failed:     %d\n", failed)
	fmt.Printf("  I/O Duration Statistics\n")
	fmt.Printf("    Min:        %.4f msec\n", min)
	fmt.Printf("    Avg:        %.4f msec\n", avg)
	fmt.Printf("    Max:        %.4f msec\n", max)
	fmt.Printf("  I/O Duration Percentiles\n")

	// Sort results and print duration of operations.
	sort.Slice(resultList, func(i, j int) bool { return resultList[i].Duration < resultList[j].Duration })
	for _, p := range []int{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99} {
		pDuration := resultList[p*common.Iterations/100].Duration
		fmt.Printf("    P%d: %.4f\n", p, pDuration)
	}
}

func (config *DiskConfig) GetCommonConfig() *CommonConfig {
	return &config.Common
}

func (config *DiskConfig) Init() {
	fmt.Printf("    Direct:      %t\n", config.Direct)
	fmt.Printf("    Fsync:       %t\n", config.Fsync)
}

func (config *DiskConfig) Worker(id int, tasks chan string, results chan Result) {
	debugPrintf("Disk Worker %d started\n", id)
	for path := range tasks {
		var result Result
		if config.Common.Operation == "write" {
			result = diskWrite(path, config.Common.FileSizeMiB, config.Fsync)
		} else {
			result = diskRead(path)
		}
		result.WorkerId = id
		results <- result
	}
	debugPrintf("Disk Worker %d ended\n", id)
}

func diskWrite(path string, size int64, fsync bool) Result {
	var result Result
	result.Operation = "disk-write"
	result.Path = path

	data := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		data[i] = byte(i % 256)
	}

	start := time.Now()
	result.StartTimeMs = millis64(start.Sub(STARTTIME))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		fmt.Println(err)
		return result
	}
	defer f.Close()
	blocksToWrite := size * 1024
	for i := 0; i < int(blocksToWrite); i++ {
		written, err := f.Write(data)
		if err != nil {
			fmt.Println(err)
			end := time.Now()
			result.Duration = millis64(end.Sub(start))
			return result
		}
		result.Bytes += int64(written)
	}
	if fsync {
		f.Sync()
	}
	end := time.Now()
	result.Duration = millis64(end.Sub(start))
	result.Succeeded = true
	return result
}

func diskRead(path string) Result {
	var result Result
	result.Operation = "disk-read"
	result.Path = path
	data := make([]byte, 1024)

	start := time.Now()
	result.StartTimeMs = millis64(start.Sub(STARTTIME))
	f, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		return result
	}
	defer f.Close()
	for {
		bytes, err := f.Read(data)
		if err == io.EOF {
			result.Succeeded = true
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}
		result.Bytes += int64(bytes)
		if result.FirstBlockArrivalMs <= 0.0 {
			now := time.Now()
			result.FirstBlockArrivalMs = millis64(now.Sub(start))
		}
	}
	end := time.Now()
	result.Duration = millis64(end.Sub(start))
	return result
}

func (config *S3Config) GetCommonConfig() *CommonConfig {
	return &config.Common
}

// func s3Init(config *Config) {
func (config *S3Config) Init() {
	// Parse the S3 URL.
	parsedUrl, err := url.Parse(config.S3Url)
	if err != nil {
		panic(fmt.Sprintf("Unable to parse S3 URL: err=%s, url=%s", err, config.S3Url))
	}
	if parsedUrl.Scheme != "s3" || parsedUrl.Host == "" || parsedUrl.Path == "" {
		panic(fmt.Sprintf("URL must have format s3://bucket/path"))
	}
	config.S3Bucket = parsedUrl.Host
	config.Common.Path = parsedUrl.Path

	// Print the parameters.
	fmt.Printf("    S3Url:       %s\n", config.S3Url)
	fmt.Printf("    S3Bucket:    %s\n", config.S3Bucket)
}

// func s3Worker(id int, config Config, tasks chan string, results chan Result) {
func (config *S3Config) Worker(id int, tasks chan string, results chan Result) {
	debugPrintf("S3 Worker %d started\n", id)
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	}))
	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)

	for path := range tasks {
		var result Result
		if config.Common.Operation == "write" {
			result = s3Write(config.S3Bucket, path, uploader)
		} else {
			result = s3Read(config.S3Bucket, path, downloader)
		}
		result.WorkerId = id
		results <- result
	}
	debugPrintf("S3 Worker %d ended\n", id)
}

func s3Write(bucket string, path string, uploader *s3manager.Uploader) Result {
	var result Result
	result.Operation = "s3-write"
	result.Path = path

	start := time.Now()
	result.StartTimeMs = millis64(start.Sub(STARTTIME))

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(DATA),
	})
	end := time.Now()
	result.Duration = millis64(end.Sub(start))
	if err == nil {
		result.Bytes = int64(len(DATA))
		result.Succeeded = true
	} else {
		fmt.Printf("failed to upload file, %v\n", err)
	}
	return result
}

// Implement a WriterAt interface that counts bytes written to it.
type ByteCounter struct {
	Count              int64
	FirstByteArrivedAt time.Time
}

func (bc *ByteCounter) WriteAt(p []byte, off int64) (n int, err error) {
	if bc.Count == 0 {
		bc.FirstByteArrivedAt = time.Now()
	}
	bc.Count += int64(len(p))
	return len(p), nil
}

func s3Read(bucket string, path string, downloader *s3manager.Downloader) Result {
	var result Result
	result.Operation = "s3-read"
	result.Path = path

	start := time.Now()
	result.StartTimeMs = millis64(start.Sub(STARTTIME))

	var bc ByteCounter

	numBytes, err := downloader.Download(&bc, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
	end := time.Now()
	result.Duration = millis64(end.Sub(start))
	if err == nil {
		if bc.Count != numBytes {
			fmt.Printf("Warning, mismatch--numBytes: %d bc.Count: %d\n", numBytes, bc.Count)
		}
		result.Bytes = bc.Count
		result.FirstBlockArrivalMs = millis64(bc.FirstByteArrivedAt.Sub(start))
		result.Succeeded = true
	} else {
		fmt.Printf("failed to upload file, %v\n", err)
	}
	return result
}
