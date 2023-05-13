package ioperf

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"math"
	"os"
	"sort"
	"time"
)

// IO Perf configuration
type Config struct {
	FileType    string // "disk" or "s3"
	Operation   string // "read" or "write"
	Iterations  int    // Number of operations to run
	Threads     int    // Number of threads to run
	DirPath     string // Containing directory path (prefix for S3)
	Bucket      string // Bucket for S3 operations
	Files       int    // Number of files to write/read
	FileSizeMiB int64  // File length in MiB (write)
	Sync        bool   // If true fsync after operation (write)
	Debug       bool   // Log extra information if true
	CsvData     bool   // Print results as CVS if true
}

type Result struct {
	Operation  string
	WorkerId   int
	Path       string
	Succeeded  bool
	Bytes      int64
	SinceStart float64
	Duration   float64
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

func DoTest(config Config) {
	DEBUG = config.Debug

	// Prepare worker task and result channels.
	var tasks = make(chan string, config.Iterations)
	var results = make(chan Result, config.Iterations)
	var resultList = make([]Result, config.Iterations)

	// Generate file paths for each operation and place in task queue.
	for i := 0; i < config.Iterations; i++ {
		index := i % config.Files
		tasks <- fmt.Sprintf("%s/ioperf-file-%d.dat", config.DirPath, index)
	}

	// If this is a write operation, create a data buffer to write from.
	if config.Operation == "write" {
		byteCount := config.FileSizeMiB * 1024 * 1024
		DATA = make([]byte, byteCount)
		for i := int64(0); i < byteCount; i++ {
			DATA[i] = byte(i % 256)
		}
	}

	// Generate and start workers.
	STARTTIME = time.Now()
	fmt.Println("Starting...", STARTTIME)

	for i := 0; i < config.Threads; i++ {
		if config.FileType == "disk" {
			go diskWorker(i, config, tasks, results)
		} else if config.FileType == "s3" {
			go s3Worker(i, config, tasks, results)
		}
	}
	close(tasks)

	// Read results and accumulate.
	for i := 0; i < config.Iterations; i++ {
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
	if config.CsvData {
		fmt.Printf("CSV1,Operation,WorkerId,Path,Succeeded,Bytes,SinceStart,Duration\n")
		for _, res := range resultList {
			fmt.Printf("CSV1,%s,%d,%s,%t,%d,%.4f,%.4f\n", res.Operation, res.WorkerId,
				res.Path, res.Succeeded, res.Bytes, res.SinceStart, res.Duration)
		}
	}

	// Print summary results.
	avg := sum / float64(config.Iterations)
	bytes_per_second := float64(bytes) / float64(duration.Nanoseconds()) * 1000.0 * 1000.0 * 1000.0
	mibs_per_second := bytes_per_second / 1024 / 1024
	fmt.Printf("RUN STATS--Throughput: %.4f MiB/sec files: %d Bytes: %d Succeeded: %d Failed: %d\n", mibs_per_second, files, bytes, succeeded, failed)
	fmt.Printf("OP STATS --Min: %.4f Average: %.4f Max: %.4f\n", min, avg, max)

	// Sort results and print duration of operations.
	sort.Slice(resultList, func(i, j int) bool { return resultList[i].Duration < resultList[j].Duration })
	for _, p := range []int{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99} {
		pDuration := resultList[p*config.Iterations/100].Duration
		fmt.Printf("P%d: %.4f\n", p, pDuration)
	}
}

func diskWorker(id int, config Config, tasks chan string, results chan Result) {
	debugPrintf("Disk Worker %d started\n", id)
	for path := range tasks {
		var result Result
		if config.Operation == "write" {
			result = diskWrite(path, config.FileSizeMiB)
		} else {
			result = diskRead(path)
		}
		result.WorkerId = id
		results <- result
	}
	debugPrintf("Disk Worker %d ended\n", id)
}

func diskWrite(path string, size int64) Result {
	var result Result
	result.Operation = "disk-write"
	result.Path = path

	data := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		data[i] = byte(i % 256)
	}

	start := time.Now()
	result.SinceStart = millis64(start.Sub(STARTTIME))
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
	result.SinceStart = millis64(start.Sub(STARTTIME))
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
	}
	end := time.Now()
	result.Duration = millis64(end.Sub(start))
	return result
}

func s3Worker(id int, config Config, tasks chan string, results chan Result) {
	debugPrintf("S3 Worker %d started\n", id)
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	}))
	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)

	for path := range tasks {
		var result Result
		if config.Operation == "write" {
			result = s3Write(config.Bucket, path, uploader)
		} else {
			result = s3Read(config.Bucket, path, downloader)
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
	result.SinceStart = millis64(start.Sub(STARTTIME))

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
type ByteCounter int64

func (c *ByteCounter) WriteAt(p []byte, off int64) (n int, err error) {
	*c += ByteCounter(len(p))
	return len(p), nil
}

func s3Read(bucket string, path string, downloader *s3manager.Downloader) Result {
	var result Result
	result.Operation = "s3-read"
	result.Path = path

	start := time.Now()
	result.SinceStart = millis64(start.Sub(STARTTIME))

	var c ByteCounter

	numBytes, err := downloader.Download(&c, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
	end := time.Now()
	fmt.Println(c, numBytes)
	result.Duration = millis64(end.Sub(start))
	if err == nil {
		result.Bytes = numBytes
		result.Succeeded = true
	} else {
		fmt.Printf("failed to upload file, %v\n", err)
	}
	return result
}
