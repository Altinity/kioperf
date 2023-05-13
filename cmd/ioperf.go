package main

import (
	"fmt"
	"github.com/hodgesrm/ioperf/pkg/ioperf"
	"github.com/spf13/cobra"
	"os"
)

var cfg ioperf.Config

var diskCmd = &cobra.Command{
	Use:   "disk",
	Short: "Run disk I/O test",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("disk command")
		cfg.FileType = "disk"
		ioperf.DoTest(cfg)
	},
}

var s3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Run S3 I/O test",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("s3 command")
		cfg.FileType = "s3"
		ioperf.DoTest(cfg)
	},
}

var rootCmd = &cobra.Command{
	Use:   "ioperf",
	Short: "ioperf - demo test utility to show I/O performance",
	Long:  `TBD`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func main() {
	diskCmd.Flags().StringVar(&cfg.Operation, "operation", "read", "I/O to perform: write or read")
	diskCmd.Flags().IntVar(&cfg.Iterations, "iterations", 1, "Number of operations to perform")
	diskCmd.Flags().IntVar(&cfg.Threads, "threads", 1, "Number of threads to run")
	diskCmd.Flags().StringVar(&cfg.DirPath, "dir-path", "./test", "Directory path for test files")
	diskCmd.Flags().IntVar(&cfg.Files, "files", 1, "Number of files to use")
	diskCmd.Flags().Int64Var(&cfg.FileSizeMiB, "size", 1, "Size of files in MiB (write)")
	diskCmd.Flags().BoolVar(&cfg.Sync, "sync", false, "Fsync after operation (write)")
	diskCmd.Flags().BoolVar(&cfg.Debug, "debug", false, "Print debug info")
	diskCmd.Flags().BoolVar(&cfg.CsvData, "csv", false, "Generate CSV data")
	rootCmd.AddCommand(diskCmd)

	s3Cmd.Flags().StringVar(&cfg.Operation, "operation", "read", "I/O to perform: write or read")
	s3Cmd.Flags().IntVar(&cfg.Iterations, "iterations", 1, "Number of operations to perform")
	s3Cmd.Flags().IntVar(&cfg.Threads, "threads", 1, "Number of threads to run")
	s3Cmd.Flags().StringVar(&cfg.Bucket, "bucket", "", "Bucket for test files")
	s3Cmd.Flags().StringVar(&cfg.DirPath, "prefix", "test/", "Prefix for test files")
	s3Cmd.Flags().IntVar(&cfg.Files, "files", 1, "Number of files to use")
	s3Cmd.Flags().Int64Var(&cfg.FileSizeMiB, "size", 1, "Size of files in MiB (write)")
	s3Cmd.Flags().BoolVar(&cfg.Sync, "sync", false, "Fsync after operation (write)")
	s3Cmd.Flags().BoolVar(&cfg.Debug, "debug", false, "Print debug info")
	s3Cmd.Flags().BoolVar(&cfg.CsvData, "csv", false, "Generate CSV data")
	rootCmd.AddCommand(s3Cmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "There was an error during execution: '%s'", err)
		os.Exit(1)
	}
}
