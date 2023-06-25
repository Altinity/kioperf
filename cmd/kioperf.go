package main

import (
	"fmt"
	"github.com/hodgesrm/kioperf/pkg/kioperf"
	"github.com/spf13/cobra"
	"os"
)

var diskCfg = new(kioperf.DiskConfig)

var diskCmd = &cobra.Command{
	Use:   "disk",
	Short: "Run disk I/O test",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("disk command")
		diskCfg.Common.FileType = "disk"
		kioperf.DoTest(diskCfg)
	},
}

var s3Cfg = new(kioperf.S3Config)

var s3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Run S3 I/O test",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("s3 command")
		s3Cfg.Common.FileType = "s3"
		kioperf.DoTest(s3Cfg)
	},
}

var rootCmd = &cobra.Command{
	Use:   "kioperf",
	Short: "kioperf - demo utility to show I/O performance",
	Long:  "kioperf - demo utility to show I/O performance",
}

func createCommonFlags(cmd *cobra.Command, cfg *kioperf.CommonConfig) {
	cmd.Flags().StringVar(&cfg.Operation, "operation", "read", "I/O to perform: write,read,clean")
	cmd.Flags().IntVar(&cfg.Iterations, "iterations", 1, "Number of operations to perform")
	cmd.Flags().IntVar(&cfg.Threads, "threads", 1, "Number of threads to run")
	cmd.Flags().StringVar(&cfg.Path, "dir-path", "./kioperf-data", "Directory path for test files")
	cmd.Flags().IntVar(&cfg.Files, "files", 1, "Number of files to use")
	cmd.Flags().Int64Var(&cfg.FileSizeMiB, "size", 1, "Size of files in MiB (write)")
	cmd.Flags().BoolVar(&cfg.Debug, "debug", false, "Print debug info")
	cmd.Flags().BoolVar(&cfg.CsvData, "csv", false, "Generate CSV data")
	cmd.Flags().BoolVar(&cfg.DryRun, "dry-run", false, "Parse arguments and quit")
}

func main() {
	createCommonFlags(diskCmd, &diskCfg.Common)
	diskCmd.Flags().BoolVar(&diskCfg.Fsync, "fsync", false, "Fsync file at close (write)")
	diskCmd.Flags().BoolVar(&diskCfg.Direct, "direct", false, "Use direct I/O")
	rootCmd.AddCommand(diskCmd)

	createCommonFlags(s3Cmd, &s3Cfg.Common)
	s3Cmd.Flags().StringVar(&s3Cfg.S3Url, "s3-url", "s3://bucket/kioperf-data/", "S3 URL prefix for test files")
	rootCmd.AddCommand(s3Cmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "There was an error during execution: '%s'", err)
		os.Exit(1)
	}
}
