package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jobplatform/jobplatform"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("command required")
	}
	switch args[0] {
	case "submit":
		return submit(args[1:])
	case "worker":
		return worker(args[1:])
	case "cancel":
		return cancelJob(args[1:])
	case "get":
		return getJob(args[1:])
	case "list":
		return listJobs(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func submit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	storePath := fs.String("store", "jobs.json", "job store path")
	taskType := fs.String("type", "noop", "registered task type")
	maxRetries := fs.Int("retries", 0, "maximum retry attempts")
	argJSON := fs.String("args", "{}", "JSON task arguments")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var taskArgs map[string]any
	if err := json.Unmarshal([]byte(*argJSON), &taskArgs); err != nil {
		return fmt.Errorf("parse args: %w", err)
	}
	store, err := jobplatform.OpenStore(*storePath)
	if err != nil {
		return err
	}
	engine := jobplatform.NewEngine(store, jobplatform.NewRegistry(), jobplatform.Options{})
	job, err := engine.Submit(jobplatform.JobDefinition{
		Type:       *taskType,
		Args:       taskArgs,
		MaxRetries: *maxRetries,
	})
	if err != nil {
		return err
	}
	return printJSON(job)
}

func worker(args []string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	storePath := fs.String("store", "jobs.json", "job store path")
	workers := fs.Int("workers", 1, "number of local workers")
	once := fs.Bool("once", false, "stop when all currently known jobs are terminal")
	pollMS := fs.Int("poll-ms", 25, "queue polling interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := jobplatform.OpenStore(*storePath)
	if err != nil {
		return err
	}
	engine := jobplatform.NewEngine(store, jobplatform.NewRegistry(), jobplatform.Options{
		Workers:      *workers,
		PollInterval: time.Duration(*pollMS) * time.Millisecond,
	})
	if *once {
		jobs, err := engine.List()
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(jobs))
		for _, job := range jobs {
			ids = append(ids, job.ID)
		}
		signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stopSignals()
		runCtx, stopRun := context.WithCancel(signalCtx)
		errCh := make(chan error, 1)
		go func() { errCh <- engine.Run(runCtx) }()
		go func() {
			if err := engine.WaitForTerminal(signalCtx, ids...); err == nil {
				stopRun()
			}
		}()
		runErr := <-errCh
		if signalCtx.Err() != nil {
			return runErr
		}
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return engine.Run(ctx)
}

func cancelJob(args []string) error {
	fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
	storePath := fs.String("store", "jobs.json", "job store path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("cancel requires exactly one job id")
	}
	store, err := jobplatform.OpenStore(*storePath)
	if err != nil {
		return err
	}
	engine := jobplatform.NewEngine(store, jobplatform.NewRegistry(), jobplatform.Options{})
	if err := engine.Cancel(fs.Arg(0)); err != nil {
		return err
	}
	job, err := engine.Get(fs.Arg(0))
	if err != nil {
		return err
	}
	return printJSON(job)
}

func getJob(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	storePath := fs.String("store", "jobs.json", "job store path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("get requires exactly one job id")
	}
	store, err := jobplatform.OpenStore(*storePath)
	if err != nil {
		return err
	}
	engine := jobplatform.NewEngine(store, jobplatform.NewRegistry(), jobplatform.Options{})
	job, err := engine.Get(fs.Arg(0))
	if err != nil {
		return err
	}
	return printJSON(job)
}

func listJobs(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	storePath := fs.String("store", "jobs.json", "job store path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := jobplatform.OpenStore(*storePath)
	if err != nil {
		return err
	}
	engine := jobplatform.NewEngine(store, jobplatform.NewRegistry(), jobplatform.Options{})
	jobs, err := engine.List()
	if err != nil {
		return err
	}
	return printJSON(jobs)
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: jobplatform <submit|worker|cancel|get|list> [flags]")
}
