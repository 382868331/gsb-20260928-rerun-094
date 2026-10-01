package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"background-jobs/jobs"
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
		return nil
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	storePath := fs.String("store", "data/jobs.json", "job state file")
	dependsOn := fs.String("depends-on", "", "comma-separated predecessor job IDs (enqueue)")
	group := fs.String("group", "", "group name for batch cancel (enqueue)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	rest := fs.Args()
	registry := jobs.BuiltinRegistry()

	switch args[0] {
	case "types":
		for _, typ := range registry.Types() {
			fmt.Println(typ)
		}
	case "enqueue":
		if len(rest) < 1 {
			return fmt.Errorf("usage: jobctl enqueue [--store file] [--depends-on id1,id2] [--group name] <type> [payload] [max-attempts]")
		}
		payload := ""
		maxAttempts := 3
		if len(rest) > 1 {
			payload = rest[1]
		}
		if len(rest) > 2 {
			if _, err := fmt.Sscanf(rest[2], "%d", &maxAttempts); err != nil {
				return err
			}
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		var opts []jobs.EnqueueOption
		if *dependsOn != "" {
			opts = append(opts, jobs.WithDependencies(strings.Split(*dependsOn, ",")...))
		}
		if *group != "" {
			opts = append(opts, jobs.WithGroup(*group))
		}
		job, err := jobs.NewManager(store, registry).Enqueue(rest[0], payload, maxAttempts, opts...)
		if err != nil {
			return err
		}
		fmt.Println(job.ID)
	case "cancel-group":
		if len(rest) != 1 {
			return fmt.Errorf("usage: jobctl cancel-group [--store file] <group-name>")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		canceled, err := jobs.NewManager(store, registry).CancelGroup(rest[0])
		if err != nil {
			return err
		}
		return printJSON(canceled)
	case "list":
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		jobList, err := store.List()
		if err != nil {
			return err
		}
		return printJSON(jobList)
	case "show":
		if len(rest) != 1 {
			return fmt.Errorf("usage: jobctl show [--store file] <job-id>")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		job, attempts, err := store.Get(rest[0])
		if err != nil {
			return err
		}
		return printJSON(map[string]any{"job": job, "attempts": attempts})
	case "cancel":
		if len(rest) != 1 {
			return fmt.Errorf("usage: jobctl cancel [--store file] <job-id>")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		_, err = jobs.NewManager(store, registry).Cancel(rest[0])
		return err
	case "recover":
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		recovered, err := jobs.NewManager(store, registry).RecoverInterrupted()
		if err != nil {
			return err
		}
		return printJSON(recovered)
	case "worker":
		return runWorker(*storePath, registry)
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
	return nil
}

func runWorker(path string, registry *jobs.Registry) error {
	store, err := jobs.OpenStore(path)
	if err != nil {
		return err
	}
	defer store.Close()
	manager := jobs.NewManager(store, registry)
	recovered, err := manager.RecoverInterrupted()
	if err != nil {
		return err
	}
	for _, id := range recovered {
		fmt.Fprintln(os.Stderr, "recovered job for redispatch:", id)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = jobs.NewWorker(manager, registry).Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func printJSON(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func usage() {
	fmt.Println(`commands: types, enqueue, list, show, cancel, cancel-group, recover, worker`)
}
