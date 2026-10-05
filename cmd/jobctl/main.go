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
	command := args[0]
	storePath := "data/jobs.json"
	rest, err := extractGlobalStore(args[1:], &storePath)
	if err != nil {
		return err
	}
	registry := jobs.BuiltinRegistry()

	switch command {
	case "types":
		for _, typ := range registry.Types() {
			fmt.Println(typ)
		}
	case "enqueue":
		enqueueFlags := flag.NewFlagSet("enqueue", flag.ContinueOnError)
		dependsOn := enqueueFlags.String("depends-on", "", "comma-separated predecessor job IDs (enqueue)")
		group := enqueueFlags.String("group", "", "group name for batch cancel (enqueue)")
		if err := enqueueFlags.Parse(rest); err != nil {
			return err
		}
		rest = enqueueFlags.Args()
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
		store, err := jobs.OpenStore(storePath)
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
		store, err := jobs.OpenStore(storePath)
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
		store, err := jobs.OpenStore(storePath)
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
		store, err := jobs.OpenStore(storePath)
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
		store, err := jobs.OpenStore(storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		_, err = jobs.NewManager(store, registry).Cancel(rest[0])
		return err
	case "recover":
		store, err := jobs.OpenStore(storePath)
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
		return runWorker(storePath, registry)
	case "revisions":
		store, err := jobs.OpenStore(storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		revisions, err := jobs.NewManager(store, registry).ListRevisions()
		if err != nil {
			return err
		}
		return printJSON(revisions)
	case "propose-revision":
		cmdFlags := flag.NewFlagSet("propose-revision", flag.ContinueOnError)
		cmdStore := cmdFlags.String("store", storePath, "job state file")
		specPath := cmdFlags.String("spec-file", "", "JSON file containing ChangeSpec values")
		specJSON := cmdFlags.String("spec", "", "JSON array containing ChangeSpec values")
		note := cmdFlags.String("note", "", "revision note")
		var err error
		if err = cmdFlags.Parse(rest); err != nil {
			return err
		}
		var data []byte
		data = []byte(*specJSON)
		if *specPath != "" {
			data, err = os.ReadFile(*specPath)
			if err != nil {
				return err
			}
		}
		var specs []jobs.ChangeSpec
		if err := json.Unmarshal(data, &specs); err != nil {
			return err
		}
		store, err := jobs.OpenStore(*cmdStore)
		if err != nil {
			return err
		}
		defer store.Close()
		revision, err := jobs.NewManager(store, registry).ProposeRevision(specs, *note)
		if err != nil {
			return err
		}
		return printJSON(revision)
	case "preview-revision", "show-revision", "compare-revision":
		cmdFlags := flag.NewFlagSet(args[0], flag.ContinueOnError)
		cmdStore := cmdFlags.String("store", storePath, "job state file")
		if err := cmdFlags.Parse(rest); err != nil {
			return err
		}
		if cmdFlags.NArg() != 1 {
			return fmt.Errorf("usage: jobctl %s [--store file] <revision-id>", args[0])
		}
		store, err := jobs.OpenStore(*cmdStore)
		if err != nil {
			return err
		}
		defer store.Close()
		manager := jobs.NewManager(store, registry)
		var value any
		switch args[0] {
		case "preview-revision":
			value, err = manager.PreviewRevision(cmdFlags.Arg(0))
		case "show-revision":
			value, err = manager.GetRevision(cmdFlags.Arg(0))
		case "compare-revision":
			value, err = manager.CompareRevision(cmdFlags.Arg(0))
		}
		if err != nil {
			return err
		}
		return printJSON(value)
	case "confirm-revision", "cancel-revision":
		cmdFlags := flag.NewFlagSet(args[0], flag.ContinueOnError)
		cmdStore := cmdFlags.String("store", storePath, "job state file")
		indexes := cmdFlags.String("indexes", "", "comma-separated zero-based change indexes; empty means all")
		if err := cmdFlags.Parse(rest); err != nil {
			return err
		}
		if cmdFlags.NArg() != 1 {
			return fmt.Errorf("usage: jobctl %s [--store file] [--indexes 0,2] <revision-id>", args[0])
		}
		var selected []int
		if *indexes != "" {
			for _, part := range strings.Split(*indexes, ",") {
				var index int
				if _, err := fmt.Sscanf(strings.TrimSpace(part), "%d", &index); err != nil {
					return err
				}
				selected = append(selected, index)
			}
		} else {
			selected = nil
		}
		store, err := jobs.OpenStore(*cmdStore)
		if err != nil {
			return err
		}
		defer store.Close()
		manager := jobs.NewManager(store, registry)
		if args[0] == "confirm-revision" {
			report, err := manager.ConfirmRevision(cmdFlags.Arg(0), selected)
			if err != nil {
				return err
			}
			return printJSON(report)
		}
		revision, err := manager.CancelRevision(cmdFlags.Arg(0), selected)
		if err != nil {
			return err
		}
		return printJSON(revision)
	case "review-revision":
		cmdFlags := flag.NewFlagSet("review-revision", flag.ContinueOnError)
		cmdStore := cmdFlags.String("store", storePath, "job state file")
		reviewer := cmdFlags.String("reviewer", "", "assigned reviewer name")
		decision := cmdFlags.String("decision", "", "accept or reject")
		index := cmdFlags.Int("index", -1, "zero-based change index")
		note := cmdFlags.String("note", "", "review note")
		if err := cmdFlags.Parse(rest); err != nil {
			return err
		}
		if cmdFlags.NArg() != 1 || *index < 0 {
			return fmt.Errorf("usage: jobctl review-revision [--store file] --reviewer name --decision accept|reject --index N [--note text] <revision-id>")
		}
		store, err := jobs.OpenStore(*cmdStore)
		if err != nil {
			return err
		}
		defer store.Close()
		report, err := jobs.NewManager(store, registry).ReviewChange(cmdFlags.Arg(0), *index, *reviewer, jobs.ReviewDecision(strings.ToUpper(*decision)), *note)
		if err != nil {
			return err
		}
		return printJSON(report)
	case "resolve-pending":
		cmdFlags := flag.NewFlagSet("resolve-pending", flag.ContinueOnError)
		cmdStore := cmdFlags.String("store", storePath, "job state file")
		status := cmdFlags.String("status", string(jobs.PendingConfirmed), "CONFIRMED or DISMISSED")
		resolution := cmdFlags.String("resolution", "", "human verification note")
		if err := cmdFlags.Parse(rest); err != nil {
			return err
		}
		if cmdFlags.NArg() != 1 {
			return fmt.Errorf("usage: jobctl resolve-pending [--store file] [--status CONFIRMED|DISMISSED] [--resolution text] <pending-id>")
		}
		store, err := jobs.OpenStore(*cmdStore)
		if err != nil {
			return err
		}
		defer store.Close()
		item, err := jobs.NewManager(store, registry).ResolvePending(cmdFlags.Arg(0), jobs.PendingStatus(*status), *resolution)
		if err != nil {
			return err
		}
		return printJSON(item)
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

func extractGlobalStore(args []string, storePath *string) ([]string, error) {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--store" || arg == "-store" {
			if i+1 >= len(args) {
				return nil, fmt.Errorf("--store requires a path")
			}
			*storePath = args[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, "--store=") || strings.HasPrefix(arg, "-store=") {
			*storePath = strings.SplitN(arg, "=", 2)[1]
			continue
		}
		out = append(out, arg)
	}
	return out, nil
}

func usage() {
	fmt.Println(`commands: types, enqueue, list, show, cancel, cancel-group, recover, worker, revisions, propose-revision, preview-revision, show-revision, confirm-revision, cancel-revision, review-revision, resolve-pending, compare-revision`)
}
