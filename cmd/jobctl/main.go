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
	replaces := replaceFlags{}
	fs.Var(replaces, "replace", "artifact replacement job-id=new-result (revise, repeatable)")
	withdraws := stringListFlag{}
	fs.Var(&withdraws, "withdraw", "job ID whose artifact is withdrawn (revise, repeatable)")
	only := fs.String("only", "", "comma-separated job IDs to confirm partially (revise-confirm)")
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
	case "revise":
		if len(replaces) == 0 && len(withdraws) == 0 {
			return fmt.Errorf("usage: jobctl revise [--store file] --replace job-id=new-result [--withdraw job-id]")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		var specs []jobs.ChangeSpec
		for jobID, newResult := range replaces {
			specs = append(specs, jobs.ChangeSpec{JobID: jobID, Action: jobs.ActionReplace, NewResult: newResult})
		}
		for _, jobID := range withdraws {
			specs = append(specs, jobs.ChangeSpec{JobID: jobID, Action: jobs.ActionWithdraw})
		}
		rev, err := jobs.NewManager(store, registry).ProposeRevision(specs)
		if err != nil {
			return err
		}
		return printJSON(rev)
	case "revisions":
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		revisions, err := jobs.NewManager(store, registry).ListRevisions()
		if err != nil {
			return err
		}
		return printJSON(revisions)
	case "revise-show":
		if len(rest) != 1 {
			return fmt.Errorf("usage: jobctl revise-show [--store file] <revision-id>")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		rev, err := jobs.NewManager(store, registry).GetRevision(rest[0])
		if err != nil {
			return err
		}
		return printJSON(rev)
	case "revise-confirm":
		if len(rest) != 1 {
			return fmt.Errorf("usage: jobctl revise-confirm [--store file] [--only id1,id2] <revision-id>")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		var onlyIDs []string
		if *only != "" {
			onlyIDs = strings.Split(*only, ",")
		}
		rev, err := jobs.NewManager(store, registry).ConfirmRevision(rest[0], onlyIDs)
		if err != nil {
			return err
		}
		return printJSON(rev)
	case "revise-cancel":
		if len(rest) != 1 {
			return fmt.Errorf("usage: jobctl revise-cancel [--store file] <revision-id>")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		rev, err := jobs.NewManager(store, registry).CancelRevision(rest[0])
		if err != nil {
			return err
		}
		return printJSON(rev)
	case "resolve":
		if len(rest) < 2 {
			return fmt.Errorf("usage: jobctl resolve [--store file] <job-id> <confirmed|failed> [result]")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		confirmed := rest[1] == "confirmed"
		if !confirmed && rest[1] != "failed" {
			return fmt.Errorf("outcome must be confirmed or failed, got %q", rest[1])
		}
		result := ""
		if len(rest) > 2 {
			result = rest[2]
		}
		job, err := jobs.NewManager(store, registry).ResolveExternal(rest[0], confirmed, result)
		if err != nil {
			return err
		}
		return printJSON(job)
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
	fmt.Println(`commands: types, enqueue, list, show, cancel, cancel-group, recover, revise, revisions, revise-show, revise-confirm, revise-cancel, resolve, worker`)
}

type replaceFlags map[string]string

func (f replaceFlags) String() string { return "" }

func (f replaceFlags) Set(value string) error {
	jobID, result, ok := strings.Cut(value, "=")
	if !ok || jobID == "" {
		return fmt.Errorf("--replace expects job-id=new-result, got %q", value)
	}
	f[jobID] = result
	return nil
}

type stringListFlag []string

func (f *stringListFlag) String() string { return strings.Join(*f, ",") }

func (f *stringListFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}
