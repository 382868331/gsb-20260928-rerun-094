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
	note := fs.String("note", "", "revision note (propose)")
	var changeFlags, externalFlags stringList
	fs.Var(&changeFlags, "change", "revision change as job:replace:<new-result> or job:revoke (repeatable)")
	fs.Var(&externalFlags, "external", "unverified external effect as job:summary (repeatable, propose)")
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
	case "propose":
		if len(changeFlags) == 0 {
			return fmt.Errorf("usage: jobctl propose [--store file] [--note text] --change job:replace:<result>|job:revoke [--external job:summary]...")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		externals := map[string][]string{}
		for _, spec := range externalFlags {
			jobID, summary, ok := strings.Cut(spec, ":")
			if !ok || jobID == "" || summary == "" {
				return fmt.Errorf("invalid --external %q, want job:summary", spec)
			}
			externals[jobID] = append(externals[jobID], summary)
		}
		var specs []jobs.ChangeSpec
		for _, raw := range changeFlags {
			spec, err := parseChangeSpec(raw, externals)
			if err != nil {
				return err
			}
			specs = append(specs, spec)
		}
		candidate, err := jobs.NewManager(store, registry).ProposeRevision(specs, *note)
		if err != nil {
			return err
		}
		return printJSON(candidate)
	case "revisions":
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		list, err := jobs.NewManager(store, registry).ListRevisions()
		if err != nil {
			return err
		}
		return printJSON(list)
	case "preview-revision":
		if len(rest) != 1 {
			return fmt.Errorf("usage: jobctl preview-revision [--store file] <revision-id>")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		preview, err := jobs.NewManager(store, registry).PreviewRevision(rest[0])
		if err != nil {
			return err
		}
		return printJSON(preview)
	case "confirm-revision":
		if len(rest) < 1 {
			return fmt.Errorf("usage: jobctl confirm-revision [--store file] <revision-id> [change-index...]")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		indexes, err := parseIndexes(rest[1:])
		if err != nil {
			return err
		}
		report, err := jobs.NewManager(store, registry).ConfirmRevision(rest[0], indexes)
		if err != nil {
			return err
		}
		return printJSON(report)
	case "cancel-revision":
		if len(rest) < 1 {
			return fmt.Errorf("usage: jobctl cancel-revision [--store file] <revision-id> [change-index...]")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		indexes, err := parseIndexes(rest[1:])
		if err != nil {
			return err
		}
		candidate, err := jobs.NewManager(store, registry).CancelRevision(rest[0], indexes)
		if err != nil {
			return err
		}
		return printJSON(candidate)
	case "resolve-pending":
		if len(rest) < 2 {
			return fmt.Errorf("usage: jobctl resolve-pending [--store file] <pending-id> <confirm|dismiss> [resolution]")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		var status jobs.PendingStatus
		switch rest[1] {
		case "confirm":
			status = jobs.PendingConfirmed
		case "dismiss":
			status = jobs.PendingDismissed
		default:
			return fmt.Errorf("resolution must be confirm or dismiss, got %q", rest[1])
		}
		resolution := ""
		if len(rest) > 2 {
			resolution = strings.Join(rest[2:], " ")
		}
		candidate, err := jobs.NewManager(store, registry).ResolvePending(rest[0], status, resolution)
		if err != nil {
			return err
		}
		return printJSON(candidate)
	case "diff-revision":
		if len(rest) != 1 {
			return fmt.Errorf("usage: jobctl diff-revision [--store file] <revision-id>")
		}
		store, err := jobs.OpenStore(*storePath)
		if err != nil {
			return err
		}
		defer store.Close()
		diff, err := jobs.NewManager(store, registry).RevisionDiff(rest[0])
		if err != nil {
			return err
		}
		return printJSON(diff)
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
	fmt.Println(`commands: types, enqueue, list, show, cancel, cancel-group, recover, worker,
  propose, revisions, preview-revision, confirm-revision, cancel-revision,
  resolve-pending, diff-revision`)
}

type stringList []string

func (l *stringList) String() string { return strings.Join(*l, ",") }

func (l *stringList) Set(value string) error {
	*l = append(*l, value)
	return nil
}

func parseChangeSpec(raw string, externals map[string][]string) (jobs.ChangeSpec, error) {
	jobID, after, _ := strings.Cut(raw, ":")
	kind, rest, _ := strings.Cut(after, ":")
	spec := jobs.ChangeSpec{JobID: jobID, ExternalEffects: externals[jobID]}
	switch kind {
	case "replace":
		if rest == "" {
			return spec, fmt.Errorf("invalid --change %q, want job:replace:<new-result>", raw)
		}
		spec.Kind = jobs.ChangeReplace
		spec.NewResult = rest
	case "revoke":
		spec.Kind = jobs.ChangeRevoke
	default:
		return spec, fmt.Errorf("invalid --change %q, want job:replace:<new-result> or job:revoke", raw)
	}
	return spec, nil
}

func parseIndexes(args []string) ([]int, error) {
	if len(args) == 0 {
		return nil, nil
	}
	indexes := make([]int, 0, len(args))
	for _, arg := range args {
		var idx int
		if _, err := fmt.Sscanf(arg, "%d", &idx); err != nil {
			return nil, fmt.Errorf("invalid change index %q", arg)
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}
