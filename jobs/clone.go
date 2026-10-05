package jobs

func cloneRevision(in *RevisionCandidate) *RevisionCandidate {
	if in == nil {
		return nil
	}
	out := *in
	out.Changes = make([]*ArtifactChange, len(in.Changes))
	for i := range in.Changes {
		change := *in.Changes[i]
		change.Reviews = make([]ReviewRecord, len(in.Changes[i].Reviews))
		copy(change.Reviews, in.Changes[i].Reviews)
		if in.Changes[i].DecidedAt != nil {
			decided := *in.Changes[i].DecidedAt
			change.DecidedAt = &decided
		}
		out.Changes[i] = &change
	}
	out.Pending = make([]*PendingItem, len(in.Pending))
	for i := range in.Pending {
		out.Pending[i] = clonePending(in.Pending[i])
	}
	return &out
}

func clonePending(in *PendingItem) *PendingItem {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneJob(in *Job) *Job {
	if in == nil {
		return nil
	}
	out := *in
	out.DependsOn = append([]string(nil), in.DependsOn...)
	out.Artifacts = make([]Artifact, len(in.Artifacts))
	for i := range in.Artifacts {
		artifact := in.Artifacts[i]
		artifact.BasedOnGenerations = cloneGenMap(artifact.BasedOnGenerations)
		artifact.Sources = append([]string(nil), artifact.Sources...)
		out.Artifacts[i] = artifact
	}
	out.ExternalPendingIDs = append([]string(nil), in.ExternalPendingIDs...)
	out.PendingRevisionIDs = append([]string(nil), in.PendingRevisionIDs...)
	if in.NextRunAt != nil {
		next := *in.NextRunAt
		out.NextRunAt = &next
	}
	return &out
}

func cloneJobMap(in map[string]*Job) map[string]*Job {
	out := make(map[string]*Job, len(in))
	for id, job := range in {
		out[id] = cloneJob(job)
	}
	return out
}

func cloneAttempt(in *Attempt) *Attempt {
	if in == nil {
		return nil
	}
	out := *in
	out.DepResults = cloneStringMap(in.DepResults)
	out.DepGenerations = cloneGenMap(in.DepGenerations)
	out.RevisionIDs = append([]string(nil), in.RevisionIDs...)
	if in.FinishedAt != nil {
		finished := *in.FinishedAt
		out.FinishedAt = &finished
	}
	return &out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range in {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func removeString(values []string, remove string) []string {
	out := values[:0]
	for _, value := range values {
		if value != remove {
			out = append(out, value)
		}
	}
	return out
}
