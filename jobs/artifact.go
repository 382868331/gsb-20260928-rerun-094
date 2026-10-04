package jobs

import (
	"strings"
	"time"
)

// ensureArtifacts backfills generations for records written before revisions
// existed: a succeeded job is treated as generation 1.
func ensureArtifacts(job *Job, now time.Time) {
	if job.Generation == 0 {
		switch {
		case job.ArtifactState == ArtifactRevoked:
		case len(job.Artifacts) > 0:
			job.Generation = job.Artifacts[len(job.Artifacts)-1].Generation
		case job.Status == StatusSucceeded && job.Result != "":
			job.Generation = 1
			job.ArtifactState = ArtifactActive
			job.Artifacts = []Artifact{{
				Generation: 1,
				Result:     job.Result,
				Attempt:    1,
				State:      ArtifactActive,
				Source:     "attempt",
				CreatedAt:  job.UpdatedAt,
			}}
		}
	}
	for i := range job.Artifacts {
		if job.Artifacts[i].State == "" {
			job.Artifacts[i].State = ArtifactActive
		}
	}
	_ = now
}

func activeArtifact(job *Job) *Artifact {
	for i := len(job.Artifacts) - 1; i >= 0; i-- {
		if job.Artifacts[i].State == ArtifactActive {
			a := job.Artifacts[i]
			return &a
		}
	}
	return nil
}

func artifactAtGeneration(job *Job, generation int) *Artifact {
	for i := range job.Artifacts {
		if job.Artifacts[i].Generation == generation {
			a := job.Artifacts[i]
			return &a
		}
	}
	return nil
}

// commitArtifact records a successful execution. Revisions supersede prior
// artifacts; normal attempts only ever add the first generation, because
// later executions of a job only happen after an invalidating revision.
func commitArtifact(job *Job, result string, attempt int, depGenerations map[string]int, revisionIDs []string, now time.Time) {
	gen := 1
	for i := range job.Artifacts {
		if job.Artifacts[i].Generation+1 > gen {
			gen = job.Artifacts[i].Generation + 1
		}
		if job.Artifacts[i].State == ArtifactActive {
			job.Artifacts[i].State = ArtifactSuperseded
		}
	}
	revisionIDs = nonEmptyStrings(revisionIDs...)
	source := "attempt"
	if len(revisionIDs) > 0 {
		source = revisionIDs[0]
	}
	job.Artifacts = append(job.Artifacts, Artifact{
		Generation:         gen,
		Result:             result,
		Attempt:            attempt,
		State:              ArtifactActive,
		Source:             source,
		Sources:            append([]string(nil), revisionIDs...),
		CreatedAt:          now,
		BasedOnGenerations: cloneGenMap(depGenerations),
	})
	job.Generation = gen
	job.ArtifactState = ArtifactActive
	job.Result = result
	job.RevisionNote = ""
	if len(revisionIDs) > 0 {
		job.RevisionNote = "rerun after confirmed " + strings.Join(revisionIDs, ",")
	}
}

func nonEmptyStrings(values ...string) []string {
	out := values[:0]
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return append([]string(nil), out...)
}

func cloneGenMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
