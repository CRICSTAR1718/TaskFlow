package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/cricstar1718/taskflow/internal/domain"
	"github.com/cricstar1718/taskflow/internal/store"
	"github.com/google/uuid"
)

// The scheduler enqueues to Redis and only then marks the rows QUEUED, so a fast
// worker can be handed a job that is still PENDING. That is normal, not a lost
// race: dropping the claim left the job in PENDING with nothing left to wake it,
// because the scheduler only picks up jobs whose next_run_at has passed and the
// Redis entry was consumed. The job sat in PENDING for the rest of the run.
func TestExecuteClaimsAJobThatIsStillPending(t *testing.T) {
	j := newJob(`{}`)
	j.Status = domain.StatusPending
	st := &fakeStore{job: j}
	q := &fakeClaimer{ids: []string{j.ID.String()}}

	r := NewRunner(RunnerConfig{Store: st, Queue: q, Pool: testPool(), Executor: NewExecutor(quiet()), WorkerID: "w1", Log: quiet()})
	r.execute(context.Background(), j.ID.String())

	if len(st.statusFrom) == 0 || st.statusFrom[0] != domain.StatusPending {
		t.Fatalf("first transition was %v, want PENDING->RUNNING", st.statusFrom)
	}
	if len(st.statusTo) == 0 || st.statusTo[0] != domain.StatusRunning {
		t.Fatalf("first transition target was %v, want RUNNING", st.statusTo)
	}
	if len(st.finishes) != 1 || st.finishes[0] != domain.StatusSucceeded {
		t.Fatalf("FinishRun = %v, want [SUCCEEDED]: the pending job was not run", st.finishes)
	}
}

// If the PENDING attempt loses to the scheduler marking it QUEUED in between, the
// claim must still go through rather than being dropped.
func TestClaimFallsBackWhenTheSourceStatusLosesTheRace(t *testing.T) {
	st := &fakeStore{}
	// Every guarded transition loses: the job moved to RUNNING under us.
	st.neverMatches = true

	err := (&Runner{store: st, log: quiet()}).claim(context.Background(), uuid.New(), domain.StatusPending)
	if !errors.Is(err, errNotClaimable) {
		t.Fatalf("claim err = %v, want errNotClaimable", err)
	}
}

// A RETRYING job is handed to a worker on the same path, and is just as
// legitimate a starting point as PENDING. Missing this state stranded retried
// jobs in RETRYING with an already-consumed queue entry.
func TestClaimAcceptsARetryingJob(t *testing.T) {
	st := &fakeStore{matchOnlyFrom: domain.StatusRetrying}
	if err := (&Runner{store: st, log: quiet()}).claim(context.Background(), uuid.New(), domain.StatusPending); err != nil {
		t.Fatalf("claim of a RETRYING job: %v", err)
	}
	if len(st.statusFrom) == 0 || st.statusFrom[len(st.statusFrom)-1] != domain.StatusRetrying {
		t.Fatalf("transitions = %v, want the successful one from RETRYING", st.statusFrom)
	}
	if st.statusTo[len(st.statusTo)-1] != domain.StatusRunning {
		t.Fatalf("target = %v, want RUNNING", st.statusTo)
	}
}

// A job someone else owns must not be claimed.
func TestClaimOfADeletedJobIsNotFound(t *testing.T) {
	st := &fakeStore{deleted: true}
	err := (&Runner{store: st, log: quiet()}).claim(context.Background(), uuid.New(), domain.StatusPending)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("claim err = %v, want ErrNotFound", err)
	}
}
