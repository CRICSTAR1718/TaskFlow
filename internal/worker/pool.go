package worker

import (
	"context"
	"log/slog"
	"sync"
)

// Pool bounds concurrent job execution.
type Pool struct {
	size int
	exec *Executor
	log  *slog.Logger
	jobs chan Task
}

// NewPool creates a pool of size concurrent workers.
func NewPool(size int, exec *Executor, log *slog.Logger) *Pool {
	if size < 1 {
		size = 1
	}
	return &Pool{size: size, exec: exec, log: log, jobs: make(chan Task)}
}

// Submit hands a task to the pool, blocking until a goroutine is free.
func (p *Pool) Submit(ctx context.Context, t Task) error {
	select {
	case p.jobs <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts the goroutines and blocks until ctx is cancelled and all running
// tasks finish. TODO: add the claim loop that feeds Submit from the queue.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-p.jobs:
					if err := p.exec.Execute(ctx, t); err != nil {
						p.log.Warn("job failed", slog.String("job_id", t.JobID), slog.Any("error", err))
					}
				}
			}
		}()
	}
	wg.Wait()
}
