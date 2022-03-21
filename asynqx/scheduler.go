package asynqx

import (
	"context"

	"github.com/hibiken/asynq"
)

type Scheduler struct {
	*asynq.Scheduler
}

func NewScheduler(r asynq.RedisConnOpt, opts *asynq.SchedulerOpts) *Scheduler {
	return &Scheduler{
		Scheduler: asynq.NewScheduler(r, opts),
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	return s.Scheduler.Start()
}

func (s *Scheduler) Stop(ctx context.Context) error {
	s.Scheduler.Shutdown()
	return nil
}

func (s *Scheduler) Register(cron string, task *asynq.Task, opts ...asynq.Option) (entryID string, err error) {
	return s.Scheduler.Register(cron, task, opts...)
}

func (s *Scheduler) Unregister(entryID string) error {
	return s.Scheduler.Unregister(entryID)
}
