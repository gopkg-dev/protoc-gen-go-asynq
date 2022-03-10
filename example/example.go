package example

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/amzapi/protoc-gen-go-asynq/asynqx"
	"github.com/hibiken/asynq"
)

func TestServer(t *testing.T) {
	ctx := context.Background()
	s := asynqx.NewServer(
		asynqx.Concurrency(10),
		asynqx.ErrorHandler(func(ctx context.Context, task *asynq.Task, err error) {
			// .......
		}),
		asynqx.Queues(map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		}),
		asynqx.IsFailure(func(err error) bool {
			return !IsRateLimitError(err)
		}),
		asynqx.RetryDelayFunc(func(n int, err error, task *asynq.Task) time.Duration {
			var ratelimitErr *RateLimitError
			if errors.As(err, &ratelimitErr) {
				return ratelimitErr.RetryIn
			}
			return asynq.DefaultRetryDelayFunc(n, err, task)
		}),
		asynqx.Middleware(
			func(next asynq.Handler) asynq.Handler {
				return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
					return next.ProcessTask(ctx, t)
				})
			},
		),
		asynqx.BaseContext(func() context.Context {
			return context.WithValue(ctx, "test", "123456")
		}),
	)
	RegisterUserTaskServer(s, new(userTask))
	if err := s.Start(ctx); err != nil {
		panic(err)
	}
}

type userTask struct {
}

func (u *userTask) CreateUser(ctx context.Context, payload *CreateUserPayload) error {
	log.Printf("[*] context value %s", ctx.Value("test"))
	if !limiter.Allow() {
		return &RateLimitError{
			RetryIn: time.Duration(rand.Intn(10)) * time.Second,
		}
	}
	log.Printf("[*] processing %s", payload)
	return nil
}

func (u *userTask) UpdateUser(ctx context.Context, payload *UpdateUserPayload) error {
	// .......
	return nil
}

// Rate is 10 events/sec and permits burst of at most 30 events.
var limiter = rate.NewLimiter(10, 30)

type RateLimitError struct {
	RetryIn time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited (retry in  %v)", e.RetryIn)
}

func IsRateLimitError(err error) bool {
	_, ok := err.(*RateLimitError)
	return ok
}
