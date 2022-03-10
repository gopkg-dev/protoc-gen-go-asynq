package asynqx

import (
	"context"
	"github.com/hibiken/asynq"
)

// ServerOption is an Asynq server option.
type ServerOption func(*Server)

// Logger with server logger.
func Logger(logger asynq.Logger) ServerOption {
	return func(s *Server) {
		s.cfg.Logger = logger
	}
}

// Middleware with service middleware option.
func Middleware(m ...asynq.MiddlewareFunc) ServerOption {
	return func(o *Server) {
		o.ms = m
	}
}

// Concurrency Maximum number of concurrent processing of tasks.
func Concurrency(v int) ServerOption {
	return func(s *Server) {
		s.cfg.Concurrency = v
	}
}

// BaseContext optionally specifies a function that returns the base context for Handler invocations on this server.
//
// If BaseContext is nil, the default is context.Background().
// If this is defined, then it MUST return a non-nil context
func BaseContext(ctx func() context.Context) ServerOption {
	return func(s *Server) {
		s.cfg.BaseContext = ctx
	}
}

// RetryDelayFunc Function to calculate retry delay for a failed task.
//
// By default, it uses exponential backoff algorithm to calculate the delay.
func RetryDelayFunc(f asynq.RetryDelayFunc) ServerOption {
	return func(s *Server) {
		s.cfg.RetryDelayFunc = f
	}
}

// IsFailure Predicate function to determine whether the error returned from Handler is a failure.
// If the function returns false, Server will not increment the retried counter for the task,
// and Server won't record the queue stats (processed and failed stats) to avoid skewing the error
// rate of the queue.
//
// By default, if the given error is non-nil the function returns true.
func IsFailure(f func(error) bool) ServerOption {
	return func(s *Server) {
		s.cfg.IsFailure = f
	}
}

// Queues List of queues to process with given priority value. Keys are the names of the
// queues and values are associated priority value.
//
// If set to nil or not specified, the server will process only the "default" queue.
//
// Priority is treated as follows to avoid starving low priority queues.
//
// Example:
//
//     Queues: map[string]int{
//         "critical": 6,
//         "default":  3,
//         "low":      1,
//     }
//
// With the above config and given that all queues are not empty, the tasks
// in "critical", "default", "low" should be processed 60%, 30%, 10% of
// the time respectively.
//
// If a queue has a zero or negative priority value, the queue will be ignored.
func Queues(v map[string]int) ServerOption {
	return func(s *Server) {
		s.cfg.Queues = v
	}
}

// StrictPriority indicates whether the queue priority should be treated strictly.
//
// If set to true, tasks in the queue with the highest priority is processed first.
// The tasks in lower priority queues are processed only when those queues with
// higher priorities are empty.
func StrictPriority(v bool) ServerOption {
	return func(s *Server) {
		s.cfg.StrictPriority = v
	}
}

// ErrorHandler handles errors returned by the task handler.
//
// HandleError is invoked only if the task handler returns a non-nil error.
//
// Example:
//
//     func reportError(ctx context, task *asynq.Task, err error) {
//         retried, _ := asynq.GetRetryCount(ctx)
//         maxRetry, _ := asynq.GetMaxRetry(ctx)
//     	   if retried >= maxRetry {
//             err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
//     	   }
//         errorReportingService.Notify(err)
//     })
//
//     ErrorHandler: asynq.ErrorHandlerFunc(reportError)
func ErrorHandler(f asynq.ErrorHandlerFunc) ServerOption {
	return func(s *Server) {
		s.cfg.ErrorHandler = f
	}
}

func RedisConnOpt(v asynq.RedisConnOpt) ServerOption {
	return func(s *Server) {
		s.redisOpt = v
	}
}

type Server struct {
	*asynq.Server
	redisOpt asynq.RedisConnOpt
	mux      *asynq.ServeMux
	cfg      asynq.Config
	ms       []asynq.MiddlewareFunc
}

func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		redisOpt: asynq.RedisClientOpt{
			Addr: ":6379",
		},
		mux: asynq.NewServeMux(),
	}
	for _, o := range opts {
		o(srv)
	}
	srv.Server = asynq.NewServer(srv.redisOpt, srv.cfg)
	srv.mux.Use(srv.ms...)
	return srv
}

func (s *Server) Start(ctx context.Context) error {
	return s.Server.Start(s.mux)
}

// Handle registers the handler for the given pattern.
// If a handler already exists for pattern, Handle panics.
func (s *Server) Handle(pattern string, handler asynq.Handler) {
	s.mux.Handle(pattern, handler)
}

// HandleFunc registers the handler function for the given pattern.
func (s *Server) HandleFunc(pattern string, handler asynq.HandlerFunc) {
	s.mux.HandleFunc(pattern, handler)
}

func (s *Server) Stop(ctx context.Context) error {
	s.Server.Stop()
	s.Server.Shutdown()
	return nil
}
