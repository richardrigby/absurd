package absurd

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// JsonValue represents any JSON-serializable value
type JsonValue interface{}

// JsonObject represents a JSON object (map)
type JsonObject map[string]JsonValue

// RetryStrategy defines how failed tasks should be retried
type RetryStrategy struct {
	Kind        string  `json:"kind"`        // "fixed", "exponential", or "none"
	BaseSeconds *int    `json:"baseSeconds,omitempty"`
	Factor      *int    `json:"factor,omitempty"`
	MaxSeconds  *int    `json:"maxSeconds,omitempty"`
}

// CancellationPolicy defines automatic task cancellation conditions
type CancellationPolicy struct {
	MaxDuration *int `json:"maxDuration,omitempty"` // Maximum total task runtime (seconds)
	MaxDelay    *int `json:"maxDelay,omitempty"`    // Maximum delay before first execution (seconds)
}

// SpawnOptions provides options when spawning a task
type SpawnOptions struct {
	MaxAttempts     *int                 `json:"maxAttempts,omitempty"`
	RetryStrategy   *RetryStrategy       `json:"retryStrategy,omitempty"`
	Headers         JsonObject           `json:"headers,omitempty"`
	Queue           *string              `json:"queue,omitempty"`
	Cancellation    *CancellationPolicy  `json:"cancellation,omitempty"`
}

// TaskRegistrationOptions provides options when registering a task
type TaskRegistrationOptions struct {
	Name                  string
	Queue                 *string
	DefaultMaxAttempts    *int
	DefaultCancellation   *CancellationPolicy
}

// SpawnResult contains the result of spawning a task
type SpawnResult struct {
	TaskID  string `json:"taskID"`
	RunID   string `json:"runID"`
	Attempt int    `json:"attempt"`
}

// ClaimedTask represents a task claimed by a worker
type ClaimedTask struct {
	RunID         string     `json:"runId"`
	TaskID        string     `json:"taskId"`
	TaskName      string     `json:"taskName"`
	Attempt       int        `json:"attempt"`
	Params        JsonValue  `json:"params"`
	RetryStrategy JsonValue  `json:"retryStrategy"`
	MaxAttempts   *int       `json:"maxAttempts"`
	Headers       JsonObject `json:"headers"`
	WakeEvent     *string    `json:"wakeEvent"`
	EventPayload  JsonValue  `json:"eventPayload"`
}

// WorkerOptions configures worker behavior
type WorkerOptions struct {
	WorkerID             *string
	ClaimTimeout         *int     // seconds, default: 120
	BatchSize            *int
	Concurrency          *int     // default: 1
	PollInterval         *float64 // seconds, default: 0.25
	OnError              func(error)
	FatalOnLeaseTimeout  *bool    // default: true
}

// Worker represents an active worker instance
type Worker interface {
	Close() error
}

// TaskHandler defines the signature for task handler functions
type TaskHandler func(ctx context.Context, params JsonValue, taskCtx *TaskContext) (JsonValue, error)

// AbsurdOptions configures the Absurd client
type AbsurdOptions struct {
	DB                   interface{} // *sql.DB, *sql.Conn, or connection string
	QueueName            *string
	DefaultMaxAttempts   *int
	Log                  Logger
}

// Logger interface for custom logging
type Logger interface {
	Printf(format string, v ...interface{})
	Print(v ...interface{})
}

// defaultLogger wraps the standard log package
type defaultLogger struct{}

func (d defaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (d defaultLogger) Print(v ...interface{}) {
	log.Print(v...)
}

// SuspendTaskError is thrown internally to suspend a task
type SuspendTaskError struct{}

func (e SuspendTaskError) Error() string {
	return "task suspended"
}

// TimeoutError is thrown when awaiting an event times out
type TimeoutError struct {
	Message string
}

func (e TimeoutError) Error() string {
	return e.Message
}

// NewTimeoutError creates a new TimeoutError
func NewTimeoutError(message string) *TimeoutError {
	return &TimeoutError{Message: message}
}

// AwaitEventOptions provides options for awaiting events
type AwaitEventOptions struct {
	StepName *string
	Timeout  *int // seconds
}

// registeredTask holds information about a registered task
type registeredTask struct {
	name                string
	queue               string
	defaultMaxAttempts  *int
	defaultCancellation *CancellationPolicy
	handler             TaskHandler
}

// checkpointRow represents a checkpoint record from the database
type checkpointRow struct {
	CheckpointName string    `db:"checkpoint_name"`
	State          JsonValue `db:"state"`
	Status         string    `db:"status"`
	OwnerRunID     string    `db:"owner_run_id"`
	UpdatedAt      time.Time `db:"updated_at"`
}

// serializeError converts an error to a JSON-serializable format
func serializeError(err error) JsonValue {
	if err == nil {
		return nil
	}
	return JsonObject{
		"message": err.Error(),
	}
}

// normalizeSpawnOptions converts SpawnOptions to a database-compatible format
func normalizeSpawnOptions(options *SpawnOptions) (JsonObject, error) {
	if options == nil {
		return JsonObject{}, nil
	}

	normalized := JsonObject{}
	
	if options.Headers != nil {
		normalized["headers"] = options.Headers
	}
	
	if options.MaxAttempts != nil {
		normalized["max_attempts"] = *options.MaxAttempts
	}
	
	if options.RetryStrategy != nil {
		serialized, err := serializeRetryStrategy(options.RetryStrategy)
		if err != nil {
			return nil, fmt.Errorf("serialize retry strategy: %w", err)
		}
		normalized["retry_strategy"] = serialized
	}
	
	if options.Cancellation != nil {
		cancellation, err := normalizeCancellation(options.Cancellation)
		if err != nil {
			return nil, fmt.Errorf("normalize cancellation: %w", err)
		}
		if cancellation != nil {
			normalized["cancellation"] = cancellation
		}
	}
	
	return normalized, nil
}

// serializeRetryStrategy converts RetryStrategy to database format
func serializeRetryStrategy(strategy *RetryStrategy) (JsonObject, error) {
	if strategy == nil {
		return nil, nil
	}
	
	serialized := JsonObject{
		"kind": strategy.Kind,
	}
	
	if strategy.BaseSeconds != nil {
		serialized["base_seconds"] = *strategy.BaseSeconds
	}
	
	if strategy.Factor != nil {
		serialized["factor"] = *strategy.Factor
	}
	
	if strategy.MaxSeconds != nil {
		serialized["max_seconds"] = *strategy.MaxSeconds
	}
	
	return serialized, nil
}

// normalizeCancellation converts CancellationPolicy to database format
func normalizeCancellation(policy *CancellationPolicy) (JsonObject, error) {
	if policy == nil {
		return nil, nil
	}
	
	normalized := JsonObject{}
	
	if policy.MaxDuration != nil {
		normalized["max_duration"] = *policy.MaxDuration
	}
	
	if policy.MaxDelay != nil {
		normalized["max_delay"] = *policy.MaxDelay
	}
	
	if len(normalized) == 0 {
		return nil, nil
	}
	
	return normalized, nil
}

// JsonValueWrapper wraps JsonValue to implement driver.Valuer
type JsonValueWrapper struct {
	Data JsonValue
}

// Value implements the driver.Valuer interface
func (j JsonValueWrapper) Value() (driver.Value, error) {
	if j.Data == nil {
		return nil, nil
	}
	return json.Marshal(j.Data)
}