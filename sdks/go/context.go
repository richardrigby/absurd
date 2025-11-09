package absurd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// TaskContext provides execution context and utilities within task handlers
type TaskContext struct {
	TaskID           string
	stepNameCounter  map[string]int
	log              Logger
	db               *sql.DB
	queueName        string
	task             *ClaimedTask
	checkpointCache  map[string]JsonValue
	claimTimeout     int
}

// NewTaskContext creates a new TaskContext instance
func NewTaskContext(
	taskID string,
	log Logger,
	db *sql.DB,
	queueName string,
	task *ClaimedTask,
	claimTimeout int,
) (*TaskContext, error) {
	ctx := &TaskContext{
		TaskID:          taskID,
		stepNameCounter: make(map[string]int),
		log:             log,
		db:              db,
		queueName:       queueName,
		task:            task,
		checkpointCache: make(map[string]JsonValue),
		claimTimeout:    claimTimeout,
	}

	// Load existing checkpoints
	if err := ctx.loadCheckpoints(); err != nil {
		return nil, fmt.Errorf("load checkpoints: %w", err)
	}

	return ctx, nil
}

// Step defines a checkpointed step within the task. Steps are idempotent.
func (tc *TaskContext) Step(ctx context.Context, name string, fn func() (JsonValue, error)) (JsonValue, error) {
	checkpointName := tc.getCheckpointName(name)
	
	// Check if we already have a cached result
	if cached, exists := tc.checkpointCache[checkpointName]; exists {
		return cached, nil
	}

	// Check database for existing checkpoint
	state, err := tc.lookupCheckpoint(ctx, checkpointName)
	if err != nil {
		return nil, fmt.Errorf("lookup checkpoint: %w", err)
	}
	if state != nil {
		tc.checkpointCache[checkpointName] = state
		return state, nil
	}

	// Execute the step function
	result, err := fn()
	if err != nil {
		return nil, err
	}

	// Persist the checkpoint
	if err := tc.persistCheckpoint(ctx, checkpointName, result); err != nil {
		return nil, fmt.Errorf("persist checkpoint: %w", err)
	}

	return result, nil
}

// SleepFor suspends the task for a specified duration in seconds
func (tc *TaskContext) SleepFor(ctx context.Context, stepName string, duration int) error {
	wakeAt := time.Now().Add(time.Duration(duration) * time.Second)
	return tc.SleepUntil(ctx, stepName, wakeAt)
}

// SleepUntil suspends the task until a specific timestamp
func (tc *TaskContext) SleepUntil(ctx context.Context, stepName string, wakeAt time.Time) error {
	checkpointName := tc.getCheckpointName(stepName)
	
	// Check for existing checkpoint
	state, err := tc.lookupCheckpoint(ctx, checkpointName)
	if err != nil {
		return fmt.Errorf("lookup checkpoint: %w", err)
	}

	var actualWakeAt time.Time
	if state != nil {
		// Parse the stored wake time
		if timeStr, ok := state.(string); ok {
			if parsed, err := time.Parse(time.RFC3339, timeStr); err == nil {
				actualWakeAt = parsed
			} else {
				actualWakeAt = wakeAt
			}
		} else {
			actualWakeAt = wakeAt
		}
	} else {
		actualWakeAt = wakeAt
		// Store the wake time as checkpoint
		if err := tc.persistCheckpoint(ctx, checkpointName, actualWakeAt.Format(time.RFC3339)); err != nil {
			return fmt.Errorf("persist sleep checkpoint: %w", err)
		}
	}

	// If it's still in the future, suspend
	if time.Now().Before(actualWakeAt) {
		if err := tc.scheduleRun(ctx, actualWakeAt); err != nil {
			return fmt.Errorf("schedule run: %w", err)
		}
		return SuspendTaskError{}
	}

	return nil
}

// AwaitEvent waits for an event to be emitted. Events are cached and race-free.
func (tc *TaskContext) AwaitEvent(ctx context.Context, eventName string, options *AwaitEventOptions) (JsonValue, error) {
	if eventName == "" {
		return nil, fmt.Errorf("eventName must be a non-empty string")
	}

	stepName := fmt.Sprintf("$awaitEvent:%s", eventName)
	var timeout *int
	
	if options != nil {
		if options.StepName != nil {
			stepName = *options.StepName
		}
		if options.Timeout != nil && *options.Timeout >= 0 {
			timeout = options.Timeout
		}
	}

	checkpointName := tc.getCheckpointName(stepName)
	
	// Check for cached result
	if cached, exists := tc.checkpointCache[checkpointName]; exists {
		return cached, nil
	}

	// Check if we woke up due to timeout
	if tc.task.WakeEvent != nil && *tc.task.WakeEvent == eventName && tc.task.EventPayload == nil {
		return nil, NewTimeoutError(fmt.Sprintf("Timed out waiting for event \"%s\"", eventName))
	}

	// Call database function to await event
	query := `SELECT should_suspend, payload FROM absurd.await_event($1, $2, $3, $4, $5, $6)`
	var shouldSuspend bool
	var payloadBytes []byte
	
	err := tc.db.QueryRowContext(ctx, query,
		tc.queueName,
		tc.task.TaskID,
		tc.task.RunID,
		checkpointName,
		eventName,
		timeout,
	).Scan(&shouldSuspend, &payloadBytes)
	
	if err != nil {
		return nil, fmt.Errorf("await event query failed: %w", err)
	}

	if !shouldSuspend {
		// Event was available, parse payload and cache it
		var payload JsonValue
		if len(payloadBytes) > 0 {
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				return nil, fmt.Errorf("unmarshal event payload: %w", err)
			}
		}
		
		tc.checkpointCache[checkpointName] = payload
		return payload, nil
	}

	// Need to suspend
	return nil, SuspendTaskError{}
}

// EmitEvent emits an event from within a task
func (tc *TaskContext) EmitEvent(ctx context.Context, eventName string, payload JsonValue) error {
	if eventName == "" {
		return fmt.Errorf("eventName must be a non-empty string")
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	query := `SELECT absurd.emit_event($1, $2, $3)`
	_, err = tc.db.ExecContext(ctx, query, tc.queueName, eventName, string(payloadJSON))
	if err != nil {
		return fmt.Errorf("emit event: %w", err)
	}

	return nil
}

// Complete marks the task as completed with an optional result
func (tc *TaskContext) Complete(ctx context.Context, result JsonValue) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	query := `SELECT absurd.complete_run($1, $2, $3)`
	_, err = tc.db.ExecContext(ctx, query, tc.queueName, tc.task.RunID, string(resultJSON))
	if err != nil {
		return fmt.Errorf("complete run: %w", err)
	}

	return nil
}

// Fail marks the task as failed with the given error
func (tc *TaskContext) Fail(ctx context.Context, taskErr error) error {
	tc.log.Printf("[absurd] task execution failed: %v", taskErr)
	
	errorJSON, err := json.Marshal(serializeError(taskErr))
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	query := `SELECT absurd.fail_run($1, $2, $3, $4)`
	_, err = tc.db.ExecContext(ctx, query, tc.queueName, tc.task.RunID, string(errorJSON), nil)
	if err != nil {
		return fmt.Errorf("fail run: %w", err)
	}

	return nil
}

// getCheckpointName generates a unique checkpoint name handling duplicates
func (tc *TaskContext) getCheckpointName(name string) string {
	count, exists := tc.stepNameCounter[name]
	if !exists {
		count = 0
	}
	count++
	tc.stepNameCounter[name] = count

	if count == 1 {
		return name
	}
	return fmt.Sprintf("%s#%d", name, count)
}

// loadCheckpoints loads existing checkpoints from the database
func (tc *TaskContext) loadCheckpoints() error {
	query := `
		SELECT checkpoint_name, state, status, owner_run_id, updated_at
		FROM absurd.get_task_checkpoint_states($1, $2, $3)
	`
	
	rows, err := tc.db.Query(query, tc.queueName, tc.task.TaskID, tc.task.RunID)
	if err != nil {
		return fmt.Errorf("query checkpoints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var checkpointName string
		var stateBytes []byte
		var status string
		var ownerRunID string
		var updatedAt time.Time

		if err := rows.Scan(&checkpointName, &stateBytes, &status, &ownerRunID, &updatedAt); err != nil {
			return fmt.Errorf("scan checkpoint: %w", err)
		}

		var state JsonValue
		if len(stateBytes) > 0 {
			if err := json.Unmarshal(stateBytes, &state); err != nil {
				return fmt.Errorf("unmarshal checkpoint state: %w", err)
			}
		}

		tc.checkpointCache[checkpointName] = state
	}

	return rows.Err()
}

// lookupCheckpoint retrieves a checkpoint from the database
func (tc *TaskContext) lookupCheckpoint(ctx context.Context, checkpointName string) (JsonValue, error) {
	query := `
		SELECT checkpoint_name, state, status, owner_run_id, updated_at
		FROM absurd.get_task_checkpoint_state($1, $2, $3)
	`
	
	var name string
	var stateBytes []byte
	var status string
	var ownerRunID string
	var updatedAt time.Time

	err := tc.db.QueryRowContext(ctx, query, tc.queueName, tc.task.TaskID, checkpointName).Scan(
		&name, &stateBytes, &status, &ownerRunID, &updatedAt)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query checkpoint: %w", err)
	}

	var state JsonValue
	if len(stateBytes) > 0 {
		if err := json.Unmarshal(stateBytes, &state); err != nil {
			return nil, fmt.Errorf("unmarshal checkpoint state: %w", err)
		}
	}

	return state, nil
}

// persistCheckpoint saves a checkpoint to the database
func (tc *TaskContext) persistCheckpoint(ctx context.Context, checkpointName string, value JsonValue) error {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal checkpoint value: %w", err)
	}

	query := `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`
	_, err = tc.db.ExecContext(ctx, query,
		tc.queueName,
		tc.task.TaskID,
		checkpointName,
		string(valueJSON),
		tc.task.RunID,
		tc.claimTimeout,
	)
	
	if err != nil {
		return fmt.Errorf("set checkpoint: %w", err)
	}

	// Cache the value
	tc.checkpointCache[checkpointName] = value
	return nil
}

// scheduleRun schedules the task to run at a specific time
func (tc *TaskContext) scheduleRun(ctx context.Context, wakeAt time.Time) error {
	query := `SELECT absurd.schedule_run($1, $2, $3)`
	_, err := tc.db.ExecContext(ctx, query, tc.queueName, tc.task.RunID, wakeAt)
	if err != nil {
		return fmt.Errorf("schedule run: %w", err)
	}
	return nil
}