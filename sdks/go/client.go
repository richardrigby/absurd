package absurd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

// Client is the main SDK client for interacting with Absurd
type Client struct {
	db                 *sql.DB
	ownedDB            bool
	queueName          string
	defaultMaxAttempts int
	registry           map[string]*registeredTask
	log                Logger
	worker             Worker
}

// New creates a new Absurd client
func New(options *AbsurdOptions) (*Client, error) {
	if options == nil {
		options = &AbsurdOptions{}
	}

	var db *sql.DB
	var ownedDB bool

	// Handle database connection
	if options.DB == nil {
		// Use environment variable or default
		connStr := os.Getenv("ABSURD_DATABASE_URL")
		if connStr == "" {
			connStr = "postgresql://localhost/absurd"
		}
		var err error
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			return nil, fmt.Errorf("open database: %w", err)
		}
		ownedDB = true
	} else {
		switch v := options.DB.(type) {
		case *sql.DB:
			db = v
			ownedDB = false
		case string:
			var err error
			db, err = sql.Open("postgres", v)
			if err != nil {
				return nil, fmt.Errorf("open database: %w", err)
			}
			ownedDB = true
		default:
			return nil, fmt.Errorf("invalid DB type: must be *sql.DB or connection string")
		}
	}

	// Set default queue name
	queueName := "default"
	if options.QueueName != nil {
		queueName = *options.QueueName
	}

	// Set default max attempts
	defaultMaxAttempts := 5
	if options.DefaultMaxAttempts != nil {
		defaultMaxAttempts = *options.DefaultMaxAttempts
	}

	// Set logger
	var logger Logger = defaultLogger{}
	if options.Log != nil {
		logger = options.Log
	}

	return &Client{
		db:                 db,
		ownedDB:            ownedDB,
		queueName:          queueName,
		defaultMaxAttempts: defaultMaxAttempts,
		registry:           make(map[string]*registeredTask),
		log:                logger,
	}, nil
}

// RegisterTask registers a task handler function
func (c *Client) RegisterTask(options *TaskRegistrationOptions, handler TaskHandler) error {
	if options == nil || options.Name == "" {
		return fmt.Errorf("task registration requires a name")
	}

	if options.DefaultMaxAttempts != nil && *options.DefaultMaxAttempts < 1 {
		return fmt.Errorf("defaultMaxAttempts must be at least 1")
	}

	queue := c.queueName
	if options.Queue != nil {
		queue = *options.Queue
	}
	if queue == "" {
		return fmt.Errorf("task \"%s\" must specify a queue or use a client with a default queue", options.Name)
	}

	c.registry[options.Name] = &registeredTask{
		name:                options.Name,
		queue:               queue,
		defaultMaxAttempts:  options.DefaultMaxAttempts,
		defaultCancellation: options.DefaultCancellation,
		handler:             handler,
	}

	return nil
}

// CreateQueue creates a new queue in the database
func (c *Client) CreateQueue(ctx context.Context, queueName *string) error {
	queue := c.queueName
	if queueName != nil {
		queue = *queueName
	}

	query := `SELECT absurd.create_queue($1)`
	_, err := c.db.ExecContext(ctx, query, queue)
	if err != nil {
		return fmt.Errorf("create queue: %w", err)
	}

	return nil
}

// DropQueue drops an existing queue and all its data
func (c *Client) DropQueue(ctx context.Context, queueName *string) error {
	queue := c.queueName
	if queueName != nil {
		queue = *queueName
	}

	query := `SELECT absurd.drop_queue($1)`
	_, err := c.db.ExecContext(ctx, query, queue)
	if err != nil {
		return fmt.Errorf("drop queue: %w", err)
	}

	return nil
}

// ListQueues returns all available queues
func (c *Client) ListQueues(ctx context.Context) ([]string, error) {
	query := `SELECT * FROM absurd.list_queues()`
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var queueName string
		if err := rows.Scan(&queueName); err != nil {
			return nil, fmt.Errorf("scan queue name: %w", err)
		}
		queues = append(queues, queueName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queues: %w", err)
	}

	return queues, nil
}

// Spawn spawns a new task for execution
func (c *Client) Spawn(ctx context.Context, taskName string, params JsonValue, options *SpawnOptions) (*SpawnResult, error) {
	registration := c.registry[taskName]
	var queue string

	if registration != nil {
		queue = registration.queue
		if options != nil && options.Queue != nil && *options.Queue != registration.queue {
			return nil, fmt.Errorf("task \"%s\" is registered for queue \"%s\" but spawn requested queue \"%s\"", 
				taskName, registration.queue, *options.Queue)
		}
	} else if options == nil || options.Queue == nil {
		return nil, fmt.Errorf("task \"%s\" is not registered. Provide options.Queue when spawning unregistered tasks", taskName)
	} else {
		queue = *options.Queue
	}

	// Determine effective max attempts
	effectiveMaxAttempts := c.defaultMaxAttempts
	if options != nil && options.MaxAttempts != nil {
		effectiveMaxAttempts = *options.MaxAttempts
	} else if registration != nil && registration.defaultMaxAttempts != nil {
		effectiveMaxAttempts = *registration.defaultMaxAttempts
	}

	// Determine effective cancellation
	var effectiveCancellation *CancellationPolicy
	if options != nil && options.Cancellation != nil {
		effectiveCancellation = options.Cancellation
	} else if registration != nil && registration.defaultCancellation != nil {
		effectiveCancellation = registration.defaultCancellation
	}

	// Normalize spawn options
	normalizedOptions := &SpawnOptions{
		MaxAttempts:   &effectiveMaxAttempts,
		RetryStrategy: options.GetRetryStrategy(),
		Headers:       options.GetHeaders(),
		Cancellation:  effectiveCancellation,
	}

	optionsJSON, err := normalizeSpawnOptions(normalizedOptions)
	if err != nil {
		return nil, fmt.Errorf("normalize spawn options: %w", err)
	}

	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal params: %w", err)
	}

	optionsJSONBytes, err := json.Marshal(optionsJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal options: %w", err)
	}

	query := `SELECT task_id, run_id, attempt FROM absurd.spawn_task($1, $2, $3, $4)`
	var taskID, runID string
	var attempt int

	err = c.db.QueryRowContext(ctx, query, queue, taskName, string(paramsJSON), string(optionsJSONBytes)).Scan(&taskID, &runID, &attempt)
	if err != nil {
		return nil, fmt.Errorf("spawn task: %w", err)
	}

	return &SpawnResult{
		TaskID:  taskID,
		RunID:   runID,
		Attempt: attempt,
	}, nil
}

// EmitEvent emits an event from outside of a task
func (c *Client) EmitEvent(ctx context.Context, eventName string, payload JsonValue, queueName *string) error {
	if eventName == "" {
		return fmt.Errorf("eventName must be a non-empty string")
	}

	queue := c.queueName
	if queueName != nil {
		queue = *queueName
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	query := `SELECT absurd.emit_event($1, $2, $3)`
	_, err = c.db.ExecContext(ctx, query, queue, eventName, string(payloadJSON))
	if err != nil {
		return fmt.Errorf("emit event: %w", err)
	}

	return nil
}

// ClaimTasks claims tasks for processing (low-level API)
func (c *Client) ClaimTasks(ctx context.Context, options *ClaimTasksOptions) ([]*ClaimedTask, error) {
	if options == nil {
		options = &ClaimTasksOptions{}
	}

	batchSize := 1
	if options.BatchSize != nil {
		batchSize = *options.BatchSize
	}

	claimTimeout := 120
	if options.ClaimTimeout != nil {
		claimTimeout = *options.ClaimTimeout
	}

	workerID := "worker"
	if options.WorkerID != nil {
		workerID = *options.WorkerID
	}

	query := `
		SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
		       headers, wake_event, event_payload
		FROM absurd.claim_task($1, $2, $3, $4)
	`

	rows, err := c.db.QueryContext(ctx, query, c.queueName, workerID, claimTimeout, batchSize)
	if err != nil {
		return nil, fmt.Errorf("claim tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*ClaimedTask
	for rows.Next() {
		var task ClaimedTask
		var paramsBytes, retryStrategyBytes, headersBytes, eventPayloadBytes []byte
		var maxAttemptsNullable sql.NullInt64
		var wakeEventNullable, eventPayloadNullable sql.NullString

		err := rows.Scan(
			&task.RunID,
			&task.TaskID,
			&task.Attempt,
			&task.TaskName,
			&paramsBytes,
			&retryStrategyBytes,
			&maxAttemptsNullable,
			&headersBytes,
			&wakeEventNullable,
			&eventPayloadNullable,
		)
		if err != nil {
			return nil, fmt.Errorf("scan claimed task: %w", err)
		}

		// Parse JSON fields
		if len(paramsBytes) > 0 {
			if err := json.Unmarshal(paramsBytes, &task.Params); err != nil {
				return nil, fmt.Errorf("unmarshal params: %w", err)
			}
		}

		if len(retryStrategyBytes) > 0 {
			if err := json.Unmarshal(retryStrategyBytes, &task.RetryStrategy); err != nil {
				return nil, fmt.Errorf("unmarshal retry strategy: %w", err)
			}
		}

		if len(headersBytes) > 0 {
			if err := json.Unmarshal(headersBytes, &task.Headers); err != nil {
				return nil, fmt.Errorf("unmarshal headers: %w", err)
			}
		}

		if maxAttemptsNullable.Valid {
			maxAttempts := int(maxAttemptsNullable.Int64)
			task.MaxAttempts = &maxAttempts
		}

		if wakeEventNullable.Valid {
			task.WakeEvent = &wakeEventNullable.String
		}

		if eventPayloadNullable.Valid {
			eventPayloadBytes = []byte(eventPayloadNullable.String)
			if len(eventPayloadBytes) > 0 {
				if err := json.Unmarshal(eventPayloadBytes, &task.EventPayload); err != nil {
					return nil, fmt.Errorf("unmarshal event payload: %w", err)
				}
			}
		}

		tasks = append(tasks, &task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed tasks: %w", err)
	}

	return tasks, nil
}

// WorkBatch processes a single batch of tasks (for manual polling)
func (c *Client) WorkBatch(ctx context.Context, workerID string, claimTimeout, batchSize int) error {
	options := &ClaimTasksOptions{
		WorkerID:     &workerID,
		ClaimTimeout: &claimTimeout,
		BatchSize:    &batchSize,
	}

	tasks, err := c.ClaimTasks(ctx, options)
	if err != nil {
		return fmt.Errorf("claim tasks: %w", err)
	}

	for _, task := range tasks {
		if err := c.executeTask(ctx, task, claimTimeout, nil); err != nil {
			c.log.Printf("Error executing task %s: %v", task.TaskID, err)
		}
	}

	return nil
}

// Close closes the client and any active workers
func (c *Client) Close() error {
	if c.worker != nil {
		if err := c.worker.Close(); err != nil {
			return fmt.Errorf("close worker: %w", err)
		}
	}

	if c.ownedDB {
		if err := c.db.Close(); err != nil {
			return fmt.Errorf("close database: %w", err)
		}
	}

	return nil
}

// executeTask executes a single claimed task
func (c *Client) executeTask(ctx context.Context, task *ClaimedTask, claimTimeout int, options *ExecuteTaskOptions) error {
	registration := c.registry[task.TaskName]
	if registration == nil {
		return fmt.Errorf("unknown task: %s", task.TaskName)
	}

	if registration.queue != c.queueName {
		return fmt.Errorf("misconfigured task (queue mismatch): expected %s, got %s", c.queueName, registration.queue)
	}

	taskCtx, err := NewTaskContext(task.TaskID, c.log, c.db, registration.queue, task, claimTimeout)
	if err != nil {
		return fmt.Errorf("create task context: %w", err)
	}

	// Execute the task handler
	result, err := registration.handler(ctx, task.Params, taskCtx)
	if err != nil {
		// Check if it's a suspend error
		if _, isSuspend := err.(SuspendTaskError); isSuspend {
			// Task suspended (sleep or await), don't complete or fail
			return nil
		}
		
		// Task failed, mark as failed
		if failErr := taskCtx.Fail(ctx, err); failErr != nil {
			return fmt.Errorf("fail task: %w", failErr)
		}
		return nil
	}

	// Task completed successfully
	if err := taskCtx.Complete(ctx, result); err != nil {
		return fmt.Errorf("complete task: %w", err)
	}

	return nil
}

// ClaimTasksOptions provides options for claiming tasks
type ClaimTasksOptions struct {
	BatchSize    *int
	ClaimTimeout *int
	WorkerID     *string
}

// ExecuteTaskOptions provides options for task execution
type ExecuteTaskOptions struct {
	FatalOnLeaseTimeout *bool
}

// Helper methods for SpawnOptions to handle nil safely
func (so *SpawnOptions) GetRetryStrategy() *RetryStrategy {
	if so == nil {
		return nil
	}
	return so.RetryStrategy
}

func (so *SpawnOptions) GetHeaders() JsonObject {
	if so == nil {
		return nil
	}
	return so.Headers
}