package absurd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// workerImpl implements the Worker interface
type workerImpl struct {
	client        *Client
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	options       *WorkerOptions
	executing     map[string]bool
	executingMu   sync.Mutex
	availability  chan struct{}
}

// StartWorker starts a worker that continuously processes tasks
func (c *Client) StartWorker(ctx context.Context, options *WorkerOptions) (Worker, error) {
	if c.worker != nil {
		return nil, fmt.Errorf("worker already started")
	}

	if options == nil {
		options = &WorkerOptions{}
	}

	// Set defaults
	workerID := fmt.Sprintf("%s:%d", getHostname(), os.Getpid())
	if options.WorkerID != nil {
		workerID = *options.WorkerID
	}

	claimTimeout := 120
	if options.ClaimTimeout != nil {
		claimTimeout = *options.ClaimTimeout
	}

	concurrency := 1
	if options.Concurrency != nil {
		concurrency = *options.Concurrency
	}

	batchSize := concurrency
	if options.BatchSize != nil {
		batchSize = *options.BatchSize
	}

	pollInterval := 0.25
	if options.PollInterval != nil {
		pollInterval = *options.PollInterval
	}

	onError := func(err error) {
		c.log.Printf("Worker error: %v", err)
	}
	if options.OnError != nil {
		onError = options.OnError
	}

	fatalOnLeaseTimeout := true
	if options.FatalOnLeaseTimeout != nil {
		fatalOnLeaseTimeout = *options.FatalOnLeaseTimeout
	}

	// Create worker context
	workerCtx, cancel := context.WithCancel(ctx)

	worker := &workerImpl{
		client:       c,
		ctx:          workerCtx,
		cancel:       cancel,
		options:      options,
		executing:    make(map[string]bool),
		availability: make(chan struct{}, 1),
	}

	// Set effective options
	worker.options = &WorkerOptions{
		WorkerID:             &workerID,
		ClaimTimeout:         &claimTimeout,
		BatchSize:            &batchSize,
		Concurrency:          &concurrency,
		PollInterval:         &pollInterval,
		OnError:              onError,
		FatalOnLeaseTimeout:  &fatalOnLeaseTimeout,
	}

	// Start the worker loop
	worker.wg.Add(1)
	go worker.run()

	c.worker = worker
	return worker, nil
}

// Close shuts down the worker gracefully
func (w *workerImpl) Close() error {
	w.cancel()
	w.wg.Wait()
	return nil
}

// run is the main worker loop
func (w *workerImpl) run() {
	defer w.wg.Done()

	pollDuration := time.Duration(*w.options.PollInterval * float64(time.Second))
	ticker := time.NewTicker(pollDuration)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.poll()
		case <-w.availability:
			w.poll()
		}
	}
}

// poll checks for available tasks and processes them
func (w *workerImpl) poll() {
	w.executingMu.Lock()
	executingCount := len(w.executing)
	w.executingMu.Unlock()

	if executingCount >= *w.options.Concurrency {
		return
	}

	availableCapacity := *w.options.Concurrency - executingCount
	toClaim := *w.options.BatchSize
	if toClaim > availableCapacity {
		toClaim = availableCapacity
	}

	if toClaim <= 0 {
		return
	}

	claimOptions := &ClaimTasksOptions{
		BatchSize:    &toClaim,
		ClaimTimeout: w.options.ClaimTimeout,
		WorkerID:     w.options.WorkerID,
	}

	tasks, err := w.client.ClaimTasks(w.ctx, claimOptions)
	if err != nil {
		w.options.OnError(fmt.Errorf("claim tasks: %w", err))
		return
	}

	if len(tasks) == 0 {
		return
	}

	// Process each task concurrently
	for _, task := range tasks {
		w.wg.Add(1)
		go w.processTask(task)
	}
}

// processTask processes a single task
func (w *workerImpl) processTask(task *ClaimedTask) {
	defer w.wg.Done()
	
	// Track execution
	w.executingMu.Lock()
	w.executing[task.RunID] = true
	w.executingMu.Unlock()

	defer func() {
		w.executingMu.Lock()
		delete(w.executing, task.RunID)
		w.executingMu.Unlock()
		
		// Signal availability
		select {
		case w.availability <- struct{}{}:
		default:
		}
	}()

	// Set up timeout warnings and fatal timeout
	var warnTimer, fatalTimer *time.Timer
	claimTimeout := time.Duration(*w.options.ClaimTimeout) * time.Second
	
	if *w.options.ClaimTimeout > 0 {
		taskLabel := fmt.Sprintf("%s (%s)", task.TaskName, task.TaskID)
		
		warnTimer = time.AfterFunc(claimTimeout, func() {
			w.client.log.Printf("task %s exceeded claim timeout of %ds", taskLabel, *w.options.ClaimTimeout)
		})
		
		if *w.options.FatalOnLeaseTimeout {
			fatalTimer = time.AfterFunc(claimTimeout*2, func() {
				w.client.log.Printf("task %s exceeded claim timeout of %ds by more than 100%%; terminating process", 
					taskLabel, *w.options.ClaimTimeout)
				os.Exit(1)
			})
		}
	}

	// Clean up timers
	defer func() {
		if warnTimer != nil {
			warnTimer.Stop()
		}
		if fatalTimer != nil {
			fatalTimer.Stop()
		}
	}()

	// Execute the task
	executeOptions := &ExecuteTaskOptions{
		FatalOnLeaseTimeout: w.options.FatalOnLeaseTimeout,
	}

	if err := w.client.executeTask(w.ctx, task, *w.options.ClaimTimeout, executeOptions); err != nil {
		w.options.OnError(fmt.Errorf("execute task %s: %w", task.TaskID, err))
	}
}

// notifyAvailability signals that capacity is available
func (w *workerImpl) notifyAvailability() {
	select {
	case w.availability <- struct{}{}:
	default:
	}
}

// getHostname returns the hostname, falling back to "host" if unavailable
func getHostname() string {
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}
	return "host"
}

// generateWorkerID generates a unique worker ID
func generateWorkerID() string {
	return fmt.Sprintf("%s:%s", getHostname(), uuid.New().String()[:8])
}