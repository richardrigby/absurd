# Absurd Go SDK

The Absurd Go SDK provides a complete interface for building durable, fault-tolerant workflows using PostgreSQL as the backend.

## Installation

```bash
go get github.com/earendil-works/absurd/sdks/go
```

The SDK requires PostgreSQL with the Absurd schema installed and Go 1.21+.

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/earendil-works/absurd/sdks/go"
)

func main() {
	// Create client
	client, err := absurd.New(&absurd.AbsurdOptions{
		DB: "postgresql://localhost/mydb",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Register a task
	err = client.RegisterTask(&absurd.TaskRegistrationOptions{
		Name: "hello-world",
	}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
		result, err := taskCtx.Step(ctx, "greet", func() (absurd.JsonValue, error) {
			return fmt.Sprintf("Hello, %v!", params), nil
		})
		return result, err
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start a worker
	worker, err := client.StartWorker(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer worker.Close()

	// Spawn a task
	result, err := client.Spawn(context.Background(), "hello-world", "World", nil)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("Spawned task: %s\n", result.TaskID)
}
```

## API Reference

### Client

The main SDK client for interacting with Absurd.

#### `New(options *AbsurdOptions) (*Client, error)`

Creates a new Absurd client.

**Options:**
- `DB` - Database connection (*sql.DB or connection string)
- `QueueName` - Default queue name (default: "default")
- `DefaultMaxAttempts` - Default retry attempts (default: 5)
- `Log` - Custom logger (default: standard log package)

#### `RegisterTask(options *TaskRegistrationOptions, handler TaskHandler) error`

Registers a task handler function.

**TaskRegistrationOptions:**
- `Name` - Unique task name
- `Queue` - Queue name (optional, defaults to client's queue)
- `DefaultMaxAttempts` - Default retry attempts for this task
- `DefaultCancellation` - Default cancellation policy

**TaskHandler signature:**
```go
func(ctx context.Context, params JsonValue, taskCtx *TaskContext) (JsonValue, error)
```

#### `Spawn(ctx context.Context, taskName string, params JsonValue, options *SpawnOptions) (*SpawnResult, error)`

Spawns a new task for execution.

**SpawnOptions:**
- `MaxAttempts` - Override retry attempts
- `RetryStrategy` - Retry backoff strategy
- `Headers` - Metadata attached to task
- `Queue` - Target queue (for unregistered tasks)
- `Cancellation` - Cancellation timeouts

**Returns:** `SpawnResult` with `TaskID`, `RunID`, and `Attempt`

#### `StartWorker(ctx context.Context, options *WorkerOptions) (Worker, error)`

Starts a worker that continuously processes tasks.

**WorkerOptions:**
- `WorkerID` - Worker identifier
- `ClaimTimeout` - Task claim timeout in seconds (default: 120)
- `BatchSize` - Tasks to claim per batch
- `Concurrency` - Parallel task execution limit (default: 1)
- `PollInterval` - Polling frequency in seconds (default: 0.25)
- `OnError` - Error handler function
- `FatalOnLeaseTimeout` - Exit process on claim timeout (default: true)

#### `EmitEvent(ctx context.Context, eventName string, payload JsonValue, queueName *string) error`

Emits an event that tasks can await.

#### `CreateQueue(ctx context.Context, queueName *string) error`

Creates a new queue in the database.

#### `DropQueue(ctx context.Context, queueName *string) error`

Drops an existing queue and all its data.

#### `ListQueues(ctx context.Context) ([]string, error)`

Lists all available queues.

#### `Close() error`

Closes the client and any active workers.

### TaskContext

Provides execution context and utilities within task handlers.

#### `Step(ctx context.Context, name string, fn func() (JsonValue, error)) (JsonValue, error)`

Defines a checkpointed step within the task. Steps are idempotent.

```go
result, err := taskCtx.Step(ctx, "process-payment", func() (absurd.JsonValue, error) {
	return stripe.CreateCharge(amount, token)
})
```

#### `SleepFor(ctx context.Context, stepName string, duration int) error`

Suspends the task for a specified duration in seconds.

```go
err := taskCtx.SleepFor(ctx, "wait-before-retry", 30) // Sleep for 30 seconds
```

#### `SleepUntil(ctx context.Context, stepName string, wakeAt time.Time) error`

Suspends the task until a specific timestamp.

#### `AwaitEvent(ctx context.Context, eventName string, options *AwaitEventOptions) (JsonValue, error)`

Waits for an event to be emitted. Events are cached and race-free.

```go
// Wait indefinitely
shipment, err := taskCtx.AwaitEvent(ctx, fmt.Sprintf("order:%s:shipped", orderID), nil)

// Wait with timeout
approval, err := taskCtx.AwaitEvent(ctx, "approval", &absurd.AwaitEventOptions{
	Timeout: &timeout, // 300 seconds
})
if err != nil {
	if _, ok := err.(*absurd.TimeoutError); ok {
		// Handle timeout
	}
}
```

#### `EmitEvent(ctx context.Context, eventName string, payload JsonValue) error`

Emits an event from within a task.

### Types

#### `JsonValue`

Type alias for `interface{}` representing any JSON-serializable value.

#### `JsonObject`

Type alias for `map[string]JsonValue`.

#### `RetryStrategy`

```go
type RetryStrategy struct {
	Kind        string  // "fixed", "exponential", or "none"
	BaseSeconds *int    // Base delay
	Factor      *int    // Exponential backoff factor (default: 2)
	MaxSeconds  *int    // Maximum delay cap
}
```

#### `CancellationPolicy`

```go
type CancellationPolicy struct {
	MaxDuration *int // Maximum total task runtime (seconds)
	MaxDelay    *int // Maximum delay before first execution (seconds)
}
```

#### `TimeoutError`

Error type thrown when `AwaitEvent` times out.

## Examples

### Basic Task with Steps

```go
err = client.RegisterTask(&absurd.TaskRegistrationOptions{
	Name: "order-processing",
}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
	// Each step is checkpointed
	order, err := taskCtx.Step(ctx, "validate-order", func() (absurd.JsonValue, error) {
		return validateOrder(params)
	})
	if err != nil {
		return nil, err
	}

	payment, err := taskCtx.Step(ctx, "charge-payment", func() (absurd.JsonValue, error) {
		orderData := order.(map[string]interface{})
		return processPayment(orderData["total"], orderData["paymentToken"])
	})
	if err != nil {
		return nil, err
	}

	shipping, err := taskCtx.Step(ctx, "arrange-shipping", func() (absurd.JsonValue, error) {
		orderData := order.(map[string]interface{})
		return scheduleShipment(orderData["items"], orderData["address"])
	})
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"orderId": order.(map[string]interface{})["id"],
		"trackingNumber": shipping.(map[string]interface{})["tracking"],
	}, nil
})
```

### Event-Driven Workflow

```go
err = client.RegisterTask(&absurd.TaskRegistrationOptions{
	Name: "approval-workflow",
}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
	paramMap := params.(map[string]interface{})
	
	// Send approval request
	_, err := taskCtx.Step(ctx, "send-approval", func() (absurd.JsonValue, error) {
		return sendApprovalEmail(paramMap["managerId"], paramMap["requestDetails"])
	})
	if err != nil {
		return nil, err
	}

	// Wait for manager's decision (24 hours)
	timeout := 86400
	eventName := fmt.Sprintf("approval:%s", paramMap["requestId"])
	decision, err := taskCtx.AwaitEvent(ctx, eventName, &absurd.AwaitEventOptions{
		Timeout: &timeout,
	})
	if err != nil {
		return nil, err
	}

	decisionMap := decision.(map[string]interface{})
	if approved, ok := decisionMap["approved"].(bool); ok && approved {
		_, err := taskCtx.Step(ctx, "execute-request", func() (absurd.JsonValue, error) {
			return executeRequest(paramMap["requestDetails"])
		})
		if err != nil {
			return nil, err
		}
	}

	return decision, nil
})

// Elsewhere in your application
err = client.EmitEvent(context.Background(), fmt.Sprintf("approval:%s", requestID), 
	map[string]interface{}{
		"approved":  true,
		"managerId": "mgr1",
	}, nil)
```

### Scheduled Tasks with Sleep

```go
err = client.RegisterTask(&absurd.TaskRegistrationOptions{
	Name: "reminder-sequence",
}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
	paramMap := params.(map[string]interface{})
	userEmail := paramMap["userEmail"].(string)
	
	reminders := []struct {
		delay   int
		message string
	}{
		{86400, "Your trial expires in 7 days"},     // 1 day
		{518400, "Your trial expires tomorrow"},     // 6 days  
		{86400, "Your trial has expired"},           // 1 day
	}

	for i, reminder := range reminders {
		err := taskCtx.SleepFor(ctx, fmt.Sprintf("wait-%d", i), reminder.delay)
		if err != nil {
			return nil, err
		}
		
		_, err = taskCtx.Step(ctx, fmt.Sprintf("send-reminder-%d", i), func() (absurd.JsonValue, error) {
			return sendEmail(userEmail, reminder.message)
		})
		if err != nil {
			return nil, err
		}
	}
	
	return "sequence-complete", nil
})
```

### Worker Configuration

```go
worker, err := client.StartWorker(context.Background(), &absurd.WorkerOptions{
	WorkerID:    stringPtr("worker-1"),
	Concurrency: intPtr(4),        // Process up to 4 tasks in parallel
	ClaimTimeout: intPtr(300),     // 5 minutes to complete each task
	PollInterval: float64Ptr(1.0), // Check for new tasks every second
	OnError: func(err error) {
		log.Printf("Worker error: %v", err)
		// Custom error handling, logging, monitoring
	},
})
if err != nil {
	log.Fatal(err)
}

// Graceful shutdown
go func() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<-sigChan
	worker.Close()
	client.Close()
}()
```

### Multiple Queues

```go
orderProcessor, err := absurd.New(&absurd.AbsurdOptions{
	QueueName: stringPtr("orders"),
})
if err != nil {
	log.Fatal(err)
}

emailProcessor, err := absurd.New(&absurd.AbsurdOptions{
	QueueName: stringPtr("emails"),
})
if err != nil {
	log.Fatal(err)
}

// Register tasks on different queues
orderProcessor.RegisterTask(&absurd.TaskRegistrationOptions{
	Name: "fulfill-order",
}, orderHandler)

emailProcessor.RegisterTask(&absurd.TaskRegistrationOptions{
	Name: "send-email",
}, emailHandler)

// Start dedicated workers
orderWorker, _ := orderProcessor.StartWorker(context.Background(), &absurd.WorkerOptions{
	Concurrency: intPtr(2),
})
emailWorker, _ := emailProcessor.StartWorker(context.Background(), &absurd.WorkerOptions{
	Concurrency: intPtr(10),
})
```

## Helper Functions

```go
func stringPtr(s string) *string { return &s }
func intPtr(i int) *int { return &i }
func float64Ptr(f float64) *float64 { return &f }
```

This documentation covers the complete public API of the Absurd Go SDK. The SDK follows Go conventions and provides type-safe interfaces for building durable workflows.