package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/earendil-works/absurd/sdks/go"
)

func main() {
	// Create client
	client, err := absurd.New(&absurd.AbsurdOptions{
		DB: "postgresql://localhost/absurd?sslmode=disable",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create queue
	if err := client.CreateQueue(context.Background(), nil); err != nil {
		log.Printf("Queue creation failed (may already exist): %v", err)
	}

	// Register a task that waits for events
	err = client.RegisterTask(&absurd.TaskRegistrationOptions{
		Name: "event-waiting-task",
	}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
		paramMap := params.(map[string]interface{})
		taskID := paramMap["taskId"].(string)

		// Step 1: Initialize and log start
		_, err := taskCtx.Step(ctx, "start", func() (absurd.JsonValue, error) {
			log.Printf("Task %s: Starting and waiting for events...", taskID)
			return fmt.Sprintf("Task %s started", taskID), nil
		})
		if err != nil {
			return nil, err
		}

		// Step 2: Wait for first event
		event1, err := taskCtx.AwaitEvent(ctx, fmt.Sprintf("task:%s:event1", taskID), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to await event1: %w", err)
		}
		log.Printf("Task %s: Received event1: %v", taskID, event1)

		// Step 3: Process event1
		processed1, err := taskCtx.Step(ctx, "process-event1", func() (absurd.JsonValue, error) {
			log.Printf("Task %s: Processing event1...", taskID)
			return map[string]interface{}{
				"event1_data": event1,
				"processed_at": time.Now().Unix(),
			}, nil
		})
		if err != nil {
			return nil, err
		}

		// Step 4: Wait for second event (with timeout)
		timeout := 30 // 30 seconds
		event2, err := taskCtx.AwaitEvent(ctx, fmt.Sprintf("task:%s:event2", taskID), &absurd.AwaitEventOptions{
			Timeout: &timeout,
		})
		if err != nil {
			if _, ok := err.(*absurd.TimeoutError); ok {
				log.Printf("Task %s: Timed out waiting for event2", taskID)
				return map[string]interface{}{
					"status": "partial_completion",
					"event1_result": processed1,
					"event2_result": "timed_out",
				}, nil
			}
			return nil, fmt.Errorf("failed to await event2: %w", err)
		}
		log.Printf("Task %s: Received event2: %v", taskID, event2)

		// Step 5: Process both events
		final, err := taskCtx.Step(ctx, "process-both", func() (absurd.JsonValue, error) {
			log.Printf("Task %s: Processing both events...", taskID)
			return map[string]interface{}{
				"status": "complete",
				"event1_result": processed1,
				"event2_result": map[string]interface{}{
					"event2_data": event2,
					"processed_at": time.Now().Unix(),
				},
				"completed_at": time.Now().Unix(),
			}, nil
		})
		if err != nil {
			return nil, err
		}

		log.Printf("Task %s: Completed successfully!", taskID)
		return final, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Register an event emitter task
	err = client.RegisterTask(&absurd.TaskRegistrationOptions{
		Name: "event-emitter-task",
	}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
		paramMap := params.(map[string]interface{})
		targetTaskID := paramMap["targetTaskId"].(string)
		delay := int(paramMap["delay"].(float64))

		// Sleep before emitting first event
		err := taskCtx.SleepFor(ctx, "delay-before-event1", delay)
		if err != nil {
			return nil, err
		}

		// Emit first event
		_, err = taskCtx.Step(ctx, "emit-event1", func() (absurd.JsonValue, error) {
			log.Printf("Emitting event1 for task %s", targetTaskID)
			err := taskCtx.EmitEvent(ctx, fmt.Sprintf("task:%s:event1", targetTaskID), map[string]interface{}{
				"message": "This is event 1",
				"timestamp": time.Now().Unix(),
			})
			return "event1_emitted", err
		})
		if err != nil {
			return nil, err
		}

		// Sleep before emitting second event
		err = taskCtx.SleepFor(ctx, "delay-before-event2", delay)
		if err != nil {
			return nil, err
		}

		// Emit second event
		_, err = taskCtx.Step(ctx, "emit-event2", func() (absurd.JsonValue, error) {
			log.Printf("Emitting event2 for task %s", targetTaskID)
			err := taskCtx.EmitEvent(ctx, fmt.Sprintf("task:%s:event2", targetTaskID), map[string]interface{}{
				"message": "This is event 2",
				"timestamp": time.Now().Unix(),
			})
			return "event2_emitted", err
		})
		if err != nil {
			return nil, err
		}

		return "events_emitted", nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start a worker
	worker, err := client.StartWorker(context.Background(), &absurd.WorkerOptions{
		WorkerID:    stringPtr("event-demo-worker"),
		Concurrency: intPtr(3),
		OnError: func(err error) {
			log.Printf("Worker error: %v", err)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer worker.Close()

	// Spawn tasks
	taskID := "demo-task-123"
	
	log.Println("Spawning event-waiting task...")
	result1, err := client.Spawn(context.Background(), "event-waiting-task", map[string]interface{}{
		"taskId": taskID,
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Spawned event-waiting task: %s\n", result1.TaskID)

	// Wait a moment for the first task to start
	time.Sleep(2 * time.Second)

	log.Println("Spawning event-emitter task...")
	result2, err := client.Spawn(context.Background(), "event-emitter-task", map[string]interface{}{
		"targetTaskId": taskID,
		"delay":        3, // 3 seconds between events
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Spawned event-emitter task: %s\n", result2.TaskID)

	// Let the tasks run
	log.Println("Waiting for tasks to complete...")
	time.Sleep(20 * time.Second)

	log.Println("Event demo completed!")
}

// Helper function
func stringPtr(s string) *string { return &s }
func intPtr(i int) *int { return &i }