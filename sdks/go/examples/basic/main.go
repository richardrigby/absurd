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

	// Register a simple task
	err = client.RegisterTask(&absurd.TaskRegistrationOptions{
		Name: "hello-world",
	}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
		// Use Step to create a checkpointed operation
		result, err := taskCtx.Step(ctx, "greet", func() (absurd.JsonValue, error) {
			name := "World"
			if paramMap, ok := params.(map[string]interface{}); ok {
				if nameVal, exists := paramMap["name"]; exists {
					name = fmt.Sprintf("%v", nameVal)
				}
			} else if nameStr, ok := params.(string); ok {
				name = nameStr
			}
			
			greeting := fmt.Sprintf("Hello, %s!", name)
			log.Printf("Generated greeting: %s", greeting)
			return greeting, nil
		})
		
		return result, err
	})
	if err != nil {
		log.Fatal(err)
	}

	// Register a task with multiple steps
	err = client.RegisterTask(&absurd.TaskRegistrationOptions{
		Name: "multi-step-example",
	}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
		// Step 1: Initialize
		init, err := taskCtx.Step(ctx, "initialize", func() (absurd.JsonValue, error) {
			log.Println("Initializing task...")
			return map[string]interface{}{
				"initialized_at": time.Now().Unix(),
				"input_params":   params,
			}, nil
		})
		if err != nil {
			return nil, err
		}

		// Step 2: Process (with a small sleep to demonstrate durability)
		processed, err := taskCtx.Step(ctx, "process", func() (absurd.JsonValue, error) {
			log.Println("Processing task...")
			time.Sleep(2 * time.Second) // Simulate some work
			
			initData := init.(map[string]interface{})
			return map[string]interface{}{
				"processed_at": time.Now().Unix(),
				"started_at":   initData["initialized_at"],
				"result":       "Processing completed successfully",
			}, nil
		})
		if err != nil {
			return nil, err
		}

		// Step 3: Finalize
		final, err := taskCtx.Step(ctx, "finalize", func() (absurd.JsonValue, error) {
			log.Println("Finalizing task...")
			
			processData := processed.(map[string]interface{})
			return map[string]interface{}{
				"finalized_at": time.Now().Unix(),
				"started_at":   processData["started_at"],
				"processed_at": processData["processed_at"],
				"final_result": "Task completed successfully",
			}, nil
		})
		if err != nil {
			return nil, err
		}

		return final, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start a worker
	worker, err := client.StartWorker(context.Background(), &absurd.WorkerOptions{
		WorkerID:    stringPtr("demo-worker"),
		Concurrency: intPtr(2),
		OnError: func(err error) {
			log.Printf("Worker error: %v", err)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer worker.Close()

	// Spawn some tasks
	log.Println("Spawning hello-world task...")
	result1, err := client.Spawn(context.Background(), "hello-world", "Go SDK", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Spawned hello-world task: %s (run: %s)\n", result1.TaskID, result1.RunID)

	log.Println("Spawning multi-step task...")
	result2, err := client.Spawn(context.Background(), "multi-step-example", map[string]interface{}{
		"message": "This is a test",
		"number":  42,
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Spawned multi-step task: %s (run: %s)\n", result2.TaskID, result2.RunID)

	// Let the worker run for a bit
	log.Println("Waiting for tasks to complete...")
	time.Sleep(10 * time.Second)

	log.Println("Demo completed!")
}

// Helper functions for pointer creation
func stringPtr(s string) *string { return &s }
func intPtr(i int) *int { return &i }