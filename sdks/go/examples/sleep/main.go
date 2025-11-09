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

	// Register a task that demonstrates sleeping
	err = client.RegisterTask(&absurd.TaskRegistrationOptions{
		Name: "sleep-demo-task",
	}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
		paramMap := params.(map[string]interface{})
		taskName := paramMap["taskName"].(string)

		// Step 1: Start
		start, err := taskCtx.Step(ctx, "start", func() (absurd.JsonValue, error) {
			startTime := time.Now().Unix()
			log.Printf("Task %s: Starting at %d", taskName, startTime)
			return map[string]interface{}{
				"started_at": startTime,
				"message":    fmt.Sprintf("Task %s started", taskName),
			}, nil
		})
		if err != nil {
			return nil, err
		}

		// Step 2: Sleep for 5 seconds
		log.Printf("Task %s: Sleeping for 5 seconds...", taskName)
		err = taskCtx.SleepFor(ctx, "sleep-5s", 5)
		if err != nil {
			return nil, err
		}

		// Step 3: Log after first sleep
		after1, err := taskCtx.Step(ctx, "after-first-sleep", func() (absurd.JsonValue, error) {
			currentTime := time.Now().Unix()
			log.Printf("Task %s: Woke up after first sleep at %d", taskName, currentTime)
			return map[string]interface{}{
				"woke_up_at": currentTime,
				"message":    "Completed first sleep",
			}, nil
		})
		if err != nil {
			return nil, err
		}

		// Step 4: Sleep until a specific time (10 seconds from now)
		wakeTime := time.Now().Add(10 * time.Second)
		log.Printf("Task %s: Sleeping until %s", taskName, wakeTime.Format(time.RFC3339))
		err = taskCtx.SleepUntil(ctx, "sleep-until", wakeTime)
		if err != nil {
			return nil, err
		}

		// Step 5: Log after second sleep
		after2, err := taskCtx.Step(ctx, "after-second-sleep", func() (absurd.JsonValue, error) {
			currentTime := time.Now().Unix()
			log.Printf("Task %s: Woke up after second sleep at %d", taskName, currentTime)
			return map[string]interface{}{
				"final_wake_at": currentTime,
				"message":       "Completed second sleep",
			}, nil
		})
		if err != nil {
			return nil, err
		}

		// Step 6: Final summary
		summary, err := taskCtx.Step(ctx, "summary", func() (absurd.JsonValue, error) {
			endTime := time.Now().Unix()
			startData := start.(map[string]interface{})
			startTime := int64(startData["started_at"].(float64))
			duration := endTime - startTime
			
			log.Printf("Task %s: Completed! Total duration: %d seconds", taskName, duration)
			return map[string]interface{}{
				"task_name":     taskName,
				"started_at":    startTime,
				"ended_at":      endTime,
				"duration":      duration,
				"first_sleep":   after1,
				"second_sleep":  after2,
				"status":        "completed",
			}, nil
		})
		if err != nil {
			return nil, err
		}

		return summary, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Register a task that schedules multiple sleeps
	err = client.RegisterTask(&absurd.TaskRegistrationOptions{
		Name: "staggered-tasks",
	}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
		paramMap := params.(map[string]interface{})
		delays := paramMap["delays"].([]interface{})

		results := make([]interface{}, 0, len(delays))
		
		for i, delayVal := range delays {
			delay := int(delayVal.(float64))
			stepName := fmt.Sprintf("sleep-step-%d", i+1)
			
			// Sleep for the specified delay
			log.Printf("Staggered task: Sleeping for %d seconds (step %d)", delay, i+1)
			err := taskCtx.SleepFor(ctx, stepName, delay)
			if err != nil {
				return nil, err
			}

			// Record this step
			result, err := taskCtx.Step(ctx, fmt.Sprintf("record-step-%d", i+1), func() (absurd.JsonValue, error) {
				currentTime := time.Now().Unix()
				log.Printf("Staggered task: Completed step %d at %d", i+1, currentTime)
				return map[string]interface{}{
					"step":       i + 1,
					"delay":      delay,
					"woke_up_at": currentTime,
				}, nil
			})
			if err != nil {
				return nil, err
			}

			results = append(results, result)
		}

		return map[string]interface{}{
			"status":  "all_steps_completed",
			"results": results,
		}, nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start a worker
	worker, err := client.StartWorker(context.Background(), &absurd.WorkerOptions{
		WorkerID:    stringPtr("sleep-demo-worker"),
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
	log.Println("Spawning sleep demo task...")
	result1, err := client.Spawn(context.Background(), "sleep-demo-task", map[string]interface{}{
		"taskName": "SleepDemo1",
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Spawned sleep demo task: %s\n", result1.TaskID)

	log.Println("Spawning staggered tasks...")
	result2, err := client.Spawn(context.Background(), "staggered-tasks", map[string]interface{}{
		"delays": []interface{}{3, 2, 4, 1}, // Sleep for 3s, then 2s, then 4s, then 1s
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Spawned staggered task: %s\n", result2.TaskID)

	// Let the tasks run - this should take about 25-30 seconds total
	log.Println("Waiting for tasks to complete (this will take ~30 seconds)...")
	time.Sleep(35 * time.Second)

	log.Println("Sleep demo completed!")
}

// Helper functions
func stringPtr(s string) *string { return &s }
func intPtr(i int) *int { return &i }