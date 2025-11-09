// Package absurd provides a Go SDK for the Absurd durable execution system.
//
// Absurd is a PostgreSQL-based durable workflow system that moves the complexity
// of durable execution into the database layer via stored procedures, keeping
// SDKs lightweight and language-agnostic.
//
// Basic usage:
//
//	client, err := absurd.New(&absurd.AbsurdOptions{
//		DB: "postgresql://localhost/mydb",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Register a task
//	err = client.RegisterTask(&absurd.TaskRegistrationOptions{
//		Name: "hello-world",
//	}, func(ctx context.Context, params absurd.JsonValue, taskCtx *absurd.TaskContext) (absurd.JsonValue, error) {
//		result, err := taskCtx.Step(ctx, "greet", func() (absurd.JsonValue, error) {
//			return fmt.Sprintf("Hello, %v!", params), nil
//		})
//		return result, err
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Start a worker
//	worker, err := client.StartWorker(context.Background(), nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer worker.Close()
//
//	// Spawn a task
//	result, err := client.Spawn(context.Background(), "hello-world", "World", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	
//	fmt.Printf("Spawned task: %s\n", result.TaskID)
package absurd

import (
	_ "github.com/lib/pq" // Import PostgreSQL driver
)