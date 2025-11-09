# Absurd TypeScript SDK Documentation

The Absurd TypeScript SDK provides a complete interface for building durable, fault-tolerant workflows using PostgreSQL as the backend. This document covers all classes, methods, and types available in the SDK.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Classes](#core-classes)
  - [Absurd](#absurd)
  - [TaskContext](#taskcontext)
- [Types and Interfaces](#types-and-interfaces)
- [Error Classes](#error-classes)
- [Examples](#examples)

## Installation

```bash
npm install absurd-sdk
```

The SDK requires PostgreSQL with the Absurd schema installed and Node.js 18+.

## Quick Start

```typescript
import { Absurd } from 'absurd-sdk';

const app = new Absurd('postgresql://localhost/mydb');

// Register a task
app.registerTask({ name: 'hello-world' }, async (params, ctx) => {
  const result = await ctx.step('greet', async () => {
    return `Hello, ${params.name}!`;
  });
  
  return result;
});

// Start a worker
const worker = await app.startWorker();

// Spawn a task
await app.spawn('hello-world', { name: 'World' });
```

## Core Classes

### Absurd

The main SDK client class for interacting with the Absurd system.

#### Constructor

```typescript
new Absurd(options?: AbsurdOptions | string | pg.Pool)
```

**Parameters:**

- `options` - Configuration options, database connection string, or PostgreSQL pool

**AbsurdOptions:**

```typescript
interface AbsurdOptions {
  db?: pg.Pool | string;           // Database connection
  queueName?: string;              // Default queue name (default: "default")
  defaultMaxAttempts?: number;     // Default retry attempts (default: 5)
  log?: Log;                       // Custom logger (default: console)
}
```

#### Methods

##### `registerTask<P, R>(options, handler)`

Registers a task handler function.

```typescript
registerTask<P = any, R = any>(
  options: TaskRegistrationOptions,
  handler: TaskHandler<P, R>
): void
```

**Parameters:**

- `options.name` - Unique task name
- `options.queue?` - Queue name (defaults to client's queue)
- `options.defaultMaxAttempts?` - Default retry attempts for this task
- `options.defaultCancellation?` - Default cancellation policy
- `handler` - Async function that processes the task

**Example:**

```typescript
app.registerTask(
  { 
    name: 'process-order',
    defaultMaxAttempts: 3,
    defaultCancellation: { maxDuration: 300 }
  },
  async (params: { orderId: string }, ctx) => {
    // Task logic here
    return { status: 'completed' };
  }
);
```

##### `spawn<P>(taskName, params, options?)`

Spawns a new task for execution.

```typescript
spawn<P = any>(
  taskName: string,
  params: P,
  options?: SpawnOptions
): Promise<SpawnResult>
```

**Parameters:**

- `taskName` - Name of the registered task
- `params` - Parameters to pass to the task
- `options` - Spawn configuration options

**Returns:** `SpawnResult` with `taskID`, `runID`, and `attempt`

**SpawnOptions:**

```typescript
interface SpawnOptions {
  maxAttempts?: number;         // Override retry attempts
  retryStrategy?: RetryStrategy; // Retry backoff strategy
  headers?: JsonObject;         // Metadata attached to task
  queue?: string;               // Target queue (for unregistered tasks)
  cancellation?: CancellationPolicy; // Cancellation timeouts
}
```

##### `startWorker(options?)`

Starts a worker that continuously processes tasks.

```typescript
startWorker(options?: WorkerOptions): Promise<Worker>
```

**Parameters:**

- `options` - Worker configuration

**Returns:** Worker instance with `close()` method

**WorkerOptions:**

```typescript
interface WorkerOptions {
  workerId?: string;              // Worker identifier
  claimTimeout?: number;          // Task claim timeout in seconds (default: 120)
  batchSize?: number;             // Tasks to claim per batch
  concurrency?: number;           // Parallel task execution limit (default: 1)
  pollInterval?: number;          // Polling frequency in seconds (default: 0.25)
  onError?: (error: Error) => void; // Error handler
  fatalOnLeaseTimeout?: boolean;  // Exit process on claim timeout (default: true)
}
```

##### `emitEvent(eventName, payload?, queueName?)`

Emits an event that tasks can await.

```typescript
emitEvent(
  eventName: string,
  payload?: JsonValue,
  queueName?: string
): Promise<void>
```

**Parameters:**

- `eventName` - Unique event identifier
- `payload` - Optional event data
- `queueName` - Target queue (defaults to client's queue)

##### `createQueue(queueName?)`

Creates a new queue in the database.

```typescript
createQueue(queueName?: string): Promise<void>
```

##### `dropQueue(queueName?)`

Drops an existing queue and all its data.

```typescript
dropQueue(queueName?: string): Promise<void>
```

##### `listQueues()`

Lists all available queues.

```typescript
listQueues(): Promise<string[]>
```

##### `workBatch(workerId?, claimTimeout?, batchSize?)`

Processes a single batch of tasks (for manual polling).

```typescript
workBatch(
  workerId?: string,
  claimTimeout?: number,
  batchSize?: number
): Promise<void>
```

##### `claimTasks(options?)`

Claims tasks for processing (low-level API).

```typescript
claimTasks(options?: {
  batchSize?: number;
  claimTimeout?: number;
  workerId?: string;
}): Promise<ClaimedTask[]>
```

##### `close()`

Closes the client and any active workers.

```typescript
close(): Promise<void>
```

### TaskContext

Provides execution context and utilities within task handlers. Automatically created and passed to task handlers.

#### Properties

- `taskID: string` - Unique identifier for the current task

#### Methods

##### `step<T>(name, fn)`

Defines a checkpointed step within the task. Steps are idempotent - they execute exactly once and cache their results.

```typescript
step<T>(name: string, fn: () => Promise<T>): Promise<T>
```

**Parameters:**

- `name` - Unique step name within the task
- `fn` - Async function containing the step logic

**Returns:** The step's result (cached on subsequent runs)

**Example:**

```typescript
const payment = await ctx.step('process-payment', async () => {
  return await stripe.charges.create({
    amount: params.amount,
    source: params.token,
    idempotency_key: ctx.taskID
  });
});
```

##### `sleepFor(stepName, duration)`

Suspends the task for a specified duration in seconds.

```typescript
sleepFor(stepName: string, duration: number): Promise<void>
```

**Parameters:**

- `stepName` - Unique name for this sleep checkpoint
- `duration` - Sleep duration in seconds

**Example:**

```typescript
await ctx.sleepFor('wait-before-retry', 30); // Sleep for 30 seconds
```

##### `sleepUntil(stepName, wakeAt)`

Suspends the task until a specific timestamp.

```typescript
sleepUntil(stepName: string, wakeAt: Date): Promise<void>
```

**Parameters:**

- `stepName` - Unique name for this sleep checkpoint
- `wakeAt` - When to resume execution

##### `awaitEvent(eventName, options?)`

Waits for an event to be emitted. Events are cached and race-free.

```typescript
awaitEvent(
  eventName: string,
  options?: { stepName?: string; timeout?: number }
): Promise<JsonValue>
```

**Parameters:**

- `eventName` - Event identifier to wait for
- `options.stepName` - Custom step name (defaults to `$awaitEvent:${eventName}`)
- `options.timeout` - Timeout in seconds (optional)

**Returns:** Event payload when received

**Throws:** `TimeoutError` if timeout is reached

**Example:**

```typescript
// Wait indefinitely
const shipment = await ctx.awaitEvent(`order:${orderId}:shipped`);

// Wait with timeout
try {
  const approval = await ctx.awaitEvent('approval', { timeout: 300 });
} catch (err) {
  if (err instanceof TimeoutError) {
    // Handle timeout
  }
}
```

##### `emitEvent(eventName, payload?)`

Emits an event from within a task.

```typescript
emitEvent(eventName: string, payload?: JsonValue): Promise<void>
```

**Parameters:**

- `eventName` - Unique event identifier
- `payload` - Optional event data

## Types and Interfaces

### RetryStrategy

Defines how failed tasks should be retried.

```typescript
interface RetryStrategy {
  kind: "fixed" | "exponential" | "none";
  baseSeconds?: number;     // Base delay (default varies by kind)
  factor?: number;          // Exponential backoff factor (default: 2)
  maxSeconds?: number;      // Maximum delay cap
}
```

**Examples:**

```typescript
// Fixed 5-second delays
{ kind: "fixed", baseSeconds: 5 }

// Exponential backoff: 1s, 2s, 4s, 8s...
{ kind: "exponential", baseSeconds: 1, factor: 2, maxSeconds: 60 }

// No retries
{ kind: "none" }
```

### CancellationPolicy

Defines automatic task cancellation conditions.

```typescript
interface CancellationPolicy {
  maxDuration?: number;     // Maximum total task runtime (seconds)
  maxDelay?: number;        // Maximum delay before first execution (seconds)
}
```

### JsonValue

Type-safe JSON representation.

```typescript
type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

type JsonObject = { [key: string]: JsonValue };
```

### TaskHandler

Function signature for task implementations.

```typescript
type TaskHandler<P = any, R = any> = (
  params: P,
  ctx: TaskContext
) => Promise<R>;
```

### ClaimedTask

Represents a task claimed by a worker.

```typescript
interface ClaimedTask {
  run_id: string;
  task_id: string;
  task_name: string;
  attempt: number;
  params: JsonValue;
  retry_strategy: JsonValue;
  max_attempts: number | null;
  headers: JsonObject | null;
  wake_event: string | null;
  event_payload: JsonValue | null;
}
```

## Error Classes

### SuspendTask

Internal exception used to suspend task execution. Users should not catch or handle this exception.

### TimeoutError

Thrown when `awaitEvent` times out.

```typescript
class TimeoutError extends Error {
  constructor(message: string)
}
```

**Example:**

```typescript
try {
  await ctx.awaitEvent('user-response', { timeout: 60 });
} catch (err) {
  if (err instanceof TimeoutError) {
    console.log('User did not respond in time');
  }
}
```

## Examples

### Basic Task with Steps

```typescript
app.registerTask({ name: 'order-processing' }, async (params, ctx) => {
  // Each step is checkpointed
  const order = await ctx.step('validate-order', async () => {
    return await validateOrder(params.orderId);
  });

  const payment = await ctx.step('charge-payment', async () => {
    return await processPayment(order.total, params.paymentToken);
  });

  const shipping = await ctx.step('arrange-shipping', async () => {
    return await scheduleShipment(order.items, order.address);
  });

  return { orderId: order.id, trackingNumber: shipping.tracking };
});
```

### Event-Driven Workflow

```typescript
app.registerTask({ name: 'approval-workflow' }, async (params, ctx) => {
  // Send approval request
  await ctx.step('send-approval', async () => {
    await sendApprovalEmail(params.managerId, params.requestDetails);
    return true;
  });

  // Wait for manager's decision
  const decision = await ctx.awaitEvent(`approval:${params.requestId}`, {
    timeout: 86400 // 24 hours
  });

  if (decision.approved) {
    await ctx.step('execute-request', async () => {
      return await executeRequest(params.requestDetails);
    });
  }

  return decision;
});

// Elsewhere in your application
app.emitEvent(`approval:${requestId}`, { approved: true, managerId: 'mgr1' });
```

### Scheduled Tasks with Sleep

```typescript
app.registerTask({ name: 'reminder-sequence' }, async (params, ctx) => {
  const reminders = [
    { delay: 86400, message: 'Your trial expires in 7 days' },      // 1 day
    { delay: 518400, message: 'Your trial expires tomorrow' },      // 6 days  
    { delay: 86400, message: 'Your trial has expired' }             // 1 day
  ];

  for (const [index, reminder] of reminders.entries()) {
    await ctx.sleepFor(`wait-${index}`, reminder.delay);
    
    await ctx.step(`send-reminder-${index}`, async () => {
      await sendEmail(params.userEmail, reminder.message);
      return true;
    });
  }
});
```

### Error Handling and Retries

```typescript
app.registerTask({ 
  name: 'external-api-call',
  defaultMaxAttempts: 5
}, async (params, ctx) => {
  const result = await ctx.step('api-call', async () => {
    // This step will retry up to 5 times if it throws
    const response = await fetch(`https://api.example.com/process`, {
      method: 'POST',
      headers: { 'Idempotency-Key': ctx.taskID },
      body: JSON.stringify(params)
    });
    
    if (!response.ok) {
      throw new Error(`API call failed: ${response.status}`);
    }
    
    return await response.json();
  });

  return result;
});
```

### Worker Configuration

```typescript
const worker = await app.startWorker({
  workerId: 'worker-1',
  concurrency: 4,        // Process up to 4 tasks in parallel
  claimTimeout: 300,     // 5 minutes to complete each task
  pollInterval: 1,       // Check for new tasks every second
  onError: (error) => {
    console.error('Worker error:', error);
    // Custom error handling, logging, monitoring
  }
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  await worker.close();
  await app.close();
});
```

### Multiple Queues

```typescript
const orderProcessor = new Absurd({ queueName: 'orders' });
const emailProcessor = new Absurd({ queueName: 'emails' });

// Register tasks on different queues
orderProcessor.registerTask({ name: 'fulfil-order' }, orderHandler);
emailProcessor.registerTask({ name: 'send-email' }, emailHandler);

// Start dedicated workers
const orderWorker = await orderProcessor.startWorker({ concurrency: 2 });
const emailWorker = await emailProcessor.startWorker({ concurrency: 10 });
```

This documentation covers the complete public API of the Absurd TypeScript SDK. For more examples and advanced patterns, see the examples directory in the repository.
