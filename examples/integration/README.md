# Helix Integration Example

This example demonstrates how to integrate and use the `gox-helix` library in a Go application using the `go-fx` dependency injection framework. It sets up a complete worker that can register itself, acquire partitions, and process work.

## Prerequisites

Before running this example, you need a running MySQL database. The application expects to find the database connection details from the environment. The default connection URL is `root:password@tcp(127.0.0.1:3306)/gox_helix`. You can set up a local MySQL instance using Docker:

```bash
docker run --name gox-helix-mysql -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=gox_helix -p 3306:3306 -d mysql:8.0
```

## Understanding the Code

The application uses `go-fx` to manage dependencies and application lifecycle. Here are the key components:

### 1. Supplying Configuration and Singletons

At the beginning of the `fx.New` call, we supply the necessary configuration and application-level singletons:

```go
// Create a new application singleton. It manages the application's context and lifecycle.
// Note: The name has a typo in the original notes; it is `ApplicationSingleton`.
appSignal := pkgCommon.NewApplicationSingletonWithContext(ctx)

// ...

app := fx.New(
    // Supply the application config and the domain definitions
    fx.Supply(&appConfig),
    fx.Supply(&appConfig.Domains),

    // Supply the application singleton
    fx.Supply(appSignal),

    // ...
)
```
- `fx.Supply(&appConfig.Domains)`: Provides the domain configuration that defines the tasklists and their properties.
- `fx.Supply(appSignal)`: Provides the `ApplicationSingleton` instance, which is used throughout the application to manage state and lifecycle.


### 2. Providing Helix Dependencies

The core of the `gox-helix` framework is provided through a single module:

```go
goxHelixApi.Provider,
```
This provider sets up all the necessary components for the helix framework, including services for coordination, locking, domain management, and worker registration.

### 3. Invoking the Executor Lifecycle

To start the worker and make it run, you need to invoke the `executor.NewExecutorLifecycle`:

```go
fx.Invoke(executor.NewExecutorLifecycle),
```
This function sets up and starts the executor service, which is responsible for managing the worker's lifecycle, including starting and stopping all the necessary processes.

### 4. Implementing the Client Work Function

The actual work is done in the `ClientFunctionProcessWork`. This is where you define what your worker should do when it receives a task.

```go
fx.Provide(func() coordinator.ClientFunctionProcessWork {
    return func(ctx context.Context, work coordinator.Work) {
        if atomic.AddInt64(&count, 1)%10 == 0 {
            slog.Info("Got work to do", "work", work)
        }
        time.Sleep(10 * time.Second)
        work.CompletedChannel <- coordinator.WorkResponse{}
        close(work.CompletedChannel)
    }
}),
```
- This function is provided to the `fx` container.
- It receives a `coordinator.Work` object that contains information about the task.
- The work is processed asynchronously, and upon completion, a `coordinator.WorkResponse{}` is sent to the `work.CompletedChannel`.

## How to Run

You can run the example directly from your terminal:

```bash
go run examples/integration/main.go
```

The application will start, register the worker, and begin processing work for the defined domains and tasklists. You will see log messages indicating the work being done. The application also truncates the database tables on startup for a clean run.
