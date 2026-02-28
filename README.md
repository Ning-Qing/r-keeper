# r-keeper

A distributed lock and master-slave election library based on Redis, designed for node master-slave switching and lock renewal in distributed systems.

## Features

- **Distributed Lock**: Redis-based distributed lock with lock acquisition and renewal support
- **Master-Slave Election**: Automatic master-slave node election, ensuring only one master node exists
- **Automatic Renewal**: Master nodes automatically renew locks periodically to prevent expiration
- **State Machine Management**: Uses finite state machine for node state transition management
- **Callback Mechanism**: Supports callbacks when switching between master and slave states
- **Lightweight**: Simple and easy to use with minimal dependencies

## Installation

```bash
go get github.com/Ning-Qing/r-keeper
```

## Quick Start

```go
package main

import (
    "context"
    "log/slog"
    "time"

    "github.com/Ning-Qing/r-keeper"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()

    // Create Redis client
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "",
        DB:       0,
    })

    // Create node
    node, err := rkeeper.New(ctx,
        rkeeper.WithConnect(rdb),
        rkeeper.WithTTL(2*time.Second),
        rkeeper.WithMasterCallback(func(ctx context.Context) {
            slog.Info("Became master, start executing master logic")
        }),
        rkeeper.WithNomalCallback(func(ctx context.Context) {
            slog.Info("Became slave, stop executing master logic")
        }),
    )
    if err != nil {
        panic(err)
    }

    // Start node (blocking)
    node.Start()
}
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `WithConnect` | `*redis.Client` | Required | Redis client connection |
| `WithID` | `string` | UUID | Node unique identifier |
| `WithKey` | `string` | `"rkeeper:lock"` | Redis lock key name |
| `WithTTL` | `time.Duration` | `1 second` | Lock expiration time |
| `WithLogger` | `*slog.Logger` | default logger | Logger instance |
| `WithMasterCallback` | `func(ctx context.Context)` | nil | Callback when entering master state |
| `WithNomalCallback` | `func(ctx context.Context)` | nil | Callback when entering normal state |

## How It Works

### State Machine

Node manages state transitions using finite state machine:

```
    Election Success      Renewal Failure
┌────────────┐          ┌────────────┐
│            ▼          ▼            │
┌──────┐   ┌─────────┐   ┌──────┐
│nomal │──▶│ master  │──▶│nomal │
└──────┘   └─────────┘   └──────┘
  │           ▲              ▲
  └───────────┴──────────────┘
 Election Failure    Renewal Success
 (keep nomal)
```

### Node States

- **nomal (normal node)**: Tries to acquire distributed lock, competing to become master
- **master (master node)**: Holds distributed lock, periodically renews the lock

### Events

- `electoral_success`: Election successful, switch from nomal to master
- `electoral_defeat`: Election failed, keep or switch to nomal
- `renew_success`: Renewal successful, keep master state
- `renew_defeat`: Renewal failed, switch from master to nomal

### Operation Flow

1. After node starts, initial state is `nomal`
2. Every `TTL/2` time, trigger election or renewal
3. If current state is `nomal`, try to acquire lock:
   - Success → trigger `electoral_success` event, switch to `master`
   - Failure → trigger `electoral_defeat` event, keep `nomal`
4. If current state is `master`, try to renew lock:
   - Success → trigger `renew_success` event, keep `master`
   - Failure → trigger `renew_defeat` event, switch to `nomal`

## API Documentation

### Create Node

```go
func New(ctx context.Context, opts ...Option) (*Node, error)
```

Create a new Node instance. Redis connection is required.

### Start Node

```go
func (n *Node) Start()
```

Start the node's master-slave election and lock renewal logic. This method blocks until the context is cancelled. Recommended to call in a separate goroutine.

### Get Status

```go
func (n *Node) Status() string
```

Returns current node status (`nomal` or `master`).

## Use Cases

- **Distributed Task Scheduling**: Elect a master node from multiple nodes to handle task scheduling
- **Service Leader Election**: Elect a leader node in a cluster to handle critical logic
- **Resource Locking**: Ensure only one node can access shared resources at a time
- **Failover**: Automatically elect a new master node when current master fails

## Important Notes

1. **TTL Setting**: Recommended TTL should be greater than 2x network latency to avoid false master failure detection
2. **Callback Idempotency**: Callback functions may be called multiple times, ensure they are idempotent
3. **Network Jitter**: Network jitter may cause frequent master-slave switching, adjust TTL based on business requirements
4. **Redis Cluster**: Supports both Redis standalone and cluster modes
5. **Context Cancellation**: Call `context.CancelFunc` to gracefully stop the node

## Testing

```bash
go test -v ./...
```

## Dependencies

- [go-redis/v9](https://github.com/redis/go-redis) - Redis client
- [looplab/fsm](https://github.com/looplab/fsm) - Finite state machine library
- [google/uuid](https://github.com/google/uuid) - UUID generator

## License

MIT License
