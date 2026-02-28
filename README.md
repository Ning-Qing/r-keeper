# r-keeper

基于 Redis 的分布式锁和主从选举库，用于在分布式系统中实现节点主从切换和锁续约。

## 特性

- **分布式锁**：基于 Redis 实现的分布式锁，支持锁的获取和续约
- **主从选举**：自动进行主从节点选举，只有一个节点能成为主节点
- **自动续约**：主节点定期自动续约锁，防止锁过期
- **状态机管理**：使用有限状态机管理节点状态转换
- **回调机制**：支持主从状态切换时的回调函数
- **轻量级**：简单易用，依赖少

## 安装

```bash
go get github.com/Ning-Qing/r-keeper
```

## 快速开始

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

    // 创建 Redis 客户端
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "",
        DB:       0,
    })

    // 创建节点
    node, err := rkeeper.New(ctx,
        rkeeper.WithConnect(rdb),
        rkeeper.WithTTL(2*time.Second),
        rkeeper.WithMasterCallback(func(ctx context.Context) {
            slog.Info("成为主节点，开始执行主节点逻辑")
        }),
        rkeeper.WithNomalCallback(func(ctx context.Context) {
            slog.Info("成为从节点，停止执行主节点逻辑")
        }),
    )
    if err != nil {
        panic(err)
    }

    // 启动节点（会阻塞）
    node.Start()
}
```

## 配置选项

| 选项 | 类型 | 默认值 | 描述 |
|------|------|--------|------|
| `WithConnect` | `*redis.Client` | 必填 | Redis 客户端连接 |
| `WithID` | `string` | UUID | 节点唯一标识 |
| `WithKey` | `string` | `"rkeeper:lock"` | Redis 锁键名 |
| `WithTTL` | `time.Duration` | `1秒` | 锁的有效期 |
| `WithLogger` | `*slog.Logger` | 默认 logger | 日志记录器 |
| `WithMasterCallback` | `func(ctx context.Context)` | nil | 进入主节点状态时的回调 |
| `WithNomalCallback` | `func(ctx context.Context)` | nil | 进入从节点状态时的回调 |

## 工作原理

### 状态机

节点通过有限状态机管理状态转换：

```
        选举成功                    续约失败
    ┌──────────────┐            ┌──────────────┐
    │              ▼            ▼              │
┌───┴───┐      ┌─────────┐  ┌─────────┐      ┌───┴───┐
│ nomal │──────▶│ master  │──│ master  │─────▶│ nomal │
└───┬───┘      └─────────┘  └─────────┘      └───┬───┘
    │              ▲            ▲              │
    └──────────────┴────────────┴──────────────┘
       选举失败        续约成功
```

### 节点状态

- **nomal（普通节点）**：尝试获取分布式锁，竞争成为主节点
- **master（主节点）**：持有分布式锁，定期续约锁

### 事件

- `electoral_success`：选举成功，从 nomal 切换到 master
- `electoral_defeat`：选举失败，保持或切换到 nomal
- `renew_success`：续约成功，保持 master 状态
- `renew_defeat`：续约失败，从 master 切换到 nomal

### 运行流程

1. 节点启动后，初始状态为 `nomal`
2. 每隔 `TTL/2` 时间触发一次选举或续约
3. 如果当前是 `nomal` 状态，尝试获取锁：
   - 成功 → 触发 `electoral_success` 事件，切换到 `master`
   - 失败 → 触发 `electoral_defeat` 事件，保持 `nomal`
4. 如果当前是 `master` 状态，尝试续约锁：
   - 成功 → 触发 `renew_success` 事件，保持 `master`
   - 失败 → 触发 `renew_defeat` 事件，切换到 `nomal`

## API 文档

### 创建节点

```go
func New(ctx context.Context, opts ...Option) (*Node, error)
```

创建一个新的 Node 实例。必须提供 Redis 连接。

### 启动节点

```go
func (n *Node) Start()
```

启动节点的主从选举和锁续约逻辑。该方法会阻塞，直到上下文被取消。建议在单独的 goroutine 中调用。

### 获取状态

```go
func (n *Node) Status() string
```

返回当前节点状态（`nomal` 或 `master`）。

## 使用场景

- **分布式任务调度**：在多个节点中选出一个主节点负责任务调度
- **服务选主**：在集群中选举一个 leader 节点处理关键逻辑
- **资源锁定**：确保同一时间只有一个节点可以访问共享资源
- **故障转移**：主节点故障时，自动选举新的主节点

## 注意事项

1. **TTL 设置**：建议 TTL 大于 2 倍的网络延迟，避免误判主节点失效
2. **回调幂等性**：回调函数可能会被多次调用，需要确保其幂等性
3. **网络抖动**：网络抖动可能导致主从频繁切换，可根据业务场景适当调整 TTL
4. **Redis 集群**：支持 Redis 单机和集群模式
5. **上下文取消**：调用 `context.CancelFunc` 可以优雅地停止节点

## 测试

```bash
go test -v ./...
```

## 依赖

- [go-redis/v9](https://github.com/redis/go-redis) - Redis 客户端
- [looplab/fsm](https://github.com/looplab/fsm) - 有限状态机库
- [google/uuid](https://github.com/google/uuid) - UUID 生成

## 许可证

MIT License
