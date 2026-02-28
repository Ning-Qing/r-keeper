package rkeeper

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/looplab/fsm"
	"github.com/redis/go-redis/v9"
)

// Option defines a function type for configuring Node instances. // 选项: 定义一个函数类型，用于配置 Node 实例。
type Option func(*Node) error

func WithLogger(logger *slog.Logger) Option {
	return func(n *Node) error {
		n.logger = logger
		return nil
	}
}

// WithConnect returns an Option function to set Redis connection. // 设置 Redis 连接
func WithConnect(connect *redis.Client) Option {
	return func(n *Node) error {
		n.r = connect
		return nil
	}
}

// WithID returns an Option function to set node ID. // 设置节点 ID
// ID is used to identify the node, default is UUID. // ID 用于标识节点，默认值为 UUID。
func WithID(id string) Option {
	return func(n *Node) error {
		n.Id = id
		return nil
	}
}

// WithKey returns an Option function to set lock key name. // 设置锁的键名
// Key is used to store the lock, default is "rkeeper:lock". // Key 用于存储锁的键名，默认值为 "rkeeper:lock"。
func WithKey(key string) Option {
	return func(n *Node) error {
		n.key = key
		return nil
	}
}

// WithTTL returns an Option function to set lock TTL. // 设置锁的有效期
// TTL is used to set lock expiration time, default is 1 second. // TTL 用于设置锁的有效期，默认值为 1 秒。
func WithTTL(ttl time.Duration) Option {
	return func(n *Node) error {
		n.ttl = ttl
		return nil
	}
}

// WithMasterCallback returns an Option function to set master node callback. // 设置主节点状态回调函数
// fn is the logic to execute when entering master state, default is empty. // fn 用于在主节点状态下执行的逻辑，默认值为空。
func WithMasterCallback(fn func(ctx context.Context)) Option {
	return func(n *Node) error {
		n.masterCallback = fn
		return nil
	}
}

// WithNomalCallback returns an Option function to set normal node callback. // 设置普通节点状态回调函数
// fn is the logic to execute when entering normal state, default is empty. // fn 用于在普通节点状态下执行的逻辑，默认值为空。
func WithNomalCallback(fn func(ctx context.Context)) Option {
	return func(n *Node) error {
		n.nomalCallback = fn
		return nil
	}
}

// WithExitCallback returns an Option function to set node exit callback. // 设置节点退出回调函数
// fn is the logic to execute when the node exits, default is empty. // fn 用于在节点退出时执行的逻辑，默认值为空。
func WithExitCallback(fn func(ctx context.Context)) Option {
	return func(n *Node) error {
		n.exitCallback = fn
		return nil
	}
}

// Node represents a distributed lock node for master-slave election and lock renewal. // 分布式锁节点，用于实现主从选举和锁续约
type Node struct {
	Id     string
	logger *slog.Logger

	key string
	ttl time.Duration

	r      *redis.Client
	fsm    *fsm.FSM
	ctx    context.Context
	cancel context.CancelFunc

	masterCallback func(ctx context.Context)
	nomalCallback  func(ctx context.Context)
	exitCallback   func(ctx context.Context)
}

// New creates a new Node instance. // 创建一个新的 Node 实例
// ctx: context to control lifecycle. // 上下文用于控制生命周期
// opts: optional configuration functions for Redis connection, ID, key name, and TTL. // 可选配置函数，用于设置 Redis 连接、ID、键名和 TTL
func New(ctx context.Context, opts ...Option) (*Node, error) {
	ctx, cancel := context.WithCancel(ctx)

	n := &Node{
		Id:  uuid.New().String(),
		key: "rkeeper:lock",
		ttl: time.Second * 1,

		ctx:    ctx,
		cancel: cancel,
	}
	for _, opt := range opts {
		opt(n)
	}
	if n.r == nil {
		return nil, ErrRedisConnectIsNil
	}

	n.logger = n.logger.With(slog.String("node_id", n.Id))

	loadFSM(n)
	return n, nil
}

func (n *Node) Status() string {
	return n.fsm.Current()
}

// Start starts the node's master-slave election and lock renewal logic. // 启动节点的主从选举和锁续约逻辑
// This method blocks until the context is cancelled. // 该方法会阻塞，直到上下文被取消
// Operation logic: // 运行逻辑
// 1. Create a ticker with TTL/2 interval to trigger renewal or election periodically. // 创建一个定时器，间隔为 TTL 的一半，用于定期触发续约或选举
// 2. Listen for context cancellation and ticker signals in a loop. // 在循环中监听上下文取消信号和定时器触发信号
// 3. If current node is master (StateMaster), try to renew the lock: // 如果当前节点是主节点（StateMaster），则尝试续约锁
//    - Renew success: trigger EventRenewSuccess event. // 续约成功：触发 EventRenewSuccess 事件
//    - Renew failure: trigger EventRenewDefeat event. // 续约失败：触发 EventRenewDefeat 事件
//
// 4. If current node is normal node, try to acquire the lock: // 如果当前节点是从节点，则尝试获取锁
//    - Acquire success: trigger EventElectoralSuccess event. // 获取成功：触发 EventElectoralSuccess 事件
//    - Acquire failure: trigger EventElectoralDefeat event. // 获取失败：触发 EventElectoralDefeat 事件
//
// Note: This method is not thread-safe and should be called in a separate goroutine. // 注意：该方法是非线程安全的，应在单独的协程中调用
func (n *Node) Start() {
	ticker := time.NewTicker(n.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			n.logger.InfoContext(n.ctx, "node stopped")
			if n.exitCallback != nil {
				n.exitCallback(n.ctx)
			}
			return
		default:
			select {
			case <-ticker.C:
				switch n.Status() {
				case StateMaster:
					// Try to renew the lock // 尝试续约锁
					if n.renew() {
						// Renew success, keep master state // 续约成功，保持主节点状态
						if err := n.fsm.Event(n.ctx, EventRenewSuccess); err != nil {
							n.logger.With(slog.String("status", n.Status())).
								ErrorContext(n.ctx, "renew event error: %v", slog.Any("err", err))
							continue
						}
						n.logger.With(slog.String("status", n.Status())).InfoContext(n.ctx, "renew success")
						continue
					}
					// Renew failure, enter normal node state // 续约失败，进入普通节点状态
					if err := n.fsm.Event(n.ctx, EventRenewDefeat); err != nil {
						n.logger.With(slog.String("status", n.Status())).
							ErrorContext(n.ctx, "renew defeat event error: %v", slog.Any("err", err))
						continue
					}
					n.logger.With(slog.String("status", n.Status())).InfoContext(n.ctx, "renew defeat")
				case StateNomal:
					// Try to acquire the lock // 尝试获取锁
					if n.tryAcquire() {
						// Acquire success, enter master state // 获取成功，进入主节点状态
						if err := n.fsm.Event(n.ctx, EventElectoralSuccess); err != nil {
							n.logger.With(slog.String("status", n.Status())).
								ErrorContext(n.ctx, "electoral success event error: %v", slog.Any("err", err))
							continue
						}
						n.logger.With(slog.String("status", n.Status())).InfoContext(n.ctx, "electoral success")
						continue
					}
					// Acquire failure, keep normal node state // 获取失败，保持普通节点状态
					if err := n.fsm.Event(n.ctx, EventElectoralDefeat); err != nil {
						n.logger.With(slog.String("status", n.Status())).
							ErrorContext(n.ctx, "electoral defeat event error: %v", slog.Any("err", err))
						continue
					}
					n.logger.With(slog.String("status", n.Status())).InfoContext(n.ctx, "electoral defeat")
				}
			default:
				n.logger.With(slog.String("status", n.Status())).WarnContext(n.ctx, "unexpected node status")
			}
		}
	}
}

const (
	renewScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
    	return 0
	end
	`
)

// renew tries to renew the lock held by the current node. // 尝试续约当前节点持有的锁
// Returns true for success, false for failure. // 返回 true 表示续约成功，false 表示失败
func (n *Node) renew() bool {
	expire := n.ttl / time.Millisecond
	result, err := n.r.Eval(n.ctx, renewScript, []string{n.key}, n.Id, expire).Result()
	if err != nil {
		n.logger.With(slog.String("status", n.Status())).
			ErrorContext(n.ctx, "renew error: %v", slog.Any("err", err))
		return false
	}
	resultInt, ok := result.(int64)
	if !ok {
		return false
	}
	return resultInt == 1
}

// tryAcquire tries to acquire the distributed lock. // 尝试获取分布式锁
// Returns true for success, false for failure. // 返回 true 表示获取成功，false 表示失败
func (n *Node) tryAcquire() bool {
	ok, err := n.r.SetNX(n.ctx, n.key, n.Id, n.ttl).Result()
	if err != nil {
		n.logger.With(slog.String("status", n.Status())).
			ErrorContext(n.ctx, "try acquire error: %v", slog.Any("err", err))
		return false
	}
	return ok
}

// enterMaster is the callback function when entering master state. // 进入主节点状态的回调函数
func (n *Node) enterMaster(ctx context.Context, e *fsm.Event) {
	n.logger.With(slog.String("status", n.Status())).
		InfoContext(ctx, "enter master", slog.String("src", e.Src), slog.String("dst", e.Dst))
	if e.Src == StateMaster {
		return
	}

	if n.masterCallback != nil {
		n.masterCallback(ctx)
	}
}

// enterNomal is the callback function when entering normal node state. // 进入普通节点状态的回调函数
func (n *Node) enterNomal(ctx context.Context, e *fsm.Event) {
	slog.InfoContext(ctx, "enter nomal", slog.String("src", e.Src), slog.String("dst", e.Dst))
	if e.Src == StateNomal {
		return
	}

	if n.nomalCallback != nil {
		n.nomalCallback(ctx)
	}
}
