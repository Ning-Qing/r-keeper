package rkeeper

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/looplab/fsm"
	"github.com/redis/go-redis/v9"
)

// Option 定义了一个函数类型，用于配置 Node 实例。
type Option func(*Node) error

func WithLogger(logger *slog.Logger) Option {
	return func(n *Node) error {
		n.logger = logger
		return nil
	}
}

// WithConnect 返回一个 Option 函数，用于设置 Redis 连接。
func WithConnect(connect *redis.Client) Option {
	return func(n *Node) error {
		n.r = connect
		return nil
	}
}

// WithID 返回一个 Option 函数，用于设置节点 ID。
// ID 用于标识节点，默认值为 UUID。
func WithID(id string) Option {
	return func(n *Node) error {
		n.Id = id
		return nil
	}
}

// WithKey 返回一个 Option 函数，用于设置键名。
// Key 用于存储锁的键名，默认值为 "rkeeper:lock"。
func WithKey(key string) Option {
	return func(n *Node) error {
		n.key = key
		return nil
	}
}

// WithTTL 返回一个 Option 函数，用于设置 TTL。
// TTL 用于设置锁的有效期，默认值为 1 秒。
func WithTTL(ttl time.Duration) Option {
	return func(n *Node) error {
		n.ttl = ttl
		return nil
	}
}

// WithMasterCallback 返回一个 Option 函数，用于设置主节点状态回调函数。
// fn 用于在主节点状态下执行的逻辑，默认值为空。
func WithMasterCallback(fn func(ctx context.Context)) Option {
	return func(n *Node) error {
		n.masterCallback = fn
		return nil
	}
}

// WithNomalCallback 返回一个 Option 函数，用于设置普通节点状态回调函数。
// fn 用于在普通节点状态下执行的逻辑，默认值为空。
func WithNomalCallback(fn func(ctx context.Context)) Option {
	return func(n *Node) error {
		n.nomalCallback = fn
		return nil
	}
}

// WithExitCallback 返回一个 Option 函数，用于设置节点退出回调函数。
// fn 用于在节点退出时执行的逻辑，默认值为空。
func WithExitCallback(fn func(ctx context.Context)) Option {
	return func(n *Node) error {
		n.exitCallback = fn
		return nil
	}
}

// Node 表示一个分布式锁节点，用于实现主从选举和锁续约。
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

// New 创建一个新的 Node 实例。
// ctx: 上下文用于控制生命周期。
// opts: 可选配置函数，用于设置 Redis 连接、ID、键名和 TTL。
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

// Start 启动节点的主从选举和锁续约逻辑。
// 该方法会阻塞，直到上下文被取消。
// 运行逻辑：
// 1. 创建一个定时器，间隔为 TTL 的一半，用于定期触发续约或选举。
// 2. 在循环中监听上下文取消信号和定时器触发信号。
// 3. 如果当前节点是主节点（StateMaster），则尝试续约锁：
//   - 续约成功：触发 EventRenewSuccess 事件。
//   - 续约失败：触发 EventRenewDefeat 事件。
//
// 4. 如果当前节点是从节点，则尝试获取锁：
//   - 获取成功：触发 EventElectoralSuccess 事件。
//   - 获取失败：触发 EventElectoralDefeat 事件。
//
// 注意：该方法是非线程安全的，应在单独的协程中调用。
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
					// 尝试续约锁
					if n.renew() {
						// 续约成功，保持主节点状态
						if err := n.fsm.Event(n.ctx, EventRenewSuccess); err != nil {
							n.logger.With(slog.String("status", n.Status())).
								ErrorContext(n.ctx, "renew event error: %v", slog.Any("err", err))
							continue
						}
						n.logger.With(slog.String("status", n.Status())).InfoContext(n.ctx, "renew success")
						continue
					}
					// 续约失败，进入普通节点状态
					if err := n.fsm.Event(n.ctx, EventRenewDefeat); err != nil {
						n.logger.With(slog.String("status", n.Status())).
							ErrorContext(n.ctx, "renew defeat event error: %v", slog.Any("err", err))
						continue
					}
					n.logger.With(slog.String("status", n.Status())).InfoContext(n.ctx, "renew defeat")
				case StateNomal:
					// 尝试获取锁
					if n.tryAcquire() {
						// 获取成功，进入主节点状态
						if err := n.fsm.Event(n.ctx, EventElectoralSuccess); err != nil {
							n.logger.With(slog.String("status", n.Status())).
								ErrorContext(n.ctx, "electoral success event error: %v", slog.Any("err", err))
							continue
						}
						n.logger.With(slog.String("status", n.Status())).InfoContext(n.ctx, "electoral success")
						continue
					}
					// 获取失败，保持普通节点状态
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

// renew 尝试续约当前节点持有的锁。
// 返回 true 表示续约成功，false 表示失败。
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

// tryAcquire 尝试获取分布式锁。
// 返回 true 表示获取成功，false 表示失败。
func (n *Node) tryAcquire() bool {
	ok, err := n.r.SetNX(n.ctx, n.key, n.Id, n.ttl).Result()
	if err != nil {
		n.logger.With(slog.String("status", n.Status())).
			ErrorContext(n.ctx, "try acquire error: %v", slog.Any("err", err))
		return false
	}
	return ok
}

// enterMaster 进入主节点状态的回调函数。
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

// enterNomal 进入普通节点状态的回调函数。
func (n *Node) enterNomal(ctx context.Context, e *fsm.Event) {
	slog.InfoContext(ctx, "enter nomal", slog.String("src", e.Src), slog.String("dst", e.Dst))
	if e.Src == StateNomal {
		return
	}

	if n.nomalCallback != nil {
		n.nomalCallback(ctx)
	}
}
