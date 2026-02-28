package rkeeper

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
)

// TestNewNode tests the Node constructor New. // 测试 Node 的构造函数 New
// Verify correctness of different configuration options, including Redis connection, ID, key name, and TTL. // 验证不同配置选项的正确性，包括 Redis 连接、ID、键名和 TTL
func TestNewNode(t *testing.T) {
	t.Run("WithValidOptions", func(t *testing.T) {
		ctx := context.Background()
		rdb, _ := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithID("test-id"), WithKey("test-key"), WithTTL(time.Second))
		assert.NoError(t, err)
		assert.NotNil(t, n)
		assert.Equal(t, "test-id", n.Id)
		assert.Equal(t, "test-key", n.key)
		assert.Equal(t, time.Second, n.ttl)
		assert.Equal(t, rdb, n.r)
	})

	t.Run("WithNilRedisConnect", func(t *testing.T) {
		ctx := context.Background()
		n, err := New(ctx)
		assert.Error(t, err)
		assert.Nil(t, n)
		assert.Equal(t, ErrRedisConnectIsNil, err)
	})
}

// TestNodeStatus tests the Node status management method Status. // 测试 Node 的状态管理方法 Status
// Verify that status is returned correctly. // 验证状态是否正确返回
func TestNodeStatus(t *testing.T) {
	t.Run("InitialStatus", func(t *testing.T) {
		ctx := context.Background()
		rdb, _ := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb))
		assert.NoError(t, err)
		assert.Equal(t, StateNomal, n.Status())
	})

	t.Run("SetStatus", func(t *testing.T) {
		ctx := context.Background()
		rdb, _ := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb))
		assert.NoError(t, err)
		n.fsm.SetState(StateMaster)
		assert.Equal(t, StateMaster, n.Status())
	})
}

// TestNodeStart tests the Node's master-slave election and lock renewal logic Start. // 测试 Node 的主从选举和锁续约逻辑 Start
// Verify master-slave switching and callback function triggering logic. // 验证主从切换和回调函数的触发逻辑
func TestNodeStart(t *testing.T) {

	// Election success: switch from nomal to master // 选举成功,从nomal切换到master
	t.Run("ElectionSuccess", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithTTL(100*time.Millisecond))
		assert.NoError(t, err)

		// Mock successful lock acquisition // 模拟获取锁成功
		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(true)
		// Mock successful renewal // 模拟续约成功
		mock.ExpectEval(renewScript, []string{n.key}, n.Id, n.ttl/time.Millisecond).SetVal(int64(1))

		go n.Start()
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, StateMaster, n.Status())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	// Election failure: stay in nomal state // 选举失败,从nomal切换到nomal
	t.Run("ElectionFailure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithTTL(100*time.Millisecond))
		assert.NoError(t, err)

		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(false)
		go n.Start()
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, StateNomal, n.Status())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	// Renewal success: stay in master state // 续约成功,从master切换到master
	t.Run("RenewSuccess", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithTTL(100*time.Millisecond))
		assert.NoError(t, err)

		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(true)
		mock.ExpectEval(renewScript, []string{n.key}, n.Id, n.ttl/time.Millisecond).SetVal(int64(1))
		go n.Start()

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, StateMaster, n.Status())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	// Renewal failure: switch from master to nomal // 续约失败,从master切换到nomal
	t.Run("RenewFailure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithTTL(100*time.Millisecond))
		assert.NoError(t, err)

		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(true)
		mock.ExpectEval(renewScript, []string{n.key}, n.Id, n.ttl/time.Millisecond).SetVal(int64(0))
		go n.Start()

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, StateNomal, n.Status())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	// Election success: switch from nomal to master, trigger master callback // 选举成功,从nomal切换到master,触发master回调
	t.Run("ElectionSuccessAndMasterCallbackTriggered", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var masterCalled bool
		var masterCalledCount int
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithTTL(100*time.Millisecond), WithMasterCallback(func(ctx context.Context) {
			masterCalledCount++
			masterCalled = true
		}))
		assert.NoError(t, err)

		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(true)
		mock.ExpectEval(renewScript, []string{n.key}, n.Id, n.ttl/time.Millisecond).SetVal(int64(1))
		go n.Start()

		// Wait for callback to be triggered // 等待回调触发
		time.Sleep(500 * time.Millisecond)
		assert.True(t, masterCalled)
		assert.Equal(t, 1, masterCalledCount)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	// Election failure: stay in nomal state, nomal callback not triggered // 选举失败,从nomal切换到nomal,不触发nomal回调
	t.Run("ElectionFailureAndNomalCallbackNotTriggered", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var nomalCalled bool
		var nomalCalledCount int
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithTTL(100*time.Millisecond), WithNomalCallback(func(ctx context.Context) {
			nomalCalledCount++
			nomalCalled = true
		}))
		assert.NoError(t, err)

		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(false)
		go n.Start()

		// Wait for callback to be triggered // 等待回调触发
		time.Sleep(500 * time.Millisecond)
		assert.False(t, nomalCalled)
		assert.Equal(t, 0, nomalCalledCount)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	// Election failure: switch from master to nomal, trigger nomal callback // 选举失败 从master切换到nomal 触发nomal回调
	t.Run("ElectionFailureAndMasterCallbackTriggered", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var nomalCalled bool
		var nomalCalledCount int
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithTTL(100*time.Millisecond), WithNomalCallback(func(ctx context.Context) {
			nomalCalledCount++
			nomalCalled = true
		}))
		assert.NoError(t, err)

		// Mock lock acquisition failure // 模拟获取锁失败
		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(true)
		mock.ExpectEval(renewScript, []string{n.key}, n.Id, n.ttl/time.Millisecond).SetVal(int64(0))
		go n.Start()

		// Wait for callback to be triggered // 等待回调触发
		time.Sleep(500 * time.Millisecond)
		assert.True(t, nomalCalled)
		assert.Equal(t, 1, nomalCalledCount)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestNodeRenew tests the Node's lock renewal method renew. // 测试 Node 的锁续约方法 renew
// Verify renewal success and failure logic. // 验证续约成功和失败的逻辑
func TestNodeRenew(t *testing.T) {
	t.Run("RenewSuccess", func(t *testing.T) {
		ctx := context.Background()
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithKey("test-key"), WithTTL(time.Second))
		assert.NoError(t, err)

		// Mock successful renewal // 模拟续约成功
		mock.ExpectEval(renewScript, []string{n.key}, n.Id, n.ttl/time.Millisecond).SetVal(int64(1))

		assert.True(t, n.renew())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RenewFailure", func(t *testing.T) {
		ctx := context.Background()
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithKey("test-key"), WithTTL(time.Second))
		assert.NoError(t, err)

		// Mock renewal failure // 模拟续约失败
		mock.ExpectEval(renewScript, []string{n.key}, n.Id, n.ttl/time.Millisecond).SetVal(int64(0))

		assert.False(t, n.renew())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestNodeTryAcquire tests the Node's lock acquisition method tryAcquire. // 测试 Node 的锁获取方法 tryAcquire
// Verify acquisition success and failure logic. // 验证获取成功和失败的逻辑
func TestNodeTryAcquire(t *testing.T) {
	t.Run("AcquireSuccess", func(t *testing.T) {
		ctx := context.Background()
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithKey("test-key"), WithTTL(time.Second))
		assert.NoError(t, err)

		// Mock successful lock acquisition // 模拟获取锁成功
		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(true)
		assert.True(t, n.tryAcquire())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("AcquireFailure", func(t *testing.T) {
		ctx := context.Background()
		rdb, mock := redismock.NewClientMock()
		n, err := New(ctx, WithConnect(rdb), WithKey("test-key"), WithTTL(time.Second))
		assert.NoError(t, err)

		// Mock lock acquisition failure // 模拟获取锁失败
		mock.ExpectSetNX(n.key, n.Id, n.ttl).SetVal(false)
		assert.False(t, n.tryAcquire())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
