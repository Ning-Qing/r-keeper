package rkeeper

import "errors"

var (
	ErrRedisConnectIsNil = errors.New("redis connect is nil")
)
