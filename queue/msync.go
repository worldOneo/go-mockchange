package queue

import (
	"time"

	"github.com/worldOneo/go-mockchange/fasterror"
	"golang.org/x/sys/unix"
)

type mSyncStrategy[T comparable] struct {
	timeBased bool
	current   uint64
	parameter uint64
	flag      int
}

func (strategy *mSyncStrategy[T]) Submit(t T) fasterror.Error {
	return fasterror.Nil()
}

func (strategy *mSyncStrategy[T]) Sync(fd int, region []byte) fasterror.Error {
	var condition = false
	if strategy.timeBased {
		nsnow := uint64(time.Now().UnixNano())
		condition = nsnow > strategy.current+strategy.parameter
		if condition {
			strategy.current = nsnow
		}
	} else {
		strategy.current += 1
		condition = strategy.current > strategy.parameter
		if condition {
			strategy.current = 0
		}
	}

	if !condition {
		return fasterror.Nil()
	}
	err := unix.Msync(region, strategy.flag)
	if err != nil {
		return fasterror.Create("failed to MSync")
	}
	return fasterror.Nil()
}

func MSyncRolling[T comparable](syncAfterNth uint64) SyncStrategy[T] {
	return &mSyncStrategy[T]{
		timeBased: false,
		current:   0,
		parameter: syncAfterNth,
		flag: unix.MS_SYNC,
	}
}

func MSyncTimeBased[T comparable](nanoseconds uint64) SyncStrategy[T] {
	return &mSyncStrategy[T]{
		timeBased: false,
		current:   0,
		parameter: nanoseconds,
		flag: unix.MS_SYNC,
	}
}

func MAsyncRolling[T comparable](syncAfterNth uint64) SyncStrategy[T] {
	return &mSyncStrategy[T]{
		timeBased: false,
		current:   0,
		parameter: syncAfterNth,
		flag: unix.MS_ASYNC,
	}
}

func MAsyncTimeBased[T comparable](nanoseconds uint64) SyncStrategy[T] {
	return &mSyncStrategy[T]{
		timeBased: false,
		current:   0,
		parameter: nanoseconds,
		flag: unix.MS_ASYNC,
	}
}