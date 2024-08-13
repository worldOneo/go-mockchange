package queue

import (
	"fmt"
	"hash/crc32"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/worldOneo/go-mockchange/fasterror"
	"golang.org/x/sys/unix"
)

// QueueWriter is the pushing side of a queue
// it can only write
type QueueWriter[T comparable] struct {
	currentOffset int64
	totalOffset   int64
	fd            int
	runningCount  int
	entrySize     int
	pagesize      int
	syncStrategy  SyncStrategy[T]
	mmapedRegion  []byte
	tmp           queueEntry[T]
}

type queueEntry[T comparable] struct {
	entry  T
	status uint32
}

type checkedQueueEntry[T comparable] struct {
	entry       T
	statusCrc32 uint64
}

type SyncStrategy[T comparable] interface {
	Submit(T) fasterror.Error
	Sync(fd int, region []byte) fasterror.Error
}

// Writer initializes a new queue backed by the file
//
// This function overwrites the file so calling Writer on a file
// after recovery will lose the content
//
// runnningCount determins the amounts of entries in memory
func Writer[T comparable](file string, runningCount int64, syncStrategy SyncStrategy[T]) (QueueWriter[T], error) {
	var queue QueueWriter[T]
	// open backing storage
	fd, err := unix.Open(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o666)
	if err != nil {
		return queue, fmt.Errorf("failed to create persisted file: %v", err)
	}

	var t checkedQueueEntry[T]
	var entrySize = unsafe.Sizeof(t)
	var memSize = entrySize * uintptr(runningCount)

	// making space in the backing file
	err = unix.Ftruncate(fd, int64(memSize))
	if err != nil {
		return queue, fmt.Errorf("failed to ensure space in file: %v", err)
	}

	// mapping file to writable region
	region, err := unix.Mmap(fd, 0, int(memSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		err2 := unix.Close(fd)
		if err2 != nil {
			return queue, fmt.Errorf("failed to close queue fd after failing to mmap the fd: %v; initial error was: %v", err2, err)
		}
		return queue, fmt.Errorf("failed to mmap the fd: %v", err)
	}

	queue.fd = fd
	queue.mmapedRegion = region
	queue.entrySize = int(entrySize)
	queue.runningCount = int(runningCount)
	queue.currentOffset = 0
	queue.syncStrategy = syncStrategy
	queue.totalOffset = 0
	queue.pagesize = os.Getpagesize()

	return queue, nil
}

var koopman = crc32.MakeTable(crc32.Koopman)

type QueueStatus = uint32

const (
	// recovery on prepublished fields are considered published
	// prepublished marks that at the time of crash a sync wasn't
	// confirmed, after recovery it is clear that the value
	// was synced.
	//
	// PrePublished entries are to be replayed as published on
	// recovery
	EntryStatusPrePublished QueueStatus = 1
	// the value is accessable for consumers and has been synced
	// in accordance with the sync strategy.
	//
	// Published entries are to be replayed on recovery
	EntryStatusPublished QueueStatus = 2
	// the consumer has started work on this entry
	// because this queue has no transactions
	// the consumer might crash before finalizing
	// its work. Recovery must be handled appropriately.
	EntryStatusConsuming QueueStatus = 3
	// the entry has been completly consumed and no replay
	// is necessary
	EntryStatusConsumed QueueStatus = 4
	// upon reading a value its integrity couldn't be
	// ensured with error checking code
	//
	// this flag is virtual and not stored only replayed
	// no data of a corrupted entry can be trusted
	EntryStatusCorrupted QueueStatus = 5
)

func moveFdMapping(fd int, oldRegion []byte, regionSize int64, totalOffset int64, pageSize int, ftrunc bool) (int64, []byte, fasterror.Error) {
	var data []byte
	err := unix.Msync(oldRegion, unix.MS_ASYNC)
	if err != nil {
		return 0, data, fasterror.Create("Failed to async msync mmaped region for new allocation")
	}

	if ftrunc {
		err = unix.Ftruncate(fd, totalOffset+regionSize*2)
		if err != nil {
			return 0, data, fasterror.Create("Failed to extended storage file via ftruncate")
		}
	}

	// page size math
	desiredOffset := totalOffset + int64(regionSize)
	pageCountBelowOffset := desiredOffset / int64(pageSize)
	pageBasedOffset := pageCountBelowOffset * int64(pageSize)
	pageDesiredOffsetDelta := desiredOffset - pageBasedOffset

	newRegion, err := unix.Mmap(
		fd,
		pageBasedOffset,
		int(regionSize)+int(pageDesiredOffsetDelta),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		fmt.Printf("Err: %v\n", err)
		return 0, data, fasterror.Create("Failed to create extended mmap")
	}

	err = unix.Munmap(oldRegion)
	if err != nil {
		err = unix.Munmap(newRegion)
		if err != nil {
			return 0, data, fasterror.Create("Failed to unmap new region after failing to unmap old region after extending")
		}
		return 0, data, fasterror.Create("Failed to unmap the old region after extending")
	}

	return pageDesiredOffsetDelta, newRegion, fasterror.Nil()
}

// Write writes a new entry to the queue
//
// Returning no error guarantees that the entry has been
// synced according to the sync strategy and that the entry
// has been made visible for a consumer
//
// Returning an error guarantees that the entry will not be published
// while this process is still running.
//
// An error usually requires recovery because a consumer might not be
// able to read an erroneous and will not notice this
func (queue *QueueWriter[T]) Write(t T) fasterror.Error {

	err := queue.syncStrategy.Submit(t)
	if !err.IsNil() {
		return err
	}

	if queue.currentOffset >= int64(len(queue.mmapedRegion)) {
		regionSize := int64(queue.entrySize) * int64(queue.runningCount)
		delta, newRegion, err := moveFdMapping(
			queue.fd,
			queue.mmapedRegion,
			regionSize,
			queue.totalOffset,
			queue.pagesize,
			true,
		)
		if !err.IsNil() {
			return err
		}
		queue.totalOffset += regionSize
		queue.currentOffset = delta
		queue.mmapedRegion = newRegion
	}

	queue.tmp = queueEntry[T]{
		entry:  t,
		status: EntryStatusPrePublished,
	}
	// prepare publish
	uptr := unsafe.Pointer(&queue.tmp)
	slice := unsafe.Slice((*byte)(uptr), unsafe.Sizeof(queue.tmp))
	crc := crc32.Checksum(slice, koopman)
	ptr := (*checkedQueueEntry[T])(unsafe.Pointer(&queue.mmapedRegion[queue.currentOffset]))
	ptr.entry = t
	atomic.StoreUint64(&ptr.statusCrc32, (uint64(EntryStatusPrePublished)<<32)+uint64(crc))
	queue.currentOffset += int64(queue.entrySize)

	// sync
	err = queue.syncStrategy.Sync(queue.fd, queue.mmapedRegion)
	if !err.IsNil() {
		return err
	}

	// ready to consume
	queue.tmp.status = EntryStatusPublished
	crc = crc32.Checksum(slice, koopman)
	atomic.StoreUint64(&ptr.statusCrc32, (uint64(EntryStatusPublished)<<32)+uint64(crc))
	return fasterror.Nil()
}

// QueueReader is the pulling side of a queue
// it can only write
type QueueReader[T comparable] struct {
	currentOffset int64
	totalOffset   int64
	fd            int
	entrySize     int
	runningCount  int
	pagesize      int
	syncStrategy  SyncStrategy[T]
	mmapedRegion  []byte
	fstat         unix.Stat_t
	tmp           queueEntry[T]
	recovery      bool
}

// Reader initializes a new reader backed by the file
//
// in recovery mode reader will also produce entries
// with the status QueueEntryStatusPrePublished
//
// runningCount must match the Writers running count
func Reader[T comparable](file string, runningCount int64, syncStrategy SyncStrategy[T], recovery bool) (QueueReader[T], error) {
	var queue QueueReader[T]
	// open backing storage
	fd, err := unix.Open(file, os.O_RDWR, 0o666)
	if err != nil {
		return queue, fmt.Errorf("failed to open persisted file: %v", err)
	}

	var t checkedQueueEntry[T]
	var entrySize = unsafe.Sizeof(t)
	var memSize = entrySize * uintptr(runningCount)

	// mapping file to writable region
	region, err := unix.Mmap(fd, 0, int(memSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		err2 := unix.Close(fd)
		if err2 != nil {
			return queue, fmt.Errorf("failed to close queue fd after failing to mmap the fd: %v; initial error was: %v", err2, err)
		}
		return queue, fmt.Errorf("failed to mmap the fd: %v", err)
	}

	// check if data is present
	err = unix.Fstat(fd, &queue.fstat)
	if err != nil {
		err2 := unix.Munmap(region)
		err3 := unix.Close(fd)
		if err2 != nil && err3 != nil {
			return queue, fmt.Errorf("failed to unmap region: %v and closing the fd: %v after failing fstat: %v", err2, err3, err)
		}
		if err2 != nil {
			return queue, fmt.Errorf("failed to unmap region: %v after failing fstat: %v", err2, err)
		}
		if err3 != nil {
			return queue, fmt.Errorf("failed to close the fd: %v after failing fstat: %v", err3, err)
		}
		return queue, err
	}

	queue.fd = fd
	queue.mmapedRegion = region
	queue.entrySize = int(entrySize)
	queue.runningCount = int(runningCount)
	queue.currentOffset = 0
	queue.syncStrategy = syncStrategy
	queue.totalOffset = 0
	queue.pagesize = os.Getpagesize()
	queue.recovery = recovery

	return queue, nil
}

// Read reads data from the queue marking the first
// readable entry as consuming
//
// When hitting a corrupt entry the data isn't modified
// and the reader cannot continue
func (queue *QueueReader[T]) Read() (T, QueueStatus, fasterror.Error) {
	for {
		var t T
		status := EntryStatusCorrupted
		regionSize := int64(queue.entrySize) * int64(queue.runningCount)
		for queue.fstat.Size < queue.totalOffset+regionSize {
			err := unix.Fstat(queue.fd, &queue.fstat)
			if err != nil {
				return t, status, fasterror.Create("Failed to fstat queue file")
			}
		}

		if queue.currentOffset >= int64(len(queue.mmapedRegion)) {
			delta, newRegion, err := moveFdMapping(
				queue.fd,
				queue.mmapedRegion,
				regionSize,
				queue.totalOffset,
				queue.pagesize,
				false,
			)
			if !err.IsNil() {
				return t, status, err
			}
			queue.totalOffset += regionSize
			queue.currentOffset = delta
			queue.mmapedRegion = newRegion
		}

		memPtr := &queue.mmapedRegion[queue.currentOffset]
		ptr := (*checkedQueueEntry[T])(unsafe.Pointer(memPtr))

		for {
			statusCrc := atomic.LoadUint64(&ptr.statusCrc32)
			status := uint32(statusCrc >> 32)
			receivedCrc := uint32(statusCrc & ((1 << 32) - 1))

			queue.tmp = queueEntry[T]{
				entry:  ptr.entry,
				status: status,
			}

			entryPtr := unsafe.Pointer(&queue.tmp)
			entryDataSlice := unsafe.Slice((*byte)(entryPtr), unsafe.Sizeof(queue.tmp))
			checkedCrc := crc32.Checksum(entryDataSlice, koopman)

			if receivedCrc != checkedCrc {
				return queue.tmp.entry, EntryStatusCorrupted, fasterror.Nil()
			}
			if status == EntryStatusConsumed {
				break
			}

			statusReadableRecovery := status == EntryStatusPrePublished || status == EntryStatusConsuming

			if (statusReadableRecovery && queue.recovery) || (status == EntryStatusPublished) {
				queue.tmp.status = EntryStatusConsuming
				consumingCrc := crc32.Checksum(entryDataSlice, koopman)
				consumingStatusCrc := (uint64(EntryStatusConsuming) << 32) + uint64(consumingCrc)
				atomic.StoreUint64(&ptr.statusCrc32, consumingStatusCrc)
				err := queue.syncStrategy.Submit(queue.tmp.entry)
				if !err.IsNil() {
					return t, status, err
				}
				err = queue.syncStrategy.Sync(queue.fd, queue.mmapedRegion)
				if !err.IsNil() {
					return t, status, err
				}
				queue.currentOffset += int64(queue.entrySize)
				return queue.tmp.entry, status, fasterror.Nil()
			}
		}
	}
}

// FinishRead marks the current entry as Consumed
// and syncs that change in accordance with the sync strategy
// func (queue *QueueReader[T]) Read() (T, QueueStatus, fasterror.Error) {
// }
