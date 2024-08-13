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
	fd            int
	currentOffset int64
	totalOffset   int64
	mmapedRegion  []byte
	entrySize     int
	runningCount  int
	syncStrategy  SyncStrategy[T]
}

type queueEntry[T comparable] struct {
	entry    T
	metadata uint32
}

type checkedQueueEntry[T comparable] struct {
	entry       T
	statusCrc32 uint64
}

type SyncStrategy[T comparable] interface {
	Submit(T) fasterror.Error
	Sync(fd int, region []byte) fasterror.Error
}

// Create initializes a new queue backed by the file
//
// This function overwrites the file so calling Create on a file
// after recovery will lose the content
//
// runnningCount determins the amounts of entries in memory
func Create[T comparable](file string, runningCount int64, syncStrategy SyncStrategy[T]) (QueueWriter[T], error) {
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

	return queue, nil
}

var koopman = crc32.MakeTable(crc32.Koopman)

const (
	// recovery on prepublished fields are considered published
	// prepublished marks that at the time of crash a sync wasn't
	// confirmed, after recovery it is clear that the value
	// was synced.
	//
	// PrePublished entries are to be replayed as published on
	// recovery
	QueueEntryStatusPrePublished = 1
	// the value is accessable for consumers and has been synced
	// in accordance with the sync strategy.
	//
	// Published entries are to be replayed on recovery
	QueueEntryStatusPublished = 2
	// the consumer has started work on this entry
	// because this queue has no transactions
	// the consumer might crash before finalizing
	// its work. Recovery must be handled appropriately.
	QueueEntryStatusConsuming = 3
	// the entry has been completly consumed and no replay
	// is necessary
	QueueEntryStatusConsumed = 4
	// upon reading a value its integrity couldn't be
	// ensured with error checking code
	//
	// this flag is virtual and not stored only replayed
	// no data of a corrupted entry can be trusted
	QueueEntryStatusCorrupted = 5
)

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
		regionSize := queue.runningCount * queue.entrySize

		err := unix.Msync(queue.mmapedRegion, unix.MS_ASYNC)
		if err != nil {
			return fasterror.Create("Failed to async msync mmaped region for new allocation")
		}

		err = unix.Ftruncate(queue.fd, queue.totalOffset+int64(regionSize*2))
		if err != nil {
			return fasterror.Create("Failed to extended storage file via ftruncate")
		}

		newRegion, err := unix.Mmap(
			queue.fd,
			queue.totalOffset+int64(regionSize),
			int(regionSize),
			unix.PROT_READ|unix.PROT_WRITE,
			unix.MAP_SHARED,
		)
		if err != nil {
			return fasterror.Create("Failed to create extended mmap")
		}

		err = unix.Munmap(queue.mmapedRegion)
		if err != nil {
			err = unix.Munmap(newRegion)
			if err != nil {
				return fasterror.Create("Failed to unmap new region after failing to unmap old region after extending")
			}
			return fasterror.Create("Failed to unmap the old region after extending")
		}

		queue.totalOffset += int64(regionSize)
		queue.currentOffset = 0
		queue.mmapedRegion = newRegion
	}

	entry := queueEntry[T]{
		entry:    t,
		metadata: QueueEntryStatusPrePublished,
	}
	// prepare publish
	uptr := unsafe.Pointer(&entry.entry)
	slice := unsafe.Slice((*byte)(uptr), unsafe.Sizeof(entry.entry))
	crc := crc32.Checksum(slice, koopman)
	ptr := (*checkedQueueEntry[T])(unsafe.Pointer(&queue.mmapedRegion[queue.currentOffset]))
	ptr.entry = t
	atomic.StoreUint64(&ptr.statusCrc32, (uint64(QueueEntryStatusPrePublished)<<32)+uint64(crc))
	queue.currentOffset += int64(queue.entrySize)
	
	// sync
	err = queue.syncStrategy.Sync(queue.fd, queue.mmapedRegion)
	if !err.IsNil() {
		return err
	}

	// ready to consume
	entry.metadata = QueueEntryStatusPublished
	crc = crc32.Checksum(slice, koopman)
	atomic.StoreUint64(&ptr.statusCrc32, (uint64(QueueEntryStatusPublished)<<32)+uint64(crc))
	return fasterror.Nil()
}
