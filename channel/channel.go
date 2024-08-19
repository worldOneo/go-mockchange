package channel

import (
	"fmt"
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Receiver is a monodirectional communication primitive
// of simple structs (cannot include pointers)
type Receiver[T comparable] struct {
	conn     net.Conn
	listener net.Listener
}

// Receive creates a new receiver with the given string
// as unix socket filename.
//
// It waits until a connections is made
func Receive[T comparable](filename string) (Receiver[T], error) {
	var recv Receiver[T]
	listener, err := net.Listen("unix", filename)
	if err != nil {
		return recv, fmt.Errorf("cant create receiver for sock %s: %v", filename, err)
	}
	conn, err := listener.Accept()
	if err != nil {
		return recv, fmt.Errorf("cant accept connection for sock %s: %v", filename, err)
	}
	recv.conn = conn
	recv.listener = listener
	return recv, nil
}

func (receiver Receiver[T]) Receive() (T, error) {
	var t T
	uptr := unsafe.Pointer(&t)
	slice := unsafe.Slice((*byte)(uptr), unsafe.Sizeof(t))
	_, err := receiver.conn.Read(slice)
	return t, err
}

func (receiver Receiver[T]) Close() error {
	receiver.conn.Close()
	return receiver.listener.Close()
}

// Sender is a monodirectional communication primitive to send data
type Sender[T comparable] struct {
	conn net.Conn
}

// Send creates a new sender with the given string
// as unix filename.
//
// It waits until a connections is made
func Send[T comparable](filename string) (Sender[T], error) {
	conn, err := net.Dial("unix", filename)
	return Sender[T]{
		conn: conn,
	}, err
}

func (sender Sender[T]) Send(t T) error {
	uptr := unsafe.Pointer(&t)
	slice := unsafe.Slice((*byte)(uptr), unsafe.Sizeof(t))
	_, err := sender.conn.Write(slice)
	return err
}

func (sender Sender[T]) Close() error {
	return sender.conn.Close()
}

type BusConfig = uint

const (
	// BusBlocking does block writes when not all readers
	// have consumed an event.
	BusBlocking BusConfig = 0
	// BusColapsing does terminate a reader when the reader
	// misses an event
	BusColapsing BusConfig = 1
	// BusSkipping allows readers to skip events and catchup
	// if they missed some
	BusSkipping BusConfig = 2
)

type event[T comparable] struct {
	event       T
	statusCount uint64
}

const (
	eventStatusEmpty   = 0
	eventStatusWriting = 1
	eventStatusWritten = 2
)

type EventBus[T comparable] struct {
	events      []event[T]
	readerCount int
	config      BusConfig
	fd          int
}

// MakeBus initializes an event bus on the given file with entryCount many places
func MakeBus[T comparable](filename string, busConfig BusConfig, entryCount uint64) (EventBus[T], error) {
	var bus EventBus[T]

	fd, err := unix.Open(filename, unix.O_CREAT|unix.O_RDWR, 0o666)
	if err != nil {
		return bus, fmt.Errorf("can't open file: %v", err)
	}

	var t event[T]
	busBytes := unsafe.Sizeof(t) * uintptr(entryCount)
	err = unix.Ftruncate(fd, int64(busBytes))
	if err != nil {
		return bus, fmt.Errorf("can't resize file: %v", err)
	}

	region, err := unix.Mmap(fd, 0, int(busBytes), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		err2 := unix.Close(fd)
		if err2 != nil {
			return bus, fmt.Errorf("failed to close file after failing to mmap file: %v; initial error was: %v", err2, err)
		}
		return bus, fmt.Errorf("can't map file: %v", err)
	}
	entries := unsafe.Slice(((*event[T])(unsafe.Pointer(&region[0]))), entryCount)

	bus.fd = fd
	bus.events = entries
	return bus, nil
}

type Reader[T comparable] struct {
	bus        *EventBus[T]
	head       uint64
	counter    uint64
	entryCount uint64
}

type Writer[T comparable] struct {
	bus     *EventBus[T]
	head    uint64
	counter uint64
}
