package channel

import (
	"fmt"
	"net"
	"unsafe"
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
