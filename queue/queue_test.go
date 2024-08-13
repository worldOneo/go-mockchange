package queue

import (
	"fmt"
	"log"
	"testing"
)

func TestQueueWriter_write(t *testing.T) {
	dir := t.TempDir()
	q, err := Writer(fmt.Sprintf("%s/tmpfile.q", dir), 100, MAsyncRolling[int](50))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1_000; i++ {
		err := q.Write(i)
		if !err.IsNil() {
			t.Fatalf("Failed to write %d: %s", i, err.ToString())
		}
	}
	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueueReader_read(t *testing.T) {
	dir := t.TempDir()
	q, err := Writer(fmt.Sprintf("%s/tmpfile.q", dir), 100, MAsyncRolling[int](50))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1_000; i++ {
		err := q.Write(i)
		if !err.IsNil() {
			t.Fatalf("Failed to write %d: %s", i, err.ToString())
		}
	}

	r, err := Reader(fmt.Sprintf("%s/tmpfile.q", dir), 100, MAsyncRolling[int](50), false)
	for i := 0; i < 1_000; i++ {
		entry, status, err := r.Read()
		if !err.IsNil() {
			t.Fatalf("Failed to read back %d: %s", i, err.ToString())
		}
		if status != EntryStatusPublished {
			t.Fatalf("Failed to read back, mismatched status %d: %v, v=%v", i, status, entry)
		}
		if entry != i {
			t.Fatalf("Invalid data readback %d: %d", i, entry)
		}
		err = r.FinishRead()
		if !err.IsNil() {
			t.Fatalf("Failed to finish read %d: %s", i, err.ToString())
		}
	}

	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkQueueWriter(b *testing.B) {
	dir := b.TempDir()
	q, err := Writer(fmt.Sprintf("%s/tmpfile.q", dir), 1000, MAsyncRolling[int](1000))
	b.ResetTimer()
	b.ReportAllocs()
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		err := q.Write(i)
		if !err.IsNil() {
			log.Fatalf(err.ToString())
		}
	}
	err = q.Close()
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkQueueReader(b *testing.B) {
	dir := b.TempDir()
	q, err := Writer(fmt.Sprintf("%s/tmpfile.q", dir), 1000, MAsyncRolling[int](1000))
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		err := q.Write(i)
		if !err.IsNil() {
			log.Fatalf(err.ToString())
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	r, err := Reader(fmt.Sprintf("%s/tmpfile.q", dir), 1000, MAsyncRolling[int](1000), false)

	for i := 0; i < b.N; i++ {
		_, _, err := r.Read()
		if !err.IsNil() {
			log.Fatalf(err.ToString())
		}
		err = r.FinishRead()
		if !err.IsNil() {
			log.Fatalf(err.ToString())
		}
	}

	err = q.Close()
	if err != nil {
		b.Fatal(err)
	}
	r.Close()
	if err != nil {
		b.Fatal(err)
	}
}
