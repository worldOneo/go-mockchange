package queue

import (
	"fmt"
	"testing"
)

func TestQueueWriter_write(t *testing.T) {
	dir := t.TempDir()
	q, err := Create(fmt.Sprintf("%s/tmpfile.q", dir), 100, MAsyncRolling[int](50))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1_000; i++ {
		err := q.Write(i)
		if !err.IsNil() {
			t.Fatalf("Failed to write %d: %s", i, err.ToString())
		}
	}
}
