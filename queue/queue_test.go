package queue

import (
	"fmt"
	"testing"
	"time"
)

func TestQueueWriter_write(t *testing.T) {
	dir := "." // t.TempDir()
	q, err := Create(fmt.Sprintf("%s/tmpfile.q", dir), 100, MAsyncTimeBased[int](uint64(time.Millisecond*10)))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		err := q.Write(i)
		if !err.IsNil() {
			t.Fatalf("Failed to write %d: %s", i, err.ToString())
		}
	}
}
