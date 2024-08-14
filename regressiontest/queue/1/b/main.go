package main

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/worldOneo/go-mockchange/histogram"
	"github.com/worldOneo/go-mockchange/queue"
)

func main() {
	log.Printf("Normalizing measuring clock, this might take a few seconds...")
	timingDuration := histogram.CalculateNanoTimeError()
	latency := histogram.New(1_000_000_000)
	writer, err := queue.Writer("./input.q", 10_000, queue.MAsyncRolling[int64](1000))
	if err != nil {
		log.Fatalf("Failed to initialize input: %v", err)
	}
	for {
		_, err := os.Stat("./back.q")
		if errors.Is(err, os.ErrNotExist) {
			time.Sleep(time.Millisecond * 300)
			log.Printf("Waiting for ./back.q")
			continue
		}
		if err != nil {
			log.Fatalf("Failed to read: %v", err)
		}
		break
	}

	reader, err := queue.Reader("./back.q", 10_000, queue.MAsyncRolling[int64](2000), false)
	if err != nil {
		log.Fatalf("Failed to initialize output: %v", err)
	}

	log.Printf("Measuring round trip latency")
	start := histogram.Nanos()
	for i := 0; i < 10_000_000; i++ {
		now := histogram.Nanos()
		err := writer.Write(now)
		if !err.IsNil() {
			log.Fatalf("Failed to write: %v", err.ToString())
		}
		v, status, err := reader.Read()
		if !err.IsNil() || v != now {
			log.Fatalf("Failed to read: %v, status: %d", err.ToString(), status)
		}
		latency.Add(uint64(histogram.Nanos() - v - 2*timingDuration))
		err = reader.FinishRead()
		if !err.IsNil() {
			log.Fatalf("Failed to confirm read: %v", err.ToString())
		}
	}
	delta := histogram.Nanos() - start
	avgRoundTrip := delta / 10_000_000
	log.Printf("Average time: %dns", avgRoundTrip)
	latency.DisplayPercentiles(120, histogram.PercentilesP9999)
}
