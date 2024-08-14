package histogram

import (
	"fmt"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Histogram struct {
	buckets []uint64
	sum     uint64
	num     uint64
}

func index(value uint64) int {
	return 64 - bits.LeadingZeros64(value)
}

func New(maxValue uint64) Histogram {
	return Histogram{
		buckets: make([]uint64, index(maxValue)),
		sum:     0,
		num:     0,
	}
}

func (histogram *Histogram) Add(value uint64) {
	idx := index(value)
	if idx > len(histogram.buckets) {
		return
	}
	histogram.buckets[idx] += 1
	histogram.sum += value
	histogram.num += 1
}

func (histogram *Histogram) Display(width int) {
	maxValue := slices.Max(histogram.buckets)
	maxHeader := len(strconv.Itoa(1 << len(histogram.buckets)))
	maxHeaderSize :=  maxHeader*2 + 10
	blocks := width - maxHeaderSize
	blockPerValue := float64(blocks) / float64(maxValue)

	for i, v := range histogram.buckets {
		bar := strings.Repeat("#", int(blockPerValue*float64(v)))
		lower := 0
		if i > 0 {
			lower = 1 << (i - 1)
		}
		upper := 1 << i
		
		header := fmt.Sprintf("%d-%d(%.2f%%)", lower, upper, float64(v*100)/float64(histogram.num))
		buff := strings.Repeat(" ", maxHeaderSize - len(header))
		fmt.Printf("%s%s|%s\n", header, buff, bar)
	}
}

func CalculateNanoTimeError() int64 {
	sum := int64(0)
	// measure how long time measuring takes
	for i := 0; i < 2_000_000; i++ {

		now := time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		time.Now().UnixNano()
		stop := time.Now().UnixNano()
		sum += stop - now

	}
	avgTimeTaken := sum / (2_000_000 * 20)
	return avgTimeTaken
}


func Nanos() int64 {
	return time.Now().UnixNano()
}