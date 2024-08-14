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

var PercentilesP99 = []float64{.50, .80, .87, .90, .97, .99}
var PercentilesP999 = []float64{.50, .90, .97, .99, .997, .999}
var PercentilesP9999 = []float64{.50, .99, .997, .999, .9997, .9999}

func (histogram *Histogram) Display(width int) {
	histogram.DisplayPercentiles(width, []float64{.50, .90, .99, .999})
}

func (histogram *Histogram) DisplayPercentiles(width int, percentiles []float64) {
	maxValue := slices.Max(histogram.buckets)
	maxHeader := len(FormatNs(int64(maxValue)))
	maxHeaderSize := maxHeader*2 + 7 + len(percentiles) + 1
	blocks := width - maxHeaderSize
	blockPerValue := float64(blocks) / float64(maxValue)

	firstPresent := 0
	for i, v := range histogram.buckets {
		firstPresent = i
		if v != 0 {
			break
		}
	}
	lastPresent := len(histogram.buckets) - 1
	for lastPresent > 0 {
		if histogram.buckets[lastPresent] != 0 {
			break
		}
		lastPresent -= 1
	}

	for i, percentile := range percentiles {
		fmt.Printf("%sp%f\n", strings.Repeat("|", i), percentile*100)
	}

	totalEntries := uint64(0)
	for i, v := range histogram.buckets[firstPresent : lastPresent+1] {
		for _, percentile := range percentiles {
			if float64(totalEntries)/float64(histogram.num) < percentile {
				if float64(totalEntries+v)/float64(histogram.num) >= percentile {
					fmt.Print("*")
				} else {
					fmt.Print("|")
				}
			} else {
				fmt.Print(" ")
			}
		}
		totalEntries += v

		bar := strings.Repeat("#", int(blockPerValue*float64(v)))
		lower := int64(0)

		bit := i + firstPresent
		if bit > 0 {
			lower = 1 << (bit - 1)
		}
		upper := int64(1) << bit

		header := fmt.Sprintf("%s - %s", FormatNs(int64(lower)), FormatNs(int64(upper)))
		percent := fmt.Sprintf("%06.3e ", float64(v)/float64(histogram.num))
		buff := strings.Repeat(" ", maxHeaderSize-len(header))
		fmt.Printf("|%s%s%s|%s\n", header, buff, percent, bar)
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

// FormatNs will format Nanos to a precission of 3 digits
// or to full seconds if the timeframe is larger than 99 seconds
func FormatNs(ns int64) string {
	if ns < 1000 {
		return fmt.Sprintf("%d ns", ns)
	}
	digits := len(strconv.FormatInt(ns, 10))
	var div int64 = 1
	truncate := 0
	for digits-truncate > 3 {
		div *= 10
		truncate += 1
	}
	strDigits := strconv.FormatInt(ns/div, 10)
	strUnit := "ns"
	if truncate > 0 {
		strUnit = "us"
	}
	if truncate > 3 {
		strUnit = "ms"
	}
	if truncate > 6 {
		strUnit = "s "
	}
	if truncate == 0 || truncate == 3 || truncate == 6 {
		return fmt.Sprintf("%s %s", strDigits[0:3], strUnit)
	}
	if truncate == 1 || truncate == 4 || truncate == 7 {
		return fmt.Sprintf("%c.%c%c%s", strDigits[0], strDigits[1], strDigits[2], strUnit)
	}
	if truncate == 2 || truncate == 5 || truncate == 8 {
		return fmt.Sprintf("%c%c.%c%s", strDigits[0], strDigits[1], strDigits[2], strUnit)
	}
	return fmt.Sprintf("%d s ", ns/1_000_000_000)
}
