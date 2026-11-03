package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"trading-trends/pkg/models"
)

// TickReader reads tick data sequentially from uncompressed or gzip-compressed CSV streams.
type TickReader struct {
	file        *os.File
	gzReader    *gzip.Reader
	bufReader   *bufio.Reader
	onlyTrades  bool
	totalLines  int64
	parsedTicks int64
}

// NewTickReader creates a new TickReader from a file path. Gzip input is
// detected from the file's magic bytes, so the extension does not matter.
func NewTickReader(filePath string, onlyTrades bool) (*TickReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file %q: %w", filePath, err)
	}

	var reader io.Reader = file
	var gzReader *gzip.Reader

	magic := make([]byte, 2)
	if n, _ := file.ReadAt(magic, 0); n == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gzReader, err = gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to create gzip reader for %q: %w", filePath, err)
		}
		reader = gzReader
	}

	return &TickReader{
		file:       file,
		gzReader:   gzReader,
		bufReader:  bufio.NewReaderSize(reader, 1024*1024), // 1MB buffer for high throughput
		onlyTrades: onlyTrades,
	}, nil
}

// NewTickReaderFromReader creates a TickReader from any generic io.Reader (useful for unit tests).
func NewTickReaderFromReader(r io.Reader, onlyTrades bool) *TickReader {
	return &TickReader{
		bufReader:  bufio.NewReaderSize(r, 64*1024),
		onlyTrades: onlyTrades,
	}
}

// Close releases the file and gzip resources.
func (r *TickReader) Close() error {
	var errs []error
	if r.gzReader != nil {
		errs = append(errs, r.gzReader.Close())
	}
	if r.file != nil {
		errs = append(errs, r.file.Close())
	}
	return errors.Join(errs...)
}

// Stats returns the number of total lines read and valid ticks produced.
func (r *TickReader) Stats() (totalLines, parsedTicks int64) {
	return r.totalLines, r.parsedTicks
}

// Next reads and returns the next valid MarketTick. Returns io.EOF when stream is exhausted.
func (r *TickReader) Next() (*models.MarketTick, error) {
	for {
		line, err := r.bufReader.ReadString('\n')
		if line != "" {
			r.totalLines++
			if tick, ok := r.parseLine(strings.TrimRight(line, "\r\n")); ok {
				r.parsedTicks++
				return tick, nil
			}
		}
		if err != nil {
			return nil, err
		}
	}
}

// parseLine extracts a tick from a single CSV line. Empty lines, comments,
// headers and (with onlyTrades) non-trade or malformed rows are rejected.
func (r *TickReader) parseLine(line string) (*models.MarketTick, bool) {
	if line == "" || line[0] == '#' || strings.HasPrefix(line, "ID,SecType") {
		return nil, false
	}

	cols := strings.Split(line, ",")
	col := func(i int) string {
		if i < len(cols) {
			return cols[i]
		}
		return ""
	}

	// Column indexes of Last, Trading time and Trading date
	var lastIdx, tradingTimeIdx, tradingDateIdx int
	switch {
	case len(cols) >= 27: // Raw DEBS 2022 dataset format
		lastIdx, tradingTimeIdx, tradingDateIdx = 21, 23, 26
	case len(cols) >= 5: // Cleaned dataset format: ID,SecType,Date,Time,Last[,TradingTime,TradingDate]
		lastIdx, tradingTimeIdx, tradingDateIdx = 4, 5, 6
	default:
		return nil, false
	}

	id, secType, date, timeVal := cols[0], cols[1], cols[2], cols[3]
	if id == "" {
		return nil, false
	}

	// Most rows are quotes with an empty Last; skip them before ParseFloat,
	// which allocates an error for empty input.
	lastPrice := col(lastIdx)
	if r.onlyTrades && lastPrice == "" {
		return nil, false
	}
	price, err := strconv.ParseFloat(lastPrice, 64)
	if err != nil {
		price = 0
	}
	// If onlyTrades is enabled, discard events without a valid trade price
	if r.onlyTrades && price <= 0 {
		return nil, false
	}

	// Fall back to system Date and Time if trade-specific timestamps are blank
	tradingTime, tradingDate := col(tradingTimeIdx), col(tradingDateIdx)
	if tradingDate == "" {
		tradingDate = date
	}
	if tradingTime == "" {
		tradingTime = timeVal
	}

	ts, err := parseTimestamp(tradingDate, tradingTime)
	if err != nil {
		ts, err = parseTimestamp(date, timeVal)
	}
	if err != nil && r.onlyTrades {
		return nil, false
	}

	return &models.MarketTick{
		ID:          id,
		SecType:     secType,
		Timestamp:   ts,
		Price:       price,
		TradingDate: tradingDate,
		TradingTime: tradingTime,
	}, true
}

// parseTimestamp converts DD-MM-YYYY and HH:MM:SS.sss into a UTC time.Time efficiently.
func parseTimestamp(dateStr, timeStr string) (time.Time, error) {
	if t, ok := parseTimestampFast(dateStr, timeStr); ok {
		return t, nil
	}

	// Fallback to standard library parser if format deviates.
	// Fractional seconds are accepted even though the layouts omit them.
	combined := dateStr + " " + timeStr
	for _, layout := range []string{"02-01-2006 15:04:05", "2006-01-02 15:04:05"} {
		if t, err := time.ParseInLocation(layout, combined, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse timestamp %q %q", dateStr, timeStr)
}

// parseTimestampFast handles the expected "DD-MM-YYYY" and "HH:MM:SS[.fraction]"
// format without allocating.
func parseTimestampFast(dateStr, timeStr string) (time.Time, bool) {
	if len(dateStr) != 10 || len(timeStr) < 8 ||
		dateStr[2] != '-' || dateStr[5] != '-' || timeStr[2] != ':' || timeStr[5] != ':' {
		return time.Time{}, false
	}

	day, ok1 := atoi(dateStr[0:2])
	month, ok2 := atoi(dateStr[3:5])
	year, ok3 := atoi(dateStr[6:10])
	hour, ok4 := atoi(timeStr[0:2])
	minute, ok5 := atoi(timeStr[3:5])
	sec, ok6 := atoi(timeStr[6:8])
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) || hour > 23 || minute > 59 || sec > 59 {
		return time.Time{}, false
	}

	nsec := 0
	if len(timeStr) > 8 {
		if timeStr[8] != '.' {
			return time.Time{}, false
		}
		multiplier := 100000000 // 10^8
		for i := 9; i < len(timeStr) && i < 18; i++ {
			c := timeStr[i]
			if c < '0' || c > '9' {
				return time.Time{}, false
			}
			nsec += int(c-'0') * multiplier
			multiplier /= 10
		}
	}

	t := time.Date(year, time.Month(month), day, hour, minute, sec, nsec, time.UTC)
	// time.Date normalizes invalid dates (e.g. 31-02 -> 03-03); reject those
	if t.Day() != day || t.Month() != time.Month(month) {
		return time.Time{}, false
	}
	return t, true
}

// atoi parses a short string of ASCII digits.
func atoi(s string) (int, bool) {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, len(s) > 0
}
