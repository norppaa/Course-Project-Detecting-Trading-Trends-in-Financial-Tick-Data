package main

import (
	"bufio"
	"compress/gzip"
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

// NewTickReader creates a new TickReader from a file path (.csv or .csv.gz).
func NewTickReader(filePath string, onlyTrades bool) (*TickReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file %q: %w", filePath, err)
	}

	var reader io.Reader = file
	var gzReader *gzip.Reader

	// Check if file is gzip: check .gz suffix or inspect magic bytes
	if strings.HasSuffix(strings.ToLower(filePath), ".gz") {
		gzReader, err = gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to create gzip reader for %q: %w", filePath, err)
		}
		reader = gzReader
	} else {
		// Also inspect the first two magic bytes for gzip in case extension is missing
		magic := make([]byte, 2)
		n, _ := file.ReadAt(magic, 0)
		if n == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
			gzReader, err = gzip.NewReader(file)
			if err != nil {
				file.Close()
				return nil, fmt.Errorf("failed to create gzip reader: %w", err)
			}
			reader = gzReader
		}
	}

	bufReader := bufio.NewReaderSize(reader, 1024*1024) // 1MB buffer for high throughput

	return &TickReader{
		file:       file,
		gzReader:   gzReader,
		bufReader:  bufReader,
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
	var errs []string
	if r.gzReader != nil {
		if err := r.gzReader.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if r.file != nil {
		if err := r.file.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing reader: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Stats returns the number of total lines read and valid ticks produced.
func (r *TickReader) Stats() (totalLines, parsedTicks int64) {
	return r.totalLines, r.parsedTicks
}

// Next reads and returns the next valid MarketTick. Returns io.EOF when stream is exhausted.
func (r *TickReader) Next() (*models.MarketTick, error) {
	for {
		line, err := r.bufReader.ReadString('\n')
		if len(line) > 0 {
			r.totalLines++
			line = strings.TrimRight(line, "\r\n")

			// Skip empty lines, comments, and headers
			if len(line) == 0 || line[0] == '#' || strings.HasPrefix(line, "ID,SecType") {
				if err != nil {
					if err == io.EOF {
						return nil, io.EOF
					}
					return nil, err
				}
				continue
			}

			tick, ok := r.parseLine(line)
			if ok {
				r.parsedTicks++
				return tick, nil
			}
		}

		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
	}
}

// parseLine extracts fields from a single CSV line.
func (r *TickReader) parseLine(line string) (*models.MarketTick, bool) {
	cols := strings.Split(line, ",")

	var id, secType, date, timeVal, lastPrice, tradingTime, tradingDate string

	if len(cols) >= 27 {
		// Raw DEBS 2022 dataset format
		id = cols[0]
		secType = cols[1]
		date = cols[2]
		timeVal = cols[3]
		lastPrice = cols[21]
		tradingTime = cols[23]
		tradingDate = cols[26]
	} else if len(cols) >= 5 {
		// Cleaned dataset format: ID,SecType,Date,Time,Last[,TradingTime,TradingDate]
		id = cols[0]
		secType = cols[1]
		date = cols[2]
		timeVal = cols[3]
		lastPrice = cols[4]
		if len(cols) >= 6 {
			tradingTime = cols[5]
		}
		if len(cols) >= 7 {
			tradingDate = cols[6]
		}
	} else {
		return nil, false
	}

	// Basic validation: ID must not be empty
	if id == "" {
		return nil, false
	}

	// Fallback to system Date and Time if trade-specific timestamps are blank
	if tradingDate == "" {
		tradingDate = date
	}
	if tradingTime == "" {
		tradingTime = timeVal
	}

	// If onlyTrades is enabled, discard events without a trade price
	if r.onlyTrades && (lastPrice == "" || lastPrice == "0" || lastPrice == "0.000000") {
		return nil, false
	}

	var price float64
	if lastPrice != "" {
		p, err := strconv.ParseFloat(lastPrice, 64)
		if err != nil {
			if r.onlyTrades {
				return nil, false
			}
		} else {
			price = p
		}
	}

	// Fast timestamp parsing
	ts, err := parseTimestamp(tradingDate, tradingTime)
	if err != nil {
		// Try date and timeVal as fallback
		ts, err = parseTimestamp(date, timeVal)
		if err != nil {
			// If parsing still fails and onlyTrades is enabled, discard malformed row
			if r.onlyTrades {
				return nil, false
			}
			ts = time.Time{}
		}
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
	// Expected dateStr: "DD-MM-YYYY" (len 10)
	// Expected timeStr: "HH:MM:SS" (len 8) or "HH:MM:SS.sss" (len 10-12)
	if len(dateStr) == 10 && len(timeStr) >= 8 &&
		dateStr[2] == '-' && dateStr[5] == '-' &&
		timeStr[2] == ':' && timeStr[5] == ':' {

		day := fastAtoi2(dateStr[0], dateStr[1])
		month := fastAtoi2(dateStr[3], dateStr[4])
		year := fastAtoi4(dateStr[6], dateStr[7], dateStr[8], dateStr[9])

		hour := fastAtoi2(timeStr[0], timeStr[1])
		min := fastAtoi2(timeStr[3], timeStr[4])
		sec := fastAtoi2(timeStr[6], timeStr[7])

		nsec := 0
		if len(timeStr) > 8 && timeStr[8] == '.' {
			fracStr := timeStr[9:]
			var multiplier = 100000000 // 10^8
			for i := 0; i < len(fracStr) && i < 9; i++ {
				c := fracStr[i]
				if c >= '0' && c <= '9' {
					nsec += int(c-'0') * multiplier
					multiplier /= 10
				} else {
					break
				}
			}
		}

		if day >= 1 && day <= 31 && month >= 1 && month <= 12 &&
			hour >= 0 && hour <= 23 && min >= 0 && min <= 59 && sec >= 0 && sec <= 60 {
			return time.Date(year, time.Month(month), day, hour, min, sec, nsec, time.UTC), nil
		}
	}

	// Fallback to standard library parser if format deviates
	combined := dateStr + " " + timeStr
	layouts := []string{
		"02-01-2006 15:04:05.999999999",
		"02-01-2006 15:04:05",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, combined, time.UTC); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp %q %q", dateStr, timeStr)
}

func fastAtoi2(c0, c1 byte) int {
	return int(c0-'0')*10 + int(c1-'0')
}

func fastAtoi4(c0, c1, c2, c3 byte) int {
	return int(c0-'0')*1000 + int(c1-'0')*100 + int(c2-'0')*10 + int(c3-'0')
}
