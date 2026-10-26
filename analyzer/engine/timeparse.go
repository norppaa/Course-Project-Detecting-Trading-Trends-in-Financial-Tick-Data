package engine

import (
	"errors"
	"time"
)

// Event times are represented as int64 microseconds since the Unix epoch,
// interpreting the dataset's date and time as a naive wall clock
// (as if it were UTC). Because every UTC day is a whole multiple of
// 5 minutes, window boundaries then fall exactly on local midnight, which is
// what the spec requires ("the first window w0 starts at 0:00").
// No time-zone conversion is done anywhere, so CET/CEST does not matter.

const (
	Microsecond int64 = 1
	Millisecond       = 1000 * Microsecond
	Second            = 1000 * Millisecond
	Minute            = 60 * Second
	Day               = 24 * 60 * Minute
)

var ErrBadTimestamp = errors.New("engine: malformed timestamp")

// TimeParser converts the dataset's "DD-MM-YYYY" dates and "HH:MM:SS.ssss"
// times into event times. It caches the most recent date, since consecutive
// rows almost always share it. Not safe for concurrent use; give each
// goroutine its own parser.
type TimeParser struct {
	lastDate  string
	lastDayUs int64
}

// Parse returns the event time in microseconds for a date and time string.
// The fractional part may have 0 to 6 digits.
func (p *TimeParser) Parse(date, clock string) (int64, error) {
	day, err := p.parseDate(date)
	if err != nil {
		return 0, err
	}
	tod, err := parseClock(clock)
	if err != nil {
		return 0, err
	}
	return day + tod, nil
}

func (p *TimeParser) parseDate(s string) (int64, error) {
	if s == p.lastDate && s != "" {
		return p.lastDayUs, nil
	}
	// DD-MM-YYYY
	if len(s) != 10 || s[2] != '-' || s[5] != '-' {
		return 0, ErrBadTimestamp
	}
	d, ok1 := atoi(s[0:2])
	m, ok2 := atoi(s[3:5])
	y, ok3 := atoi(s[6:10])
	if !ok1 || !ok2 || !ok3 || m < 1 || m > 12 || d < 1 || d > 31 {
		return 0, ErrBadTimestamp
	}
	us := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).UnixMicro()
	p.lastDate, p.lastDayUs = s, us
	return us, nil
}

// parseClock parses "HH:MM:SS" with an optional ".f" to ".ffffff" suffix
// and returns microseconds since midnight.
func parseClock(s string) (int64, error) {
	if len(s) < 8 || s[2] != ':' || s[5] != ':' {
		return 0, ErrBadTimestamp
	}
	h, ok1 := atoi(s[0:2])
	m, ok2 := atoi(s[3:5])
	sec, ok3 := atoi(s[6:8])
	if !ok1 || !ok2 || !ok3 || h > 23 || m > 59 || sec > 60 {
		return 0, ErrBadTimestamp
	}
	us := int64(h)*60*Minute + int64(m)*Minute + int64(sec)*Second
	if len(s) == 8 {
		return us, nil
	}
	if s[8] != '.' || len(s) == 9 || len(s) > 15 {
		return 0, ErrBadTimestamp
	}
	frac := s[9:]
	f, ok := atoi(frac)
	if !ok {
		return 0, ErrBadTimestamp
	}
	// Scale the fraction to microseconds, e.g. ".1234" -> 123400 µs.
	for i := len(frac); i < 6; i++ {
		f *= 10
	}
	return us + int64(f), nil
}

// atoi parses a non-empty string of ASCII digits.
func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
