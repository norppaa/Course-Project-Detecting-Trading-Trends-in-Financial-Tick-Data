package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// The Course Project (Table 1) starred attributes:
//  Col 1:  ID (Symbol with exchange code, e.g. RDSA.NL)
//  Col 2:  SecType (E = Equity, I = Index)
//  Col 3:  Date (DD-MM-YYYY)
//  Col 4:  Time (HH:MM:SS.ssss)
//  Col 22: Last (Trade price)
//  Col 24: Trading time
//  Col 27: Trading date

func main() {
	inputPath := flag.String("input", "data/debs2022-gc-trading-day-08-11-21.csv", "Path to input CSV (or .csv.gz)")
	outputPath := flag.String("output", "data/trades_cleaned.csv", "Path to output cleaned CSV (use .gz for gzip)")
	onlyTrades := flag.Bool("only-trades", false, "If true, keep only events that have a non-empty Last price (trade events)")
	flag.Parse()

	startTime := time.Now()
	fmt.Printf("Cleaning %s -> %s\n", *inputPath, *outputPath)
	if *onlyTrades {
		fmt.Println("Filter: Keeping ONLY price events (Last != '')")
	}

	inFile, err := os.Open(*inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input file: %v\n", err)
		os.Exit(1)
	}
	defer inFile.Close()

	var reader io.Reader = inFile
	if strings.HasSuffix(*inputPath, ".gz") {
		gzReader, err := gzip.NewReader(inFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening gzip reader: %v\n", err)
			os.Exit(1)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	outFile, err := os.Create(*outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	var writer io.Writer = outFile
	var gzWriter *gzip.Writer
	if strings.HasSuffix(*outputPath, ".gz") {
		gzWriter = gzip.NewWriter(outFile)
		defer gzWriter.Close()
		writer = gzWriter
	}

	bufReader := bufio.NewReaderSize(reader, 1024*1024) // 1MB buffer
	bufWriter := bufio.NewWriterSize(writer, 1024*1024) // 1MB buffer
	defer bufWriter.Flush()

	// Write clean CSV header
	header := "ID,SecType,Date,Time,Last,TradingTime,TradingDate\n"
	bufWriter.WriteString(header)

	var totalLines int64
	var writtenLines int64

	for {
		line, err := bufReader.ReadString('\n')
		if len(line) > 0 {
			// Skip comment headers
			if strings.HasPrefix(line, "#") {
				continue
			}

			// Skip header row if present
			if strings.HasPrefix(line, "ID,SecType") {
				continue
			}

			totalLines++
			cols := strings.Split(strings.TrimRight(line, "\r\n"), ",")

			// Ensure line has enough columns
			if len(cols) >= 27 {
				id := cols[0]
				secType := cols[1]
				date := cols[2]
				timeVal := cols[3]
				lastPrice := cols[21]   // 0-indexed column 22
				tradingTime := cols[23] // 0-indexed column 24
				tradingDate := cols[26] // 0-indexed column 27

				// If onlyTrades is enabled, filter out events without a trade price
				if *onlyTrades && (lastPrice == "" || lastPrice == "0" || lastPrice == "0.000000") {
					continue
				}

				bufWriter.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n",
					id, secType, date, timeVal, lastPrice, tradingTime, tradingDate))
				writtenLines++
			}

			if totalLines%5000000 == 0 {
				fmt.Printf("Processed %d million lines... (kept %d)\n", totalLines/1000000, writtenLines)
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "Read error: %v\n", err)
			break
		}
	}

	fmt.Printf("\nDone in %v!\nTotal lines processed: %d\nClean lines written: %d\nOutput saved to: %s\n",
		time.Since(startTime), totalLines, writtenLines, *outputPath)
}
