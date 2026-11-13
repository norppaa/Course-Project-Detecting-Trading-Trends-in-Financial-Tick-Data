Add a script that cleans the data. 

To get your own test data, download a day of data and run the script on it.

#### Option A: Standard Run (Uses defaults)

  Takes data/debs2022-gc-trading-day-08-11-21.csv and creates data/trades_cleaned.csv:

    go run scripts/clean_data.go

#### Option B: Specify Input and Output Names

    go run scripts/clean_data.go \
      --input=data/debs2022-gc-trading-day-08-11-21.csv \
      --output=data/trades_cleaned-08-11-21.csv

#### Option C: Keep ONLY rows with trade prices (Drops empty rows)

  (Cuts file size significantly by removing quote updates and heartbeats):

    go run scripts/clean_data.go \
      --only-trades \
      --input=data/debs2022-gc-trading-day-08-11-21.csv \
      --output=data/trades_only.csv

#### Option D: Save as compressed .gz (Takes up minimal disk space)

    go run scripts/clean_data.go \
      --output=data/trades_cleaned.csv.gz

## Ground truth and correctness check

`ground_truth.py` computes Query 1 and Query 2 directly from the spec, as a batch
job with no watermarks, grace period or partitions, and shares no code with the
analyzer. Every price event counts, however late it is, so the output is what a
perfect streaming system would produce. It needs only the Python standard library
and takes about a minute for the full week.

    python3 scripts/ground_truth.py data/trades_only_week.csv -o data/truth_week.csv

Use the same `--policy` (`skip` or `carry`) and `--suppress-first` as the system.
Several input files are read in the order given.

`compare_results.py` compares any system output with it: the offline tool's CSV or
a benchmark run's `results.csv`. It prints coverage, value mismatches, the largest
relative EMA error and BUY/SELL precision and recall, and exits with 1 if the
results differ.

    python3 scripts/compare_results.py data/truth_week.csv results/<experiment>/<run>/results.csv --json compare.json

Note: on 10-11-2021 at 18:56 (system time) the feed reports 5506 trades with a
trading time of 01:00 that day, almost 18 hours late. A streaming system with any
reasonable grace period drops them, so it never matches the ground truth exactly
on the full week. Monday alone matches exactly.
