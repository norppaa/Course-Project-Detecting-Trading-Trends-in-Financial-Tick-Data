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
