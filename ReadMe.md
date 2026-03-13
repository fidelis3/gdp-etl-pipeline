# GDP ETL Pipeline

This project implements a simple ETL (Extract, Transform, Load) pipeline that collects country GDP data from a Wikipedia snapshot, transforms the values, and stores the results in both CSV and SQLite formats.

## What This Pipeline Does

1. Extracts country GDP (nominal) data from an archived Wikipedia page.
2. Transforms GDP values from millions of USD to billions of USD.
3. Loads the transformed data into:
   - `Countries_by_GDP.csv`
   - `World_Economies.db` (SQLite table: `Countries_by_GDP`)
4. Logs ETL progress to `etl_project_log.txt`.

## Project Structure

- `etl_project_gdp.py`: Main ETL script.
- `Countries_by_GDP.csv`: CSV output from the load phase.
- `World_Economies.db`: SQLite database output.
- `etl_project_log.txt`: Execution log entries by phase.

## Requirements

Install dependencies with:

```bash
pip install pandas numpy requests beautifulsoup4
```

Python standard library modules used:

- `datetime`
- `sqlite3`

## How to Run

From the project root, run:

```bash
python etl_project_gdp.py
```

During execution, the script prints:

- Extracted Data
- Transformed Data
- Query output from the SQLite table

## Data Transformation Logic

The `transform()` function:

1. Removes commas from GDP values stored as strings.
2. Converts them to floating-point values.
3. Divides by `1000` to convert from millions to billions.
4. Rounds to 2 decimal places.
5. Renames column `GDP_USD_millions` to `GDP_USD_billions`.

## Output Details

- CSV file: `Countries_by_GDP.csv`
  - Contains country names and GDP in USD billions.
- SQLite DB: `World_Economies.db`
  - Table name: `Countries_by_GDP`
  - Replaced on each run (`if_exists='replace'`).
- Log file: `etl_project_log.txt`
  - Appends timestamped messages for ETL phases.


## Example Query

To query the generated SQLite table manually:

```sql
SELECT * FROM Countries_by_GDP;
```
