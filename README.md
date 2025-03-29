# TestAssessment

## Overview
This is a simple ETL (Extract, Transform, Load) application written in C#. It reads taxi trip data from a CSV file and efficiently bulk inserts valid records into a SQL Server database.

## Key Features
- **CSV Import & Processing:** Reads taxi trip data from CSV files.
- **Data Cleaning & Transformation:**  
  - Trims text fields.
  - Converts pickup and dropoff times from EST to UTC.
- **Validation & Error Handling:**  
  - Logs a warning if the pickup time is after the dropoff time.
  - Skips rows with conversion errors, logging issues for further review.
- **Duplicate Detection:** Identifies duplicates based on some composite key and writes them to a `duplicates.csv` file.
- **Bulk Insertion:** Uses `SqlBulkCopy` for efficient insertion into a SQL Server table.

## Business Logic to Consider
- **Data Validation:**  
  - **Numeric Fields:** Are negative values in `passenger_count` or `trip_distance` should be set to 0?
  - **Flag Conversion:** Only 'N' and 'Y' are valid for `store_and_fwd_flag`. Unexpected values should be logged or defaulted to "No"?
- **Error Handling:**  
  - Problematic rows (e.g., conversion failures) should be skipped, and errors are logged?
  - Duplicates records are separated and logged into a dedicated CSV file?

## Setup & Running
1. **Configure Settings:**  
   - Update `appsettings.json` with the correct connection string under `ConnectionStrings:DefaultConnection` and adjust Serilog settings as needed.
2. **Build & Run:**  
   - Open the solution in Visual Studio.
   - Build the project.
   - Run the application from the console.
3. **Provide Input:**  
   - When prompted, enter the path to your CSV file.
4. **Processing:**  
   - The application will process the CSV, log any issues, remove duplicates, and insert valid records into the SQL Server database.
## Working with larger input
If we were dealing with a much larger file—say, a 10GB CSV—we wouldn’t load it all into memory at once. Instead, we’d break the file into smaller, manageable chunks (for example, 10,000 rows at a time), process each batch, and then insert those records into the database. This way, we keep our memory usage in check. We could also leverage asynchronous and parallel processing to speed things up and make the best use of our resources.
