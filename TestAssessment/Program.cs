using CsvHelper;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;
using TestAssessment.Mapping;
using TestAssessment.Models;

namespace TestAssessment
{
    internal class Program
    {
        const string connectionString = "Data Source=CARELESS\\SQLEXPRESS;Integrated Security=True;Trust Server Certificate=True;Initial Catalog=TestAssessmentDb;";

        static void Main(string[] args)
        {
            Console.WriteLine("ETL Test Assessment.");

            // Initialize table if needed
            if (!InitTable())
            {
                return;
            }

            // Get CSV file path from user
            string? csvPath = GetPath();
            if (csvPath is null)
            {
                return;
            }

            // Retrieve EST timezone
            TimeZoneInfo estTimeZone;
            try
            {
                estTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error retrieving EST timezone: " + ex.Message);
                return;
            }

            // Process CSV and split valid vs duplicate records
            var (validTrips, duplicateTrips) = ProcessCsvData(csvPath, estTimeZone);

            // Write duplicate records to CSV file
            if (duplicateTrips.Any())
            {
                WriteDuplicates(duplicateTrips);
                Console.WriteLine($"Found and removed {duplicateTrips.Count} duplicate records. They have been written to duplicates.csv.");
            }
            else
            {
                Console.WriteLine("No duplicate records found.");
            }

            // Bulk insert the valid records into the database
            BulkInsertTrips(validTrips);
        }

        /// <summary>
        /// Prompts the user to enter the CSV file path.
        /// </summary>
        static string? GetPath()
        {
            Console.WriteLine("Enter the CSV file path:");
            string? csvPath = Console.ReadLine();
            if (!File.Exists(csvPath))
            {
                Console.WriteLine("File not found. Exiting.");
                return null;
            }
            return csvPath;
        }

        /// <summary>
        /// Processes the CSV file and returns a tuple containing valid and duplicate trips.
        /// </summary>
        static (List<TaxiTrip> validTrips, List<TaxiTrip> duplicateTrips) ProcessCsvData(string csvPath, TimeZoneInfo estTimeZone)
        {
            var validTrips = new List<TaxiTrip>();
            var duplicateTrips = new List<TaxiTrip>();
            var uniqueKeys = new HashSet<string>();
            uint errorRows = 0;

            using (var reader = new StreamReader(csvPath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Context.RegisterClassMap<TaxiTripMap>();
                
                // Read the CSV row by row.
                while (csv.Read())
                {
                    TaxiTrip? trip = null;
                    try
                    {
                        // Try to get the record. This may throw an exception if conversion fails.
                        trip = csv.GetRecord<TaxiTrip>();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error processing row {csv.Context.Parser.RawRow}");
                        errorRows++;
                        continue; // Skip this row and continue with the next.
                    }

                    if (trip == null)
                        continue;

                    // Clean the store_and_fwd_flag field.
                    if (!string.IsNullOrEmpty(trip.store_and_fwd_flag))
                    {
                        trip.store_and_fwd_flag = trip.store_and_fwd_flag.Trim();
                        if (trip.store_and_fwd_flag.Equals("N", StringComparison.OrdinalIgnoreCase))
                            trip.store_and_fwd_flag = "No";
                        else if (trip.store_and_fwd_flag.Equals("Y", StringComparison.OrdinalIgnoreCase))
                            trip.store_and_fwd_flag = "Yes";
                    }

                    // Convert pickup and dropoff times from EST to UTC.
                    try
                    {
                        trip.tpep_pickup_datetime = TimeZoneInfo.ConvertTimeToUtc(trip.tpep_pickup_datetime, estTimeZone);
                        trip.tpep_dropoff_datetime = TimeZoneInfo.ConvertTimeToUtc(trip.tpep_dropoff_datetime, estTimeZone);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error converting times on row {csv.Context.Parser.RawRow}: {ex.Message}");
                        continue;
                    }

                    string key = $"{trip.tpep_pickup_datetime:yyyyMMddHHmmss}_{trip.tpep_dropoff_datetime:yyyyMMddHHmmss}_{trip.passenger_count}";
                    if (uniqueKeys.Contains(key))
                    {
                        duplicateTrips.Add(trip);
                    }
                    else
                    {
                        uniqueKeys.Add(key);
                        validTrips.Add(trip);
                    }
                }
            }
            Console.WriteLine($"Rows with defects found : {errorRows}");
            return (validTrips, duplicateTrips);
        }

        /// <summary>
        /// Writes duplicate trip records to a CSV file.
        /// </summary>
        static void WriteDuplicates(List<TaxiTrip> duplicateTrips)
        {
            using (var writer = new StreamWriter("duplicates.csv"))
            using (var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csvWriter.WriteHeader<TaxiTrip>();
                csvWriter.NextRecord();
                foreach (var trip in duplicateTrips)
                {
                    csvWriter.WriteRecord(trip);
                    csvWriter.NextRecord();
                }
            }
        }

        /// <summary>
        /// Performs a bulk insertion of valid trips into the database.
        /// </summary>
        static void BulkInsertTrips(List<TaxiTrip> validTrips)
        {
            using (var bulkCopy = new SqlBulkCopy(connectionString))
            {
                bulkCopy.DestinationTableName = "dbo.TaxiTrips";

                bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
                bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
                bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
                bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
                bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
                bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
                bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
                bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
                bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");

                DataTable table = CreateDataTable();
                foreach (var trip in validTrips)
                {
                    table.Rows.Add(
                        trip.tpep_pickup_datetime,
                        trip.tpep_dropoff_datetime,
                        trip.passenger_count,
                        trip.trip_distance,
                        trip.store_and_fwd_flag,
                        trip.PULocationID,
                        trip.DOLocationID,
                        trip.fare_amount,
                        trip.tip_amount);
                }

                try
                {
                    bulkCopy.WriteToServer(table);
                    Console.WriteLine($"Inserted {validTrips.Count} records into the database.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error during bulk insertion: " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Creates and returns a DataTable that matches the schema of the TaxiTrips table.
        /// </summary>
        static DataTable CreateDataTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
            table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
            table.Columns.Add("passenger_count", typeof(int));
            table.Columns.Add("trip_distance", typeof(decimal));
            table.Columns.Add("store_and_fwd_flag", typeof(string));
            table.Columns.Add("PULocationID", typeof(int));
            table.Columns.Add("DOLocationID", typeof(int));
            table.Columns.Add("fare_amount", typeof(decimal));
            table.Columns.Add("tip_amount", typeof(decimal));
            return table;
        }

        /// <summary>
        /// Checks if the TaxiTrips table exists and creates it along with indexes if it does not.
        /// </summary>
        static bool InitTable()
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string checkTableQuery = "IF OBJECT_ID('dbo.TaxiTrips', 'U') IS NULL SELECT 0 ELSE SELECT 1";
                    using (var checkCmd = new SqlCommand(checkTableQuery, connection))
                    {
                        int tableExists = (int)checkCmd.ExecuteScalar();
                        if (tableExists == 0)
                        {
                            Console.WriteLine("Table does not exist. Creating table...");
                            string createTableScript = @"
                                CREATE TABLE dbo.TaxiTrips
                                (
                                    Id INT IDENTITY(1,1) PRIMARY KEY,
                                    tpep_pickup_datetime DATETIME2(7) NOT NULL,
                                    tpep_dropoff_datetime DATETIME2(7) NOT NULL,
                                    passenger_count INT NOT NULL,
                                    trip_distance DECIMAL(10,2) NOT NULL,
                                    store_and_fwd_flag VARCHAR(3) NOT NULL,
                                    PULocationID INT NOT NULL,
                                    DOLocationID INT NOT NULL,
                                    fare_amount DECIMAL(10,2) NOT NULL,
                                    tip_amount DECIMAL(10,2) NOT NULL,
                                    trip_duration AS DATEDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) PERSISTED
                                );

                                CREATE NONCLUSTERED INDEX IX_TaxiTrips_PULocationID_TipAmount
                                ON dbo.TaxiTrips (PULocationID, tip_amount);

                                CREATE NONCLUSTERED INDEX IX_TaxiTrips_TripDistance
                                ON dbo.TaxiTrips (trip_distance DESC);

                                CREATE NONCLUSTERED INDEX IX_TaxiTrips_TripDuration
                                ON dbo.TaxiTrips (trip_duration DESC);
                                ";
                            using (var createCmd = new SqlCommand(createTableScript, connection))
                            {
                                createCmd.ExecuteNonQuery();
                                Console.WriteLine("Table and indexes created successfully.");
                            }
                        }
                        else
                        {
                            Console.WriteLine("Table TaxiTrips already exists. No action taken.");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred during initialization: " + ex.Message);
                return false;
            }

            return true;
        }
    }
}
