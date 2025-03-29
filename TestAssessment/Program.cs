using CsvHelper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Serilog;
using System.Data;
using System.Globalization;
using TestAssessment.Mapping;
using TestAssessment.Models;

namespace TestAssessment
{
    internal class Program
    {
        private static string? _connectionString;
        private static IConfigurationRoot _configuration;
        private static bool _saveDuplicates = false;

        static void Main(string[] args)
        {
            if(args.Length > 0)
            {
                _saveDuplicates = bool.TryParse(args[0], out _saveDuplicates);
            }

            ConfigureAppSettings();
            ConfigureSerilog();

            _connectionString = _configuration.GetConnectionString("DefaultConnection");
            if(string.IsNullOrEmpty(_connectionString))
            {
                Log.Error("Connection string not found.");
                Log.CloseAndFlush();
                return;
            }

            Log.Information("Connection string loaded.");

            Log.Information("ETL Test Assessment.");

            // Initialize table if needed
            if (!InitTable())
            {
                Log.CloseAndFlush();
                return;
            }

            // Get CSV file path from user
            string? csvPath = GetPath();
            if (csvPath is null)
            {
                Log.CloseAndFlush();
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
                Log.Error("Error retrieving EST timezone: " + ex.Message);
                Log.CloseAndFlush();
                return;
            }

            // Process CSV and split valid vs duplicate records
            var (validTrips, duplicateTrips) = ProcessCsvData(csvPath, estTimeZone);

            // Write duplicate records to CSV file
            if (duplicateTrips.Any())
            {
                Log.Warning($"Found and removed {duplicateTrips.Count} duplicate records.");
                if(_saveDuplicates)
                {
                    WriteDuplicates(duplicateTrips);
                }
            }
            else
            {
                Log.Information("No duplicate records found.");
            }

            // Bulk insert the valid records into the database
            BulkInsertTrips(validTrips);
        }

        /// <summary>
        /// Builds the IConfigurationRoot from appsettings.json
        /// </summary>
        private static void ConfigureAppSettings()
        {
            // The base path is typically the folder where the compiled assembly is located
            _configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();
        }

        /// <summary>
        /// Reads Serilog settings from the _configuration object and sets up the global logger.
        /// </summary>
        private static void ConfigureSerilog()
        {
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(_configuration)
                .CreateLogger();
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
                Log.Error("File not found. Exiting.");
                Log.CloseAndFlush();
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
                        Log.Warning($"Error processing row {csv.Context.Parser.RawRow}");
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
                        Log.Warning($"Error converting times on row {csv.Context.Parser.RawRow}: {ex.Message}");
                        continue;
                    }

                    // Check if pickup time is after dropoff time.
                    if (trip.tpep_pickup_datetime > trip.tpep_dropoff_datetime)
                    {
                        Log.Warning($"Row {csv.Context.Parser.RawRow} has pickup time after dropoff time. Pickup: {trip.tpep_pickup_datetime}, Dropoff: {trip.tpep_dropoff_datetime}");
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
            Log.Information($"Rows with defects found : {errorRows}");
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
            Log.Information("Duplicates have been written to duplicates.csv.");
        }

        /// <summary>
        /// Performs a bulk insertion of valid trips into the database.
        /// </summary>
        static void BulkInsertTrips(List<TaxiTrip> validTrips)
        {
            using (var bulkCopy = new SqlBulkCopy(_connectionString))
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
                    Log.Error("Error during bulk insertion: " + ex.Message);
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
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    string checkTableQuery = "IF OBJECT_ID('dbo.TaxiTrips', 'U') IS NULL SELECT 0 ELSE SELECT 1";
                    using (var checkCmd = new SqlCommand(checkTableQuery, connection))
                    {
                        int tableExists = (int)checkCmd.ExecuteScalar();
                        if (tableExists == 0)
                        {
                            Log.Information("Table does not exist. Creating table...");
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
                                Log.Information("Table and indexes created successfully.");
                            }
                        }
                        else
                        {
                            Log.Information("Table TaxiTrips already exists. No action taken.");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error("An error occurred during initialization: " + ex.Message);
                return false;
            }

            return true;
        }
    }
}
