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
