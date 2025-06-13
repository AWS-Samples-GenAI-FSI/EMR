from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col, round, count, avg, when, hour
import argparse

def process_citibike_data(input_path, output_path):
    spark = SparkSession.builder.appName("CitiBikeProcessor").getOrCreate()
    
    # Read input data with the correct timestamp format
    df = spark.read.option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS").csv(input_path, header=True, inferSchema=True)
    
    # Calculate trip duration in minutes
    df = df.withColumn(
        "trip_duration_minutes",
        round((unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 60, 2)
    )
    
    # Add hour of day
    df = df.withColumn("start_hour", hour(col("started_at")))
    
    df.createOrReplaceTempView("trips")
    
    # Enhanced analysis with simplified SQL
    result = spark.sql("""
        SELECT
            COUNT(*) as total_trips,
            ROUND(AVG(trip_duration_minutes), 2) as avg_duration_minutes,
            ROUND(MAX(trip_duration_minutes), 2) as max_duration_minutes,
            ROUND(MIN(trip_duration_minutes), 2) as min_duration_minutes,
            
            -- Member vs Casual analysis
            COUNT(CASE WHEN member_casual = "member" THEN 1 END) as member_trips,
            COUNT(CASE WHEN member_casual = "casual" THEN 1 END) as casual_trips,
            ROUND(AVG(CASE WHEN member_casual = "member" THEN trip_duration_minutes END), 2) as avg_member_duration_mins,
            ROUND(AVG(CASE WHEN member_casual = "casual" THEN trip_duration_minutes END), 2) as avg_casual_duration_mins,
            
            -- Bike type analysis
            COUNT(CASE WHEN rideable_type = "classic_bike" THEN 1 END) as classic_bike_trips,
            COUNT(CASE WHEN rideable_type = "electric_bike" THEN 1 END) as electric_bike_trips,
            
            -- Rush hour analysis
            COUNT(CASE WHEN start_hour BETWEEN 7 AND 9 THEN 1 END) as morning_rush_trips,
            COUNT(CASE WHEN start_hour BETWEEN 16 AND 18 THEN 1 END) as evening_rush_trips
        FROM trips
        WHERE trip_duration_minutes > 0 AND trip_duration_minutes < 1440
    """)
    
    # Write results as a readable CSV with headers
    result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    process_citibike_data(args.input, args.output)