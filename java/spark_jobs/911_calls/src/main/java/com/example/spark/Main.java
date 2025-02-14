package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        // Step 1: Create a Spark configuration and session
        SparkConf conf = new SparkConf()
            .setAppName("911-calls-gold")
            .setMaster("yarn")
            .set("spark.mongodb.read.connection.uri", "mongodb://192.168.4.56:27017/mydb")
            .set("spark.mongodb.write.connection.uri", "mongodb://192.168.4.56:27017/mydb");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Step 2: Load the CSV file
        String filePath = "/user/hadoop/silver/911_calls/raw_911_calls.csv";
        Dataset<Row> data = spark.read()
                                .option("header", "true")
                                .option("inferSchema", "true")
                                .csv(filePath);

        // Print schema to verify column names
        System.out.println("Original Schema:");
        data.printSchema();

        // Step 3: Transform the data into a single table with all columns
        Dataset<Row> transformedData = data
            // Remove rows with NaN in specified columns
            .na().drop(new String[]{
                "Neighborhood",
                "PoliceDistrict",
                "PolicePost",
                "CouncilDistrict",
                "SheriffDistricts",
                "Community_Statistical_Areas",
                "Census_Tracts",
                "ZIPCode",
                "priority"
            })

            // Transform priority levels
            .withColumn("priority", 
                when(col("priority").equalTo("LOW"), "Emergency")
                .when(col("priority").equalTo("Non-Emergency"), "Low")
                .otherwise(col("priority")))

            // Map descriptions to medical emergencies
            .withColumn("description",
                when(col("description").equalTo("DISORDERLY"), "Severe Headache")
                .when(col("description").equalTo("911/NO  VOICE"), "Unconscious Person")
                .when(col("description").equalTo("AUTO ACCIDENT"), "Trauma")
                .when(col("description").equalTo("COMMON ASSAULT"), "Physical Injury")
                .when(col("description").equalTo("SILENT ALARM"), "Unknown Condition")
                .when(col("description").equalTo("FAMILY DISTURB"), "Mental Health Crisis")
                .when(col("description").equalTo("NARCOTICS"), "Substance Related")
                .when(col("description").equalTo("OTHER"), "General")
                .when(col("description").equalTo("HIT AND RUN"), "Trauma")
                .when(col("description").equalTo("LARCENY"), "Stress Related")
                .otherwise(col("description")))

            // Clean and transform timestamp
            .withColumn("clean_timestamp", 
                regexp_replace(col("callDateTime"), "\\+00$", ""))
            .withColumn("call_datetime", 
                to_timestamp(col("clean_timestamp"), "yyyy/MM/dd HH:mm:ss"))
            
            // Call infos
            .withColumn("call_id", col("recordId"))
            .withColumn("priority_level", col("priority"))
            .withColumn("call_source", lit("phone"))
            .withColumn("call_description", col("description"))
            
            // Date infos
            .withColumn("year", year(col("call_datetime")))
            .withColumn("month", month(col("call_datetime")))
            .withColumn("day", dayofmonth(col("call_datetime")))
            .withColumn("hour", hour(col("call_datetime")))
            .withColumn("minute", minute(col("call_datetime")))
            .withColumn("weekday", date_format(col("call_datetime"), "EEEE"))
            .withColumn("week_of_year", weekofyear(col("call_datetime")))
            .withColumn("quarter", quarter(col("call_datetime")))
            
            // Location infos
            .withColumn("region", lit("Baltimore"))
            .withColumn("district", col("PoliceDistrict"))
            .withColumn("neighborhood", col("Neighborhood"))
            .withColumn("postal_code", col("ZIPCode"))
            .withColumn("service_area", col("PolicePost"))
            .withColumn("council_district", col("CouncilDistrict"))
            .withColumn("community_area", col("Community_Statistical_Areas"))
            .withColumn("census_tract", col("Census_Tracts"))
            .withColumn("incident_address", col("incidentLocation"))
            .withColumn("full_location", col("location"))
            
            // Select and rename final columns
            .select(
                col("call_id"),
                col("call_datetime"),
                col("priority_level"),
                col("call_source"),
                col("call_description"),
                
                // Date infos
                col("year"),
                col("month"),
                col("day"),
                col("hour"),
                col("minute"),
                col("weekday"),
                col("week_of_year"),
                col("quarter"),
                
                // Location infos
                col("region"),
                col("district"),
                col("neighborhood"),
                col("postal_code"),
                col("service_area"),
                col("council_district"),
                col("community_area"),
                col("census_tract"),
                col("incident_address"),
                col("full_location")
            );

        // Save to MongoDB as a single collection
        transformedData.write()
            .format("mongodb")
            .option("collection", "911-calls-gold")
            .mode("overwrite")
            .save();

        spark.stop();
    }
}
