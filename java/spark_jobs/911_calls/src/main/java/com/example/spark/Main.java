package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        // Create Spark configuration and session
        SparkConf conf = new SparkConf()
            .setAppName("911-calls-dimensions")
            .setMaster("yarn")
            .set("spark.mongodb.read.connection.uri", "mongodb://192.168.4.56:27017/mydb")
            .set("spark.mongodb.write.connection.uri", "mongodb://192.168.4.56:27017/mydb");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Load and clean the data
        String filePath = "/user/hadoop/silver/911_calls/raw_911_calls.csv";
        Dataset<Row> rawData = spark.read()
                                  .option("header", "true")
                                  .option("inferSchema", "true")
                                  .csv(filePath);

        // Clean timestamp and create base transformed dataset
        Dataset<Row> cleanedData = rawData
            .na().drop(new String[]{
                "Neighborhood", "PoliceDistrict", "PolicePost", "CouncilDistrict",
                "SheriffDistricts", "Community_Statistical_Areas", "Census_Tracts",
                "ZIPCode", "priority"
            })
            .withColumn("clean_timestamp", 
                regexp_replace(col("callDateTime"), "\\+00$", ""))
            .withColumn("call_datetime", 
                to_timestamp(col("clean_timestamp"), "yyyy/MM/dd HH:mm:ss"));

        // 1. Create 911_calls table (fact table)
        Dataset<Row> callsTable = cleanedData
            .withColumn("priority_level", 
                when(col("priority").equalTo("LOW"), "Emergency")
                .when(col("priority").equalTo("Non-Emergency"), "Low")
                .otherwise(col("priority")))
            .withColumn("call_description",
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
            .select(
                col("recordId").as("call_id"),
                col("call_datetime"),
                col("priority_level"),
                lit("phone").as("call_source"),
                col("call_description")
            );

        // 2. Create 911_time table (time dimension)
        Dataset<Row> timeTable = cleanedData
            .select(
                col("recordId").as("call_id"),
                col("call_datetime"),
                year(col("call_datetime")).as("year"),
                month(col("call_datetime")).as("month"),
                dayofmonth(col("call_datetime")).as("day"),
                hour(col("call_datetime")).as("hour"),
                minute(col("call_datetime")).as("minute"),
                date_format(col("call_datetime"), "EEEE").as("weekday"),
                weekofyear(col("call_datetime")).as("week_of_year"),
                quarter(col("call_datetime")).as("quarter")
            );

        // 3. Create 911_location table (location dimension)
        Dataset<Row> locationTable = cleanedData
            .select(
                col("recordId").as("call_id"),
                lit("Baltimore").as("region"),
                col("PoliceDistrict").as("district"),
                col("Neighborhood").as("neighborhood"),
                col("ZIPCode").as("postal_code"),
                col("PolicePost").as("service_area"),
                col("CouncilDistrict").as("council_district"),
                col("Community_Statistical_Areas").as("community_area"),
                col("Census_Tracts").as("census_tract"),
                col("incidentLocation").as("incident_address"),
                col("location").as("full_location")
            );

        // Save tables to MongoDB
        callsTable.write()
            .format("mongodb")
            .option("collection", "911_calls")
            .mode("overwrite")
            .save();

        timeTable.write()
            .format("mongodb")
            .option("collection", "911_time")
            .mode("overwrite")
            .save();

        locationTable.write()
            .format("mongodb")
            .option("collection", "911_location")
            .mode("overwrite")
            .save();

        spark.stop();
    }
}