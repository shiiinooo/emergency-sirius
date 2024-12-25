package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class Main {
    public static void main(String[] args) {
        // Step 1: Create a Spark configuration and session
        SparkConf conf = new SparkConf()
                .setAppName("TransformedSparkJob")
                .setMaster("yarn")
                .set("spark.sql.catalogImplementation", "hive"); // Enable Hive support

        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        // Step 2: Load the CSV file into a DataFrame
        String filePath = "/output_stock_data_20241116174338.csv"; // Adjust the path if needed
        Dataset<Row> data = spark.read()
                .option("header", "true") // Assuming the CSV has a header
                .option("inferSchema", "true") // Automatically infer schema
                .csv(filePath);

        // Step 3: Perform a transformation (e.g., filter rows where the value in column 2 is >= 200)
        Dataset<Row> transformedData = data.filter("vw >= 200"); // Adjust column name if necessary

        // Step 4: Save the transformed data to Hive
        transformedData.write()
                .mode(SaveMode.Overwrite) // Overwrite the table if it already exists
                .saveAsTable("filtered_stock_data"); // Specify the Hive table name

        // Step 5: Print the result (e.g., row counts)
        System.out.println("Original row count: " + data.count());
        System.out.println("Filtered row count: " + transformedData.count());

        // Step 6: Stop the Spark session
        spark.stop();
    }
}
