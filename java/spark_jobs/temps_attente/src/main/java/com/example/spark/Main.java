package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        // Create Spark configuration and session
        SparkConf conf = new SparkConf()
            .setAppName("TransformedTempsAttenteJob")
            .setMaster("yarn")
            .set("spark.mongodb.read.connection.uri", "mongodb://192.168.4.56:27017/mydb")
            .set("spark.mongodb.write.connection.uri", "mongodb://192.168.4.56:27017/mydb");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Load the CSV file
        Dataset<Row> data = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("/user/hadoop/silver/temps_attente/Urgence_data.csv");

        // Trim all column names
        for (String colName : data.columns()) {
            if (colName.trim().length() != colName.length()) {
                data = data.withColumnRenamed(colName, colName.trim());
            }
        }

        // Generate unique IDs for each table
        Dataset<Row> withIds = data
            .withColumn("passage_id", monotonically_increasing_id())
            .withColumn("patient_id", hash(col("dossier_hopital")))
            .withColumn("calendar_id", hash(to_date(to_timestamp(col("DH_arrivee"), "dd/MM/yyyy HH:mm"))));

        // Create Patient table
        Dataset<Row> patientTable = withIds.select(
            col("patient_id"),
            col("dossier_hopital"),
            col("Antécédents_médicaux").as("antecedents_medicaux"),
            col("Antécédents_chirurgicaux").as("antecedents_chirurgicaux")
        ).distinct();

        // Create Calendar table
        Dataset<Row> calendarTable = withIds
            .withColumn("date_complete", to_timestamp(col("DH_arrivee"), "dd/MM/yyyy HH:mm"))
            .select(
                col("calendar_id"),
                to_date(col("date_complete")).as("date"),
                year(col("date_complete")).as("annee"),
                month(col("date_complete")).as("mois"),
                dayofmonth(col("date_complete")).as("jour"),
                date_format(col("date_complete"), "EEEE").as("jour_semaine"),
                hour(col("date_complete")).as("heure")
            ).distinct();

        // Create Passage table
        Dataset<Row> passageTable = withIds
            // Parse DH_arrivee as timestamp first
            .withColumn("DH_arrivee_ts", to_timestamp(col("DH_arrivee"), "dd/MM/yyyy HH:mm"))
            // Calculate sortie time
            .withColumn("minutes_to_add", expr("CAST(rand() * (180 - 30) + 30 AS INT)"))
            .withColumn("DH_sortie_ts", expr("from_unixtime(unix_timestamp(DH_arrivee_ts) + minutes_to_add * 60)"))
            .select(
                col("passage_id"),
                col("patient_id"),
                col("calendar_id"),
                date_format(col("DH_arrivee_ts"), "HH:mm").as("heure_arrivee"),
                date_format(col("DH_sortie_ts"), "HH:mm").as("heure_sortie"),
                col("minutes_to_add").as("temps_attente"),
                when(col("minutes_to_add").geq(60),
                    concat(
                        lpad((col("minutes_to_add").divide(60)).cast("int").cast("string"), 2, "0"),
                        lit(":"),
                        lpad((col("minutes_to_add").mod(60)).cast("int").cast("string"), 2, "0")
                    )
                ).otherwise(
                    concat(lit("00:"), lpad(col("minutes_to_add").cast("int").cast("string"), 2, "0"))
                ).as("temps_attente_heure"),
                col("motif_entree"),
                col("moyen_arrivee"),
                col("PAS/PAD").as("pas_pad"),
                col("frequence_cardiaque"),
                col("Temperature").as("temperature"),
                col("SaO2").as("sao2"),
                col("IAO_observation").as("iao_observation")
            );

        // Save to MongoDB
        patientTable.write()
            .format("mongodb")
            .option("collection", "patients")
            .mode("overwrite")
            .save();

        calendarTable.write()
            .format("mongodb")
            .option("collection", "calendrier")
            .mode("overwrite")
            .save();

        passageTable.write()
            .format("mongodb")
            .option("collection", "passages")
            .mode("overwrite")
            .save();

        // Print statistics
        System.out.println("Original row count: " + data.count());
        System.out.println("Patient table row count: " + patientTable.count());
        System.out.println("Calendar table row count: " + calendarTable.count());
        System.out.println("Passage table row count: " + passageTable.count());

        spark.stop();
    }
}