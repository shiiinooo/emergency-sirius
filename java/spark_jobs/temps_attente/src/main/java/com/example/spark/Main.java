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

        // Remove columns - using trim to handle potential whitespace in column names
        String[] columnsToDrop = {
            "moyen_arrivee", "motif_entree", "Antécédents_médicaux", "Antécédents_chirurgicaux",
            "PAS/PAD", "frequence_cardiaque", "Temperature", "SaO2", "frequence_respiratoire",
            "result_anamnese", "IAO_observation", "exam_biologie", "exam_radiologie",
            "exam_echographie", "scanner", "IRM", "DH_first_biology_prescription",
            "DH_first_biology_prelev", "type_orientation", "destination_orientation",
            "transfert_service", "transfert_hopital","result_anamnese","frequence_respiratoire","avis_specialist","code_diagnostic","dossier_hopital"        };

        // First, trim all column names to handle whitespace
        for (String colName : data.columns()) {
            if (colName.trim().length() != colName.length()) {
                data = data.withColumnRenamed(colName, colName.trim());
            }
        }

        Dataset<Row> transformedData = data.drop(columnsToDrop);

        // Handle date and time transformations
        transformedData = transformedData
            // Parse DH_arrivee as timestamp first
            .withColumn("DH_arrivee_ts", to_timestamp(col("DH_arrivee"), "dd/MM/yyyy HH:mm"))
            // Extract date and time components
            .withColumn("date_arrivee", to_date(col("DH_arrivee_ts")))
            .withColumn("heure_arrivee", date_format(col("DH_arrivee_ts"), "HH:mm"))
            // Calculate sortie time (adding random minutes between 30 and 180)
            .withColumn("minutes_to_add", expr("CAST(rand() * (180 - 30) + 30 AS INT)"))
            // Corrected the timestamp addition syntax (add minutes as seconds)
            .withColumn("DH_sortie_ts", expr("from_unixtime(unix_timestamp(DH_arrivee_ts) + minutes_to_add * 60)"))
            .withColumn("heure_sortie", date_format(col("DH_sortie_ts"), "HH:mm"))
            // Calculate waiting time in minutes
            .withColumn("temps_attente", col("minutes_to_add"))
            .withColumn("temps_attente_heure",
                  when(col("temps_attente").geq(60),
                      concat(
                         lpad((col("temps_attente").divide(60)).cast("int").cast("string"), 2, "0"),
                         lit(":"),  // Utilise lit() pour convertir ":" en Column
                         lpad((col("temps_attente").mod(60)).cast("int").cast("string"), 2, "0")
                     )
                 ).otherwise(
                     concat(lit("00:"), lpad(col("temps_attente").cast("int").cast("string"), 2, "0"))
                 )
               )

            // Drop temporary columns
            .drop("DH_arrivee_ts", "DH_sortie_ts", "minutes_to_add","temps_attente");

        // Save to MongoDB
        transformedData.write()
            .format("mongodb")
            .option("collection", "tempsattente")
            .mode("overwrite")
            .save();

        // Print statistics
        System.out.println("Original row count: " + data.count());
        System.out.println("Transformed row count: " + transformedData.count());

 

        spark.stop();
    }
}