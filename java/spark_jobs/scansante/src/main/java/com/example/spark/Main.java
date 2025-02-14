package com.example.spark;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.*;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Random;

public class Main {

    public static void main(String[] args) {
        // Configuration Spark pour YARN et MongoDB
        SparkConf conf = new SparkConf()
                .setAppName("TransformAndSaveToMongoDB")
                .setMaster("yarn")  // Utilisation de YARN comme gestionnaire de cluster
                .set("spark.mongodb.read.connection.uri", "mongodb://192.168.4.56:27017/mydb")
                .set("spark.mongodb.write.connection.uri", "mongodb://192.168.4.56:27017/mydb");

        // Initialisation de SparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // le répertoire contenant les fichiers .csv dans HDFS
        String hdfsPath = "/user/hadoop/silver/scansante/";

        // ajouter colonne Year et concatener tous les fichiers
        Dataset<Row> dfFinal = loadFilesWithYear(spark, hdfsPath);

        // Supprimer la colonne de commentaire
        dfFinal = dfFinal.drop("Commentaire");
        dfFinal = dfFinal.drop("Nombre de séjours ou journées ENCc");
        dfFinal = dfFinal.drop("Nombre de séjours ENC (HC uniquement)");
        dfFinal = dfFinal.drop("Nombre de journées ENC");
        dfFinal = dfFinal.drop("Nombre moyen de journées de présence de référence (HC uniquement)");
        dfFinal = dfFinal.drop("Coût modifié (*)");

        // Récupérer toutes les colonnes
        String[] columns = dfFinal.columns();

        // Parcourir chaque colonne pour vérifier si elle peut être convertie en numérique
        for (int i = 3; i < columns.length; i++) {
            String column = columns[i];

            // Essayer de vérifier si la colonne peut être convertie en Double
            try {
                // Appliquer la conversion en Double pour chaque colonne si les valeurs sont numériques
                dfFinal = dfFinal.withColumn(column, functions.col(column).cast(DataTypes.DoubleType));
            } catch (Exception e) {
                // Ignorer les erreurs si la conversion échoue (par exemple, pour les colonnes non numériques)
                System.out.println("La colonne " + column + " ne peut pas être convertie en Double.");
            }
        }




        // Ajouter les colonnes date_entree et date_sortie
        dfFinal = addDatesColumns(dfFinal);

        // Remplacer les valeurs nulles par "Non renseigné"
        dfFinal = dfFinal.na().fill("Non renseigné");

        // Remplacer les valeurs nulles par 0 dans les colonnes de type Double
        dfFinal = replaceNullsInDoubleColumns(dfFinal);


        dfFinal = dfFinal
                .withColumn("Libelle_GME", functions.regexp_extract(functions.col("Libellé GME"), "^(.*?)(,|$)", 1))
                .withColumn("niveau", functions.regexp_extract(functions.col("Libellé GME"), "-niv(\\d+)", 1));

        dfFinal = dfFinal.drop("Libellé GME");

        // Sauvegarder le Dataset concaténé dans MongoDB
        dfFinal.write()
                .format("mongodb")
                .mode("overwrite")
                .option("collection", "scansante_data")
                .save();

        // Créer la table Patient avec les données spécifiées (pas d'agrégation)
        Dataset<Row> patientTable = createPatientTable(dfFinal);

        // Sauvegarder la table Cout_Global dans MongoDB
        patientTable.write()
                .format("mongodb")
                .mode("overwrite")
                .option("collection", "Patient")
                .save();

        // Créer la table Cout_Global avec les données spécifiées (pas d'agrégation)
        Dataset<Row> coutGlobalTable = createCoutGlobalTable(dfFinal);

        // Sauvegarder la table Cout_Global dans MongoDB
        coutGlobalTable.write()
                .format("mongodb")
                .mode("overwrite")
                .option("collection", "cout_global")
                .save();

        // Créer la table Charge_Direct_Detail avec les données spécifiées (pas d'agrégation)
        Dataset<Row> chargeDirectDetailTable = createDetailChargeTable(dfFinal);

        // Sauvegarder la table Cout_Global dans MongoDB
        chargeDirectDetailTable.write()
                .format("mongodb")
                .mode("overwrite")
                .option("collection", "Charge_Direct_Detail")
                .save();


        // Créer la table plateaux_RR_Details avec les données spécifiées (pas d'agrégation)
        Dataset<Row> plateaux_RR_DetailsTable = createPlateaux_RR_Details(dfFinal);

        // Sauvegarder la table plateaux_RR_DetailsTable dans MongoDB
        plateaux_RR_DetailsTable.write()
                .format("mongodb")
                .mode("overwrite")
                .option("collection", "Plateaux_RR_Details")
                .save();


        // Créer la table plateaux_RR_Details avec les données spécifiées (pas d'agrégation)
        Dataset<Row> plateaux_MT_DetailsTable = createPlateaux_MT_Details(dfFinal);

        // Sauvegarder la table plateaux_RR_DetailsTable dans MongoDB
        plateaux_MT_DetailsTable.write()
                .format("mongodb")
                .mode("overwrite")
                .option("collection", "Plateaux_MT_DetailsT")
                .save();

        // Créer la table plateaux_RR_Details avec les données spécifiées (pas d'agrégation)
        Dataset<Row> structureDetailTable = createStructureDetailTable(dfFinal);

        // Sauvegarder la table structureDetailTable dans MongoDB
        structureDetailTable.write()
                .format("mongodb")
                .mode("overwrite")
                .option("collection", "Structure_Detail")
                .save();

        // Créer la table plateaux_RR_Details avec les données spécifiées (pas d'agrégation)
        Dataset<Row> logistique_Medical_Details = createLogistique_Medical_DetailsTable(dfFinal);

        // Sauvegarder la table structureDetailTable dans MongoDB
        logistique_Medical_Details.write()
                .format("mongodb")
                .mode("overwrite")
                .option("collection", "Logistique_Medical_Details")
                .save();

        spark.stop();


    }

    public static Dataset<Row> loadFilesWithYear(SparkSession spark, String hdfsPath) {
        Dataset<Row> dfFinal = null;

        for (int year = 2010; year <= 2019; year++) {
            String filePath = hdfsPath + "*" + year + ".csv";
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(filePath);

            df = df.withColumn("Annee", functions.lit(year));

            if (dfFinal == null) {
                dfFinal = df;
            } else {
                dfFinal = dfFinal.unionByName(df, true);
            }
        }

        return dfFinal;
    }

    public static Dataset<Row> addDatesColumns(Dataset<Row> df) {
        // UDF pour générer une date d'entrée aléatoire dans l'année
        UDF1<Integer, Date> randomDateUDF = (Integer year) -> {
            if (year == null) return null;
            Random random = new Random();
            LocalDate startDate = LocalDate.of(year, 1, 1);
            LocalDate endDate = LocalDate.of(year, 12, 31);
            long randomDay = random.nextInt((int) (endDate.toEpochDay() - startDate.toEpochDay() + 1));
            return Date.valueOf(startDate.plusDays(randomDay));
        };

        // UDF pour formater les dates au format "yyyy-MM-dd"
        UDF1<Date, String> formatDateUDF = (Date date) -> {
            if (date == null) return null;
            return date.toLocalDate().toString();
        };

        // Enregistrer les UDFs dans Spark
        df.sparkSession().udf().register("randomDate", randomDateUDF, DataTypes.DateType);
        df.sparkSession().udf().register("formatDate", formatDateUDF, DataTypes.StringType);

        // Ajouter les colonnes
        df = df.withColumn("date_arrivee_urgence", functions.callUDF("randomDate", functions.col("Annee").cast(DataTypes.IntegerType)));



        // Formater les colonnes pour ne garder que "yyyy-MM-dd"
        df = df.withColumn("date_arrivee_urgence", functions.callUDF("formatDate", functions.col("date_arrivee_urgence")));

        return df;
    }

    public static Dataset<Row> replaceNullsInDoubleColumns(Dataset<Row> df) {
        // Identifiez les colonnes de type Double
        StructField[] fields = df.schema().fields();
        List<String> doubleColumns = new ArrayList<>();

        for (StructField field : fields) {
            if (field.dataType().equals(DataTypes.DoubleType)) {
                doubleColumns.add(field.name());
            }
        }

        // Remplacez les valeurs nulles par 0 pour les colonnes de type Double
        df = df.na().fill(0.0, doubleColumns.toArray(new String[0]));

        return df;
    }

    // Créer la table Cout_Global avec les colonnes spécifiées (sans agrégation)
    public static Dataset<Row> createCoutGlobalTable(Dataset<Row> df) {
        // Sélectionner les colonnes spécifiques pour la table Cout_Global
        return df.withColumn("cout_global_id", functions.monotonically_increasing_id())  // Générer un identifiant unique
                .select(
                        "Charges directes",
                        "Dépenses cliniques",
                        "Dépenses plateaux MT",
                        "Dépenses plateaux de RR",
                        "Dépenses métiers",
                        "Sections spécifiques SSR",
                        "Logistique médicale",
                        "Logistique et gestion générale",
                        "Structure"
                );

    }

    // Créer la table Cout_Global avec les colonnes spécifiées (sans agrégation)
    public static Dataset<Row> createDetailChargeTable(Dataset<Row> df) {
        // Sélectionner les colonnes spécifiques pour la table Cout_Global
        return df.withColumn("charge_detail_id", functions.monotonically_increasing_id())  // Générer un identifiant unique
                .select(
                        "Spécialités pharmaceutiques liste traceurs",
                        "Spécialités pharmaceutiques hors liste traceurs",
                        "Médicaments sous ATU",
                        "Produits sanguins labiles",
                        "Consommables médicaux",
                        "Matériels médicaux traceurs",
                        "Consommables médicaux hors liste traceurs",
                        "Matériels médicaux hors traceurs",
                        "Sous-traitance à caractère médical - Laboratoires",
                        "Sous-traitance à caractère médical - Laboratoires HN",
                        "Sous-traitance à caractère médical - Imagerie",
                        "Sous-traitance à caractère médical - Exploration fonctionnelle",
                        "Sous-traitance à caractère médical - SMUR",
                        "Sous-traitance à caractère médical - Transport des patients - hors SMUR",
                        "Sous-traitance à caractère médical - Autre sous-traitance",
                        "Sous-traitance à caractère médical - Confection de prothèses et d'orthèses",
                        "Dispositifs médicaux utilisés au cours du processus d'appareillage et de confection",
                        "Honoraires des PH",
                        "Honoraires des personnels de RR"

                );
    }

    // Créer la table Cout_Global avec les colonnes spécifiées (sans agrégation)
    public static Dataset<Row> createPlateaux_RR_Details(Dataset<Row> df) {
        // Sélectionner les colonnes spécifiques
        return df.withColumn("id", functions.monotonically_increasing_id())  // Générer un identifiant unique
                .select(
                        "Espace d'ergothérapie",
                        "Salle d'orthoptie",
                        "Cuisine éducative",
                        "Locaux de simulation de logement",
                        "Appartement d'autonomie",
                        "Appareil d'isocinétisme",
                        "Laboratoire d'analyse du mouvement, de l'équilibre et de la marche",
                        "Assistance robotisé à la marche (Lokomat…)",
                        "Lokomat des membres supérieurs",
                        "Simulateur de conduite automobile",
                        "Exploration de l'équilibre et de la posture",
                        "Douche filiforme pour grands brûlés",
                        "Chambre domotisée",
                        "Salle multisensorielle",
                        "Gymnase (à différencier de la simple salle de gymnastique)",
                        "Piscine/balnéothérapie (au moins 20 m2)",
                        "Plateau de psychomotricité",
                        "Plateau de kinésithérapie",
                        "Autre plateau technique SSR"

                );
    }

    // Créer la table Cout_Global avec les colonnes spécifiées (sans agrégation)
    public static Dataset<Row> createPlateaux_MT_Details(Dataset<Row> df) {
        // Sélectionner les colonnes spécifiques pour la table Plateaux_MT_Details
        return df.withColumn("id", functions.monotonically_increasing_id())  // Générer un identifiant unique
                .select(
                        "Imagerie",
                        "Laboratoires hors ACP",
                        "Laboratoires ACP",
                        "Explorations fonctionnelles",
                        "Urgences",
                        "Bloc"

                );
    }

    // Créer la table Cout_Global avec les colonnes spécifiées (sans agrégation)
    public static Dataset<Row> createPatientTable(Dataset<Row> df) {
        // Sélectionner les colonnes spécifiques
        return df.withColumn("id_patient", functions.monotonically_increasing_id())  // Générer un identifiant unique
                .select(
                        "Type d'hospitalisation",
                        "GME",
                        "Libelle_GME",
                        "date_arrivee_urgence"

                );
    }

    // Créer la table Cout_Global avec les colonnes spécifiées (sans agrégation)
    public static Dataset<Row> createStructureDetailTable(Dataset<Row> df) {
        // Sélectionner les colonnes spécifiques
        return df.withColumn("id", functions.monotonically_increasing_id())  // Générer un identifiant unique
                .select(
                        "Structure - financier",
                        "Structure -immobilier"

                );
    }
    // Créer la table Cout_Global avec les colonnes spécifiées (sans agrégation)
    public static Dataset<Row> createLogistique_Medical_DetailsTable(Dataset<Row> df) {
        // Sélectionner les colonnes spécifiques
        return df.withColumn("id", functions.monotonically_increasing_id())  // Générer un identifiant unique
                .select(
                        "pharmacie",
                        "Génie biomédical",
                        "Autre logistique médicale"

                );
    }

}