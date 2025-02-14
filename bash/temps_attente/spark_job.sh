#!/bin/bash

# Set up Spark environment
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH


# Define variables
APP_NAME="Waiting_time_Processing"
MASTER_MODE="yarn"
DEPLOY_MODE="cluster"
MAIN_CLASS="com.example.spark.Main"
JAR_PATH="/opt/hadoop/spark-jobs/temps_attente/target/job-1.0.0-jar-with-dependencies.jar"
PACKAGES="org.mongodb.spark:mongo-spark-connector_2.12:10.1.0"

# Spark submit command
spark-submit \
  --master "$MASTER_MODE" \
  --deploy-mode "$DEPLOY_MODE" \
  --class "$MAIN_CLASS" \
  --packages "$PACKAGES" \
  --name "$APP_NAME" \
  "$JAR_PATH"