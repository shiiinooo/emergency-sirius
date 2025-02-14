#!/bin/bash

# Set up Spark environment
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH


# Define variables
APP_NAME="911_Calls_Processing"
MASTER_MODE="yarn"
DEPLOY_MODE="cluster"
MAIN_CLASS="com.example.spark.Main"
PROJECT_DIR="/opt/hadoop/spark-jobs/911_calls"
JAR_PATH="$PROJECT_DIR/target/job-1.0.0-jar-with-dependencies.jar"
PACKAGES="org.mongodb.spark:mongo-spark-connector_2.12:10.1.0"

# Step 1: Build the project using Maven
echo "Building the project with Maven..."
cd "$PROJECT_DIR" || { echo "Failed to change directory to $PROJECT_DIR"; exit 1; }
mvn clean package
if [ $? -ne 0 ]; then
  echo "Maven build failed. Exiting."
  exit 1
fi

# Step 2: Submit the Spark job
echo "Submitting the Spark job..."
spark-submit \
  --master "$MASTER_MODE" \
  --deploy-mode "$DEPLOY_MODE" \
  --class "$MAIN_CLASS" \
  --packages "$PACKAGES" \
  --name "$APP_NAME" \
  "$JAR_PATH"

# Check the exit status of spark-submit
if [ $? -ne 0 ]; then
  echo "Spark job submission failed. Exiting."
  exit 1
fi

echo "Spark job submitted successfully."