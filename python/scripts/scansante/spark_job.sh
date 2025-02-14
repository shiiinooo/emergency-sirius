#!/bin/bash

# Set up Spark environment
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH

# Spark submit command
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.example.spark.Main \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.0 \
  /opt/hadoop/spark-jobs/scansante/target/job-1.0.0-jar-with-dependencies.jar