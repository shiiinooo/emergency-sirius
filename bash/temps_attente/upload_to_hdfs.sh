#!/bin/bash

# Set Hadoop environment variables
export HADOOP_HOME=/opt/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Debugging output
echo "PATH is: $PATH"

# Check if Hadoop is installed and accessible
if ! command -v hadoop &> /dev/null; then
    echo "Hadoop is not installed or not in the PATH."
    exit 1
fi

# HDFS directory where files will be uploaded
HDFS_TARGET_DIR="/user/hadoop/silver/temps_attente"

# Local directory containing files to upload
LOCAL_SOURCE_DIR="/opt/hadoop/data/silver/temps_attente"

# Create the target directory in HDFS if it doesn't exist
echo "Creating HDFS directory: $HDFS_TARGET_DIR"
hadoop fs -mkdir -p $HDFS_TARGET_DIR

# Check if the local directory exists
if [ ! -d "$LOCAL_SOURCE_DIR" ]; then
    echo "Local directory $LOCAL_SOURCE_DIR does not exist."
    exit 1
fi

# Upload files from the local directory to HDFS
echo "Uploading files from $LOCAL_SOURCE_DIR to HDFS directory $HDFS_TARGET_DIR"
for file in $LOCAL_SOURCE_DIR/*; do
    if [ -f "$file" ]; then
        echo "Uploading $file to HDFS..."
        hadoop fs -put "$file" $HDFS_TARGET_DIR/
        if [ $? -eq 0 ]; then
            echo "Successfully uploaded $file to HDFS."
        else
            echo "Failed to upload $file to HDFS."
        fi
    fi
done

echo "All files uploaded to HDFS."