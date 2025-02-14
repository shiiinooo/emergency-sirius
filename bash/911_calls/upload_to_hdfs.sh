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

# HDFS directory where the file will be uploaded
HDFS_TARGET_DIR="/user/hadoop/silver/911_calls"

# Local directory containing the file to upload
LOCAL_SOURCE_DIR="/opt/hadoop/data/silver/911_calls"

# Specific file to upload
TARGET_FILE="raw_911_calls.csv"

# Create the target directory in HDFS if it doesn't exist
echo "Creating HDFS directory: $HDFS_TARGET_DIR"
hadoop fs -mkdir -p $HDFS_TARGET_DIR

# Check if the local directory exists
if [ ! -d "$LOCAL_SOURCE_DIR" ]; then
    echo "Local directory $LOCAL_SOURCE_DIR does not exist."
    exit 1
fi

# Check if the specific file exists in the local directory
if [ -f "$LOCAL_SOURCE_DIR/$TARGET_FILE" ]; then
    echo "Uploading $TARGET_FILE to HDFS directory $HDFS_TARGET_DIR"
    hadoop fs -put "$LOCAL_SOURCE_DIR/$TARGET_FILE" $HDFS_TARGET_DIR/
    if [ $? -eq 0 ]; then
        echo "Successfully uploaded $TARGET_FILE to HDFS."
    else
        echo "Failed to upload $TARGET_FILE to HDFS."
    fi
else
    echo "File $TARGET_FILE does not exist in the directory $LOCAL_SOURCE_DIR."
    exit 1
fi

echo "Upload process completed."