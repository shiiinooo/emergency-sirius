#!/bin/bash

# Set Hadoop environment variables
export HADOOP_HOME=/opt/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Debugging output
echo "PATH is: $PATH"

# HDFS directory where files will be uploaded
HDFS_TARGET_DIR="/user/hadoop/silver/scansante"

# Local directory containing files to upload
LOCAL_SOURCE_DIR="/opt/hadoop/data/silver/scansante"

# Check if the local directory exists
if [ ! -d "$LOCAL_SOURCE_DIR" ]; then
    echo "Local directory $LOCAL_SOURCE_DIR does not exist."
    exit 1
fi

# Remove all files in the HDFS target directory
echo "Deleting all files in HDFS directory $HDFS_TARGET_DIR..."
hadoop fs -rm -r "$HDFS_TARGET_DIR/*"
if [ $? -eq 0 ]; then
    echo "Successfully deleted all files in $HDFS_TARGET_DIR."
else
    echo "Failed to delete files in $HDFS_TARGET_DIR."
    exit 1
fi

# Upload files from the local directory to HDFS
echo "Uploading files from $LOCAL_SOURCE_DIR to HDFS directory $HDFS_TARGET_DIR"
for file in $LOCAL_SOURCE_DIR/*.csv; do
    if [ -f "$file" ]; then
        echo "Uploading $file to HDFS..."
        hadoop fs -put "$file" "$HDFS_TARGET_DIR/"
        if [ $? -eq 0 ]; then
            echo "Successfully uploaded $file to HDFS."
        else
            echo "Failed to upload $file to HDFS."
            exit 1
        fi
    fi
done

