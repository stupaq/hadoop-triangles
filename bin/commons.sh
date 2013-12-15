#!/bin/sh

# Configuration
triangles_jar="target/triangles-0.1-SNAPSHOT.jar"
root_package="pl.stupaq.hadoop.triangles"
temp_output="/tmp/triangles_job_output-`date +%s`"

# Register cleanup
trap "hdfs dfs -rm -r ${temp_output} 2>/dev/null" EXIT
