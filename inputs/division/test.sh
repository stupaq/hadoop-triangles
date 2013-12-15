#!/bin/bash

Home=`dirname $0`
Root=`dirname $0`/../../

hdfs dfs -rm -r inputs/
hdfs dfs -put inputs/ ./

$Root/bin/division.sh $Home/input /tmp/job-output 5
diff -s <(sort $Home/expected_output) <(sort /tmp/job-output)
