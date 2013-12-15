#!/bin/bash

Home=`dirname $0`
Root=`dirname $0`/../../

hdfs dfs -rm -r inputs/
hdfs dfs -put inputs/ ./

$Root/bin/join3.sh $Home/input /tmp/job-output 2
diff -s <(sort $Home/expected_output) <(sort /tmp/job-output)
