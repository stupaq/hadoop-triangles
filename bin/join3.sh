#!/bin/bash

source "`dirname $0`/commons.sh"

# Run operator
[[ "$#" -lt 3 ]] && { echo "Missing arguments..."; exit 1; }

# Arguments:
# input graph
input="$1"
# final output directory (local)
output="$2"
# concurrency level, # of reducers = level^3
level="$3"
shift 3

hadoop jar "${triangles_jar}" \
    "${root_package}.join3.Join3" \
    "${input}" \
    "${temp_output}" \
    "${level}"

rm -f "${output}"
hdfs dfs -getmerge "${temp_output}" "${output}"
