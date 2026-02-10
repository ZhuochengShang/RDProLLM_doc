#!/bin/bash
# common-beast.sh

# Initialize jars and packages list
declare -a jars
declare -a packages=("org.mortbay.jetty:jetty:6.1.26" "org.eclipse.jetty:jetty-servlet:9.4.48.v20220622" "org.eclipse.jetty:jetty-server:9.4.48.v20220622" "org.geotools:gt-epsg-hsql:26.1" "org.geotools:gt-coverage:26.1" "org.locationtech.jts:jts-core:1.19.0" "com.h2database:h2:2.2.224" "com.google.protobuf:protobuf-java:3.21.12")
declare -a excluded_packages=("org.apache.hadoop:hadoop-common" "org.apache.hadoop:hadoop-hdfs" "javax.media:jai_core")
declare -a repositories=("https://repo.osgeo.org/repository/release/")

# Initialize path to lib dir and check for existing beast-spark JAR
lib_dir="$(dirname "$0")/../lib"

# Adding error handling for lib_dir
if [ ! -d "$lib_dir" ]; then
  echo "Library directory does not exist: $lib_dir"
  exit 1
fi

declare -a additional_spark_args
# Populate jars with all jar files under ../lib/
for jar_file in "$lib_dir"/*.jar; do
  jars+=("$jar_file")
done

# Handle command line arguments
# Loop over command-line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    shift # remove the first argument from the list

    if [[ $key == "--jars" ]]; then
        jars+=("$1")
        shift # remove the value argument
    elif [[ $key == "--packages" ]]; then
        packages+=("$1")
        shift
    elif [[ $key == "--repositories" ]]; then
        repositories+=("$1")
        shift
    elif [[ $key == "--exclude-package" ]]; then
        excluded_packages+=("$1")
        shift
    elif [[ $key == --* ]]; then
        additional_spark_args+=("$key" "$1")
        shift
    else
        program="$key"
        break # exit the loop when the first non-option argument is found
    fi
done

# Generate Spark arguments
spark_args="--jars $(IFS=,; echo "${jars[*]}") --packages $(IFS=,; echo "${packages[*]}") --repositories $(IFS=,; echo "${repositories[*]}") --exclude-packages $(IFS=,; echo "${excluded_packages[*]}")"
spark_args+=" ${additional_spark_args[*]}"

export lib_dir
export spark_args
export program