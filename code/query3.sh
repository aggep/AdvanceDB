#!/bin/bash

# Bash script to run spark-submit with different numbers of executors

# First execution with 2 executors
spark-submit --conf "spark.driver.extraJavaOptions=-Dspark.log.level=WARN" --num-executors 2 query3d.py

# Second execution with 3 executors
spark-submit --conf "spark.driver.extraJavaOptions=-Dspark.log.level=WARN" --num-executors 3 query3d.py

# Third execution with 4 executors
spark-submit --conf "spark.driver.extraJavaOptions=-Dspark.log.level=WARN" --num-executors 4 query3d.py

echo "Executions completed."

