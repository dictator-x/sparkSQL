#!/usr/bin/sh
mvn clean package
time=20181007
PROCESS_DATE=`date --date='1 day ago' +%Y%m%d`
${SPARK_HOME}/bin/spark-submit \
--class com.tt.sparksql.chapter08.SparkApp \
--master local \
--jars /home/dictator/.m2/repository/org/apache/kudu/kudu-client/1.7.0/kudu-client-1.7.0.jar,/home/dictator/.m2/repository/org/apache/kudu/kudu-spark2_2.11/1.7.0/kudu-spark2_2.11-1.7.0.jar \
--conf spark.time=$time \
--conf spark.raw.path="hdfs://centos:8020/access/$time/data-test.json" \
--conf spark.ip.path="hdfs://centos:8020/access/ip.txt" \
/home/dictator/work_space/sparkSQL/target/sparksql-1.0.jar
