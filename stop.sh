# Остановка файловой системы Hadoop
hdfs dfs -rm -r input
hdfs dfs -rm -r output
hdfs dfs -rm -r metricNames.csv
/opt/hadoop-2.10.1/sbin/stop-yarn.sh
/opt/hadoop-2.10.1/sbin/stop-dfs.sh