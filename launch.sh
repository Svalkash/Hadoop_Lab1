# Генерация файлов, запуск задачи, расшифровка
java -jar ./target/L1Gen-jar-with-dependencies.jar input 1000000 3 60000
hdfs dfs -rm -r input
hdfs dfs -rm -r output
hdfs dfs -rm -r metricNames.csv
hdfs dfs -put input input
hdfs dfs -put metricNames.csv metricNames.csv
yarn jar ./target/L1App-jar-with-dependencies.jar input output metricNames.csv 1m avg
hdfs dfs -get output output
java -jar ./target/L1Reader-jar-with-dependencies.jar ./output/part-r-00000 output-decoded.txt
cat output-decoded.txt
#java -jar ./target/L1App-jar-with-dependencies.jar input output metricNames.csv 1m avg
#java -jar ./target/L1Reader-jar-with-dependencies.jar ./output/part-r-00000 output-decoded.txt