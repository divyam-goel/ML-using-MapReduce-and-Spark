hdfs dfs -rm predictions_KNN.txt
hdfs dfs -rm -r -f output_KNN
cd src
javac -cp $(hadoop classpath) *.java
jar cf KNN.jar *.class
mv KNN.jar .. 
cd ..
HADOOP_CLIENT_OPTS="-Xmx10g" hadoop jar KNN.jar Driver input/shuttle.tst input/shuttle.trn

# hdfs dfs -cat predictions_KNN.txt
