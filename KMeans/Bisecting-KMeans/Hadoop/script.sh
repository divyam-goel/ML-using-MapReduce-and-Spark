hdfs dfs -rm -r -f output_bkmeans*
cd src
javac -cp $(hadoop classpath) *.java
jar cf kmeans.jar *.class
mv kmeans.jar .. 
cd ..
HADOOP_CLIENT_OPTS="-Xmx10g" hadoop jar kmeans.jar bkmeans input/tcl_part

hdfs dfs -cat output_bkmeans/iter_5_2/part-r-00000