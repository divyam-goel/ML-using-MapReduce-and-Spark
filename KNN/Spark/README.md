# KNN: Spark Implementation
## Requirements
 - Python 2 (version 2.7.12)
 - Hadoop (2.7.3)
 - Spark (version 2.4.4)
 - Numpy (version 1.15.1)
 
 ## Steps to Run the Script
  - Start HDFS Namenode and Datanode Servers
  - Add train and test data to HDFS
    - `hdfs dfs -mkdir -p input`
    - `hdfs dfs -put <path_to_train_file_on_your_local_filesystem> input`
    - `hdfs dfs -put <path_to_test_file_on_your_local_filesystem> input`
  - Change your current directory to this directory
  - Run the knn.py script
    - `$SPARK_HOME/bin/spark-submit <path_to_the_knn_script> <path_to_test_file_in_hdfs> 
    <path_to_train_file_in_hdfs> <no_of_clusters>`
    - Sample: 
      `$SPARK_HOME/bin/spark-submit ./knn.py input/shuttle.tst input/shuttle.trn 3`

### Notes:
  - The logger is currently set to error right now. To disable that comment out
    `sc.setLogLevel("ERROR")` in the script.
  - The script runs on the whole dataset. If you are using a single-node cluster, this
    can take a long while. If you want to run it on a sample, make the following change:
    Replace `test_data.collect()` with `test_data.takeSample(False, 100, 5)` to run it on
    a sample of 100 test data points. Also make changes to accuracy calculation at the end.
  - If you data is not space separated, then simply change the value passed to split
    parameter of parseVector function.
