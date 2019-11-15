# K-Nearest Neighbours: Hadoop Implementation

## Requirements
- Python 3 (3.7.3)
- Hadoop (3.2.1)

## Steps to execute the script
- Start HDFS Namenode and Datanode servers.
- Add the train and test to HDFS
    + `hdfs dfs -mkdir -p input`
    + `hdfs dfs -put <path_to_dataset> input`
- Run the script.sh bash file which contains the commands to execute map-reduce
    + `sh script.sh`

### Note
- Code is well commented for ease of understanding. 
- Shuttle dataset is used for KNN.
- Test accuracy is computed and printed on the screen, while label predictions are written to `predictions_KNN.txt` in the HDFS.
- Data is space seperated in the dataset, the corresponding parameter can be changed in the string split.