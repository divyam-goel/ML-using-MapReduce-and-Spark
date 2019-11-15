# Bisecting K-Means: Hadoop Implementation

## Requirements
- Python 3 (3.7.3)
- Hadoop (3.2.1)

## Steps to execute the script
- Start HDFS Namenode and Datanode servers.
- Add the dataset to HDFS
    + `hdfs dfs -mkdir -p input`
    + `hdfs dfs -put <path_to_dataset> input`
- Run the script.sh bash file which contains the commands to execute map-reduce
    + `sh script.sh`

### Note
- Code is well commented for ease of understanding. 
- Data is tab seperated in the dataset, the corresponding parameter can be changed in the string split.
- Outputs for each cluster number (till k) for each iteration are saved in `output_bkmeans/iter_<num_clusters>_<num_iteration>` in the HDFS.