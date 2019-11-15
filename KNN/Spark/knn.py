import sys
import time

import numpy as np
from pyspark.sql import SparkSession


def parseVector(line, split):
    '''
    Input:
        - line: String read from a file
        - split: Parameter to split the string
    
    Returns numpy array of each record with float values
    '''
    return np.array([float(x) for x in line.split(split)])


def euclideanDistance(test, train):
    '''
    Input:
        - test: Numpy Array with label as last value
        - train: Numpy Array with label as last value
    
    Returns euclidean distance between test and train arrays
    '''
    return int(train[-1]), np.sum((test[:-1] - train[:-1]) ** 2)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("\nUsage: knn.py <test_file> <training_file> <num_of_nearest_neighbours>\n")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonKNN")\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    test_lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    test_data = test_lines.map(lambda x: parseVector(x, split=' '))

    train_lines = spark.read.text(sys.argv[2]).rdd.map(lambda r: r[0])
    train_data = train_lines.map(lambda x: parseVector(x, split=' '))

    K = int(sys.argv[3])

    start_time = time.time()

    count = 0
    for test_point in test_data.collect():
        
        true_label = int(test_point[9])

        distances = train_data.map(
            lambda train_point: euclideanDistance(test_point, train_point))

        k_nearest_neighbours = sc.parallelize(
            distances.takeOrdered(K, key = lambda p: p[1])).map(
                lambda x: (x[0], 1))

        k_nearest_predictions = k_nearest_neighbours.reduceByKey(
            lambda x1, x2: x1 + x2)

        predict_label = k_nearest_predictions.takeOrdered(1,
            key = lambda x: -x[1])[0][0]

        if predict_label == true_label:
            count += 1

    end_time = time.time()
    
    accuracy = (float(count) / test_data.count()) * 100
    time_taken = end_time - start_time

    print "\nAccuracy: " + str(accuracy) + "%\n"
    print "\nTime taken: " + str(time_taken) + "\n"