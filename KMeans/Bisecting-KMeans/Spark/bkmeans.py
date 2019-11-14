from __future__ import print_function

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
    return np.array([float(x) for x in line.split(split)])


def closestPoint(p, centers, type_):
    '''
    Input:
        - p: Numpy Array
        - centers: List
        - type_: Integer value (1 or 2)
            - Type 1: Each element of centers is a Numpy Array
            - Type 2: Each element of centers is a tuple
                containing a Numpy Array at index - [1][1]
    
    Returns the index of center closest to p
     - Type 1: Also returns distance between closest
        center and p
    '''
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        if type_ == 1:
            tempDist = np.sum((p - centers[i]) ** 2)
        elif type_ == 2:
            tempDist = np.sum((p - centers[i][1][1]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    if type_ == 1:
        return bestIndex, closest
    elif type_ == 2:
        return bestIndex


if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: kmeans <file> <no_of_clusters> <no_of_iterations> \
            <convergence_distance>", file=sys.stderr)
        sys.exit(-1)

    start_time = time.time()
    
    spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    data = lines.map(lambda p: parseVector(p, split='\t'))

    K = int(sys.argv[2])
    NUM_ITER = int(sys.argv[3])
    CONVERGENCE_DIST = float(sys.argv[4])
    
    # Initial number of clusters is 1
    kPoints = data.takeSample(False, 1, 1)

    # Loop till K clusters exist
    while K > len(kPoints):

        # Assign each point to a cluster
        closest_var = data.map(
            lambda p: (closestPoint(p, kPoints, 1), p))
        # Map to Key-Value Configuration
        closest = closest_var.map(
            lambda p: (p[0][0], (p[0][1], p[1])))
        
        # Calculate Sum of Square Error for each cluster
        sumSquaredError = closest.reduceByKey(
            lambda p1_c1, p2_c2: (p1_c1[0] + p2_c2[0], ))

        # Find the cluter with highest SSE
        maxSSECluster = sumSquaredError.takeOrdered(1, key = lambda p: -p[1][0])

        # Filter points belonging to Max SSE Cluster
        maxSSEPoints = closest.filter(lambda p: maxSSECluster[0][0] == p[0])
        
        # Sample new cluster points in the Max SSE Cluster
        kPoints_temp = maxSSEPoints.takeSample(False, 2, 1)

        # Iterate till Convergence or till MAX_NUM_ITERS
        tempDist = 1.0
        iters = NUM_ITER
            
        while iters > 0 and tempDist > CONVERGENCE_DIST:
            print("\n\nLOOP: ", len(kPoints), " - ", NUM_ITER - iters + 1, "\n\n")

            closest_temp = maxSSEPoints.map(
                lambda p: (closestPoint(p[1][1], kPoints_temp, 2), (p[1][1], 1)))

            # Compute sum and count of all points for each centroid
            pointStats = closest_temp.reduceByKey(
                lambda p1_c1, p2_c2: (p1_c1[1] + p2_c2[1], p1_c1[0] + p2_c2[0]))

            newPoints = pointStats.map(
                lambda st: (st[0], st[1][0] / st[1][1])).collect()

            # Compute distance between corresponding old and new centroids
            tempDist = sum(np.sum((kPoints_temp[iK][1][1] - p) ** 2) for (iK, p) in newPoints)

            # Assign new centroids as the centroids
            for (iK, p) in newPoints:
                kPoints_temp[iK] = (iK, (0, p))

            iters -= 1

        kPoints.pop(maxSSECluster[0][0])
        kPoints.extend([x[1][1] for x in kPoints_temp])

    print("\nFinal centers:")
    for center in kPoints:
        print(center)

    end_time = time.time()

    print("\nTime taken: ", end_time - start_time, "\n")

    spark.stop()