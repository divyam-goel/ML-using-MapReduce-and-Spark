import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bkmeans {
	public static final HashMap<Integer, ArrayList<Integer>> points = new HashMap<Integer, ArrayList<Integer>>();
	public static ArrayList<ArrayList<Float>> centroids = new ArrayList<ArrayList<Float>>();
	public static ArrayList<Integer> pointAssignment = new ArrayList<Integer>();
	public static ArrayList<Float> sse = null; // sse corresponding to each cluster
	public static int num_clusters = 1;
	public static int max_clusters = 5;
	public static int num_features = 13;
	public static int num_iterations = 3; // iterations per bisection
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path inPath = new Path(args[0]);

		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(inPath)));
		Random rand = new Random(4123);
		
		// map-reduce setup
		String line = null;
		int count = 0;
		
		// iterating over dataset to populate points
		while((line = br.readLine()) != null) {
			String[] features = line.split("\t");
			ArrayList<Integer> list = new ArrayList<Integer>();
			for(int i = 0; i < features.length; i++) 
				list.add(Integer.parseInt(features[i]));
			
			// initially all points belong to cluster 0
			points.put(count, list);
			count++;
			pointAssignment.add(0);
		}
		
		br.close();
		
		// random centroid initialization
		for(int i = 0; i < num_clusters; i++) {
			int init = rand.nextInt(count);
			ArrayList<Float> centroid = new ArrayList<Float>();
			for(int j = 0; j < num_features; j++) {
				float feat = (float) points.get(init).get(j);
				centroid.add(feat);
			}
			centroids.add(centroid);
		}
				
		// bisection driver
		while(num_clusters <= max_clusters) {
			
			// re-initialize SSE
			sse = new ArrayList<Float>();
			for(int i = 0; i < num_clusters; i++)
				sse.add(0f);
			
			for(int i = 0; i < num_iterations; i++) {
				// job setup
				Job job = new Job(conf, "Bisecting K-Means Map-reduce");
				Path outPath = new Path(new Path("output_bkmeans"), "iter_" + num_clusters + "_" + i);
				FileInputFormat.addInputPath(job, inPath);
	            FileOutputFormat.setOutputPath(job, outPath);
	            
				job.setJarByClass(bkmeans.class);
				job.setMapperClass(Map.class);
				job.setReducerClass(Reduce.class);
				job.setMapOutputKeyClass(IntWritable.class);
	            job.setMapOutputValueClass(IntWritable.class);
	            job.setOutputKeyClass(IntWritable.class);
	            job.setOutputValueClass(Text.class);
	            job.waitForCompletion(true);
			}
			
			// terminating condition
			if(num_clusters == max_clusters) 
				break;
			
			// find cluster with max SSE
			int maxCluster = 0;
			float max_dist = Float.MIN_VALUE;
			for(int i = 0; i < sse.size(); i++) {
				if(sse.get(i) > max_dist) {
					max_dist = sse.get(i);
					maxCluster = i;
				}
			}
			
			// bisection of the two clusters
			ArrayList<Float> centroid1 = new ArrayList<Float>();
			ArrayList<Float> centroid2 = new ArrayList<Float>();
			
			// initialization for the 2 centroids
			int init = rand.nextInt(count);
			for(int j = 0; j < num_features; j++) {
				float feat = (float) points.get(init).get(j);
				centroid1.add(feat);
			}
			
			init = rand.nextInt(count);
			for(int j = 0; j < num_features; j++) {
				float feat = (float) points.get(init).get(j);
				centroid2.add(feat);
			}
			
			// update centroids
			centroids.set(maxCluster, centroid1);
			centroids.add(centroid2);
			num_clusters++;
		}
		hdfs.close();
	}
	
	// MAPPER CLASS
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		// computes distance between point and centroid
		protected float distance_l2(ArrayList<Integer> point, ArrayList<Float> centroid) {
			float distance = 0;
			for(int i = 0; i < point.size(); i++) 
				distance += (point.get(i) - centroid.get(i)) * (point.get(i) - centroid.get(i));
			return (float) Math.sqrt(distance);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] features = line.split("\t");
			ArrayList<Integer> point = new ArrayList<Integer>();
			
			for(int i = 0; i < features.length; i++) 
				point.add(Integer.parseInt(features[i]));
			
			// get index of the point
			int pointIndex = 0;
			for (Entry<Integer, ArrayList<Integer>> e: points.entrySet()) {
				if(e.getValue().equals(point)) 
					pointIndex = e.getKey();
			}
					
			// find closest centroid to the point
			int finalCentroid = -1;
			float minDistance = Float.MAX_VALUE;
			for(int i = 0; i < num_clusters; i++) {
				float dist = distance_l2(point, centroids.get(i));
				if(dist < minDistance) {
					minDistance = dist; 
					finalCentroid = i;
				}
			}
			
			context.write(new IntWritable(finalCentroid), new IntWritable(pointIndex));
		}
	}
	
	
	// REDUCER CLASS
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int centroidId = key.get();
			ArrayList<ArrayList<Integer>> clusterPoints = new ArrayList<ArrayList<Integer>>();
			ArrayList<Float> newCentroid = new ArrayList<Float>();
			int count = 0;
			float cluster_sse = 0f;
						
			// add features of all points to arraylist
			for(IntWritable value: values) {
				
				// update cluster assignment
				int pointId = value.get();
				pointAssignment.set(pointId, centroidId);
				
				ArrayList<Integer> point = points.get(pointId);
				
				clusterPoints.add(point);
				count++;
			}
			
			// recompute new centroids - feature by feature
			for(int i = 0; i < clusterPoints.get(0).size(); i++) {
				float sum = 0;
				for(int j = 0; j < clusterPoints.size(); j++) 
					sum += clusterPoints.get(j).get(i);
				sum = sum / count;
				newCentroid.add(sum);
			}
			
			// computation of SSE with new centroids
			for(IntWritable value: values) {
				int pointId = value.get();
				ArrayList<Integer> point = points.get(pointId);
				
				for(int j = 0; j < num_features; j++) 
					cluster_sse += Math.pow(point.get(j) - newCentroid.get(j), 2);
			
			}
			
			// updating records
			centroids.set(centroidId, newCentroid);		
			sse.set(centroidId, cluster_sse);
			
			String str = "Count: " + count + "\n[";
			for(int i = 0; i < newCentroid.size(); i++) 
				str += newCentroid.get(i) + " ";
			str += "]\n";
			
			context.write(key, new Text(str));
			
		}
	}
}