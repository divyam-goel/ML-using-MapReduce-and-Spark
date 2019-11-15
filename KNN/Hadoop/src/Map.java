import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
	public static int[] feat = null;
	public static String groundTruth = null;
	String[] label = {"Rad Flow", "Fpv Close", "Fpv Open", "High", "Bypass", "Bpv Close", "Bpv Open"};
	public static TreeMap<Float, String> distances = new TreeMap<Float, String>(); // sorted by key
	public static float min_dist = 0;
	public static int num_features = 0;
	public static int k = 0; // parameter of KNN
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		num_features = context.getConfiguration().getInt("num_features", 1);
		k = context.getConfiguration().getInt("k", 3);
		
		// get features of the passed point
		feat = new int[num_features];
		for(int i = 0; i < num_features; i++)
			feat[i] = context.getConfiguration().getInt("feat" + i, 0);
	}

	// compute L2 distance between two points
	public static float distance_l2(int[] feat, int[] test) {
		float distance = 0;

		for(int i = 0; i < feat.length; i++)
			distance += Math.pow(feat[i] - test[i], 2);
		
		return distance;
	}

	// mapper function: called for each training point
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String temp = value.toString();
		temp = temp.replaceAll("\\s+", " ");
		String[] characteristics = temp.split("\\ ");

		// populate features of the received point
		int[] test = new int[num_features];
		for(int i = 0; i < num_features; i++)
			test[i] = Integer.parseInt(characteristics[i]);
		
		groundTruth = characteristics[num_features];
		String type = label[Integer.parseInt(groundTruth) - 1];
		distances.put(distance_l2(feat, test), type);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException  {

		// write labels for k nearest neighbours
		int count = 0;
		for(float key: distances.keySet()) {
			if (count == k) 
				break;
			
			context.write(new IntWritable(1), new Text(distances.get(key)));
			count++;
		}
	}
}
