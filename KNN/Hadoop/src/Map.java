import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
	public static int[] feat = null;
	public static String groundTruth = null;
	String[] label = {"Rad Flow", "Fpv Close", "Fpv Open", "High", "Bypass", "Bpv Close", "Bpv Open"};
	public static ArrayList<String> distances = new ArrayList<String>();
	public static float min_dist = 0;
	public static int num_features = 0;
	public static int k = 0; // parameter of KNN
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		num_features = context.getConfiguration().getInt("num_features", 1);
		k = context.getConfiguration().getInt("k", 3);
		
		// get features of the passed point
		feat = new int[num_features];
		for(int i = 0; i < num_features; i++)
			feat[i] = context.getConfiguration().getInt("feat" + i, 0);
		
	}

	// compute L2 distance between two points
	public static float distance_l2(int[] feat, int[] test, int num) {
		float distance = 0;
		float val = 0;

		for(int i = 0; i < num; i++)
			val += (feat[i] - test[i]) * (feat[i] - test[i]);
		
		distance = (float) Math.sqrt(val);
		return distance;
	}

	// mapper function: called for each training point for each test point
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
		distances.add(String.valueOf(distance_l2(feat, test, num_features)) + type);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Collections.sort(distances);
		String[] groundTruth = new String[k];
		String str = "";

		// get k nearest points to the test point
		for(int i = 0; i < k; i++) {
			str = distances.get(i);
			String s = String.valueOf(str.replaceAll("[\\d.]", ""));
			groundTruth[i] = s;
		}

		// write labels of the k nearest points
		Arrays.sort(groundTruth);
		for(int i = 0; i < k - 1; i++){
			if(groundTruth[i].equals(groundTruth[i+1])){
				context.write(new Text("1"), new Text(groundTruth[i]));
				break;
			}
		}
		
		// if no majority found - break ties randomly
		Random rand = new Random(4123);
		int randomLabel = rand.nextInt(k);
		context.write(new Text("1"), new Text("" + randomLabel));
	}
}
