import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	String[] label = {"Rad Flow", "Fpv Close", "Fpv Open", "High", "Bypass", "Bpv Close", "Bpv Open"};
	int num_features = 0;
	int correctPredictions = 0;
	List<String> predictions = new ArrayList<String>();
	Configuration conf = new Configuration();
	FileSystem hdfs = FileSystem.get(conf);

	// args[0] is the path to test file (to be classified).
	BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[0]))));
	String line = null;
	int count = 0;

	// read test file features
	while((line = br.readLine()) != null){
		
		// if terminating character encounter
		if(line.equals("") || line.charAt(0) == ' ')    
			break;
		line = line.replaceAll("\\s+"," ");
		
		// parse features of the point to provide as input to mapper
		String[] features = line.toString().split("\\ ");
		for(int i = 0; i < features.length - 1; i++)
			conf.setInt("feat"+i, Integer.parseInt(features[i]));

		// set number of features, and value of k for KNN
		num_features = features.length - 1;
		conf.setInt("num_features", num_features);
		conf.setInt("k", 3);
		
		// last feature encountered is the target label
		String result = features[features.length-1];

		// create job 
		Job job = new Job(conf, "K Nearest Neighbour: " + count);

		// args[1] is the path to the training file
		FileInputFormat.setInputPaths(job, new Path(args[1]));

		// output file for temporary computation
		FileOutputFormat.setOutputPath(job, new Path("output_KNN"));

		// set relevant parameters
		job.setJarByClass(Driver.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		count++;

		// accuracy check for KNN prediction
		BufferedReader b = new BufferedReader(new InputStreamReader(hdfs.open(new Path("output_KNN/part-r-00000"))));
		String pred = b.readLine();
		String gt = label[Integer.parseInt(result) - 1]; // ground truth
		predictions.add(gt);
		if(gt.equals(pred))
			correctPredictions++;

		// set-up for next test point iteration
		hdfs.delete(new Path("output_KNN"), true);
		conf.unset("num_features");
		for(int i = 0; i < features.length; i++)
			conf.unset("feat" + i);
	}

	// Print prediction accuracy
	correctPredictions *= 100;
	Float temp = (float) correctPredictions / count;
	System.out.println("\n---------------------");
	System.out.println("Test Accuracy: " + temp.toString());
	System.out.println("---------------------");
	
	// write test predictions to file
	FSDataOutputStream fs = hdfs.create(new Path("predictions_KNN.txt"));
	for(int i = 0; i < predictions.size(); i++)
		fs.writeBytes(predictions.get(i) + "\n");
	
	fs.close();
	br.close();
	hdfs.close();
	}
}
