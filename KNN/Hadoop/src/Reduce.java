import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<IntWritable, Text, Text, Text> {

	// k,v : count, label
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		// populate hashmap containing count for each label
		// k, v: label, counts
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (Text label: values) {
			String k = label.toString();
			if(!map.containsKey(k))
				map.put(k, 1);
			
			else {
				int existingCount = map.get(k);
				map.replace(k, existingCount + 1);
			}	
		}
		
		// find maximum occurring label
		int maxValue = 0;
		String maxKey = null;
		
		for(Entry<String, Integer> entry: map.entrySet()) {
			if(entry.getValue() > maxValue) {
				maxKey = entry.getKey();
				maxValue = entry.getValue();
			}
		}
		
		context.write(null, new Text(maxKey));
	}
}
