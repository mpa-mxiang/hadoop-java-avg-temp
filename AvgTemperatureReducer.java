package avgTemp;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTemperatureReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {

@Override
public void reduce(Text key, Iterable<IntWritable> values,
   Context context)
   throws IOException, InterruptedException {

	// Your reduce function implementation here
	int totalVal = 0;
	int numVal = 0;
	int avgVal = 0;
	for(IntWritable value : values) {
		totalVal = totalVal + value.get();
		numVal = numVal + 1;
	}
	avgVal = totalVal / numVal;
	context.write(key, new IntWritable(avgVal));
}
}

