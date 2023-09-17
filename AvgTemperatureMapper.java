package avgTemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgTemperatureMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

private static final int MISSING = 9999;

@Override
public void map(LongWritable key, Text value, Context context)
   throws IOException, InterruptedException {
   // Your mapper implementation here
	String line = value.toString();
	String year = line.substring(15,19);
	String time = line.substring(23,27);
	int month = Integer.parseInt(line.substring(20,21));
	int airTemp = 0;
	if (line.charAt(87) == '+') {
		airTemp = Integer.parseInt(line.substring(88, 92));
	}else {
		airTemp = Integer.parseInt(line.substring(87, 92));
	}
	
	if(airTemp != MISSING && month >= 5 && month <= 8 && time.equals("0600")) {
		context.write(new Text(year), new IntWritable(airTemp));
	}
}
}

