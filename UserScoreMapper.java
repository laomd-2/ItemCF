import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserScoreMapper extends Mapper<LongWritable, Text, Text, Text>{ 
	Text k = new Text();
	Text v = new Text();
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] str = line.split(",");
		k.set(str[0]);
		v.set(str[1]+":"+str[2]);
		context.write(k, v);
	}
}