import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemOccurenceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	IntWritable num = new IntWritable();
	@Override
	public void reduce(Text key, Iterable<IntWritable> value,
			Context context) throws IOException, InterruptedException {
		int cnt = 0;
		for(IntWritable v : value)
		{
			cnt += v.get();
		}
		num.set(cnt);
		context.write(key, num);
	}

}
