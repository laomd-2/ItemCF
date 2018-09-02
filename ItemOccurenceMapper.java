import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemOccurenceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	final static IntWritable one = new IntWritable(1);
	Text k = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] str = value.toString().split("\t");
		
		String[] tmps = str[1].split(",");
		for(int i = 0; i < tmps.length; i++)
		{
			String item_i = tmps[i].split(":")[0];
			for(int j = 0; j < tmps.length; j++)
			{
				String item_j = tmps[j].split(":")[0];
				k.set(item_i+":"+item_j);
				context.write(k, one);
			}
		}

	}
}