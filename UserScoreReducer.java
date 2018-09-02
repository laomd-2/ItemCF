import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserScoreReducer extends Reducer<Text, Text, Text, Text>{

	Text v = new Text();
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
		String str = new String();
		for(Text v : value)
		{
			str += "," + v.toString();
		}
		v.set(str.replaceFirst(",", ""));
		context.write(key, v);
	}
}
