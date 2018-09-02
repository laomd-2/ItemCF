import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecommendReducer extends Reducer<Text, DoubleWritable, Text, Text>{

	Text k = new Text();
	Text v = new Text();
	Map<String, Map<Double, String>> r_matrix = new HashMap<String, Map<Double, String>>();

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double totalScore = 0.0;
		for(DoubleWritable d : values)
		{
			totalScore += d.get();
		}

		String str[] = key.toString().split(":");
		if (!r_matrix.containsKey(str[0])) {
			r_matrix.put(str[0], new TreeMap<Double, String>(new Comparator(){
					public int compare(Object o1, Object o2)
					{
						return ((Comparable)o2).compareTo((Double)o1);
					}
				}
			));
		}
		r_matrix.get(str[0]).put(totalScore, str[1]);				
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		final int cnt = 10;
		for(Map.Entry<String, Map<Double, String>> entry : r_matrix.entrySet()) {
			k.set(entry.getKey());
			int tmp = cnt;
			for (Map.Entry<Double, String> e : entry.getValue().entrySet()) {
				v.set(e.getValue() + ":" + e.getKey());
				context.write(k, v);
				tmp--;
				if (tmp <= 0)	break;
			}				
		}
	}
}