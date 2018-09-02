import java.net.URI;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecommendMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	Text k = new Text();
	DoubleWritable v = new DoubleWritable();
	Map<String, Map<String, Integer>> t_matrix = new HashMap<String, Map<String, Integer>>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

	    FileReader fileReader = new FileReader(new File(context.getLocalCacheFiles()[0].getName()));
	    BufferedReader buff = new BufferedReader(fileReader);
	    
	    String line;
	    while((line = buff.readLine()) != null)
	    {
	    	String[] str = line.split("\t");
	    	String[] items = str[0].split(":");
	    	String item_i = items[0];
	    	String item_j = items[1];
	    	
	    	Integer perference = Integer.parseInt(str[1]);
	    	Map<String, Integer> t_ij;
	    	if (t_matrix.containsKey(item_i)) {
				t_ij = t_matrix.get(item_i);
			}
	    	else {
				t_ij = new HashMap<String, Integer>();
				t_matrix.put(item_i, t_ij);
			}
	    	t_ij.put(item_j, perference);
	    }
	    buff.close();
	    fileReader.close();
	}
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] str = value.toString().split("\t");
		String userId = str[0];
		
		// matrix production
		for(Map.Entry<String, Map<String, Integer>> row : t_matrix.entrySet())
		{
			String item = row.getKey();
			// filter items that have been used.
			if(!value.toString().contains(item))
			{
				double weight = 0.0;
			    Map<String, Integer> t_i = row.getValue();
			    String[] tmps = str[1].split(",");
				for(int i = 0; i < tmps.length; i++)
				{
					try {
						String[] item_score = tmps[i].split(":");
						double score = Double.parseDouble(item_score[1]);
						weight += score * t_i.get(item_score[0]);	
					}
					catch (Exception e) {
						
					}
				}
				k.set(userId + ":" + item);
				v.set(weight);
				context.write(k, v);
			}
		}
		
	}

}