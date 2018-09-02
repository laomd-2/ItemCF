import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.JsonArray;

public class ItemCF {

	static Configuration conf = new Configuration();

	public static Job addJob(String jobName, String inputPath, String outputPath,
						Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass,
						Class<?> outputKeyClass, Class<?> outputValueClass) 
			throws Exception {
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(ItemCF.class);
	    job.setMapperClass(mapperClass);
	    job.setReducerClass(reducerClass);
	    job.setOutputKeyClass(outputKeyClass);
	    job.setOutputValueClass(outputValueClass);

	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));

	    return job;
	}


	public static void main(String[] args) throws Exception {
		
	    String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	    if (otherArgs.length != 2) {
			System.err.println("Usage: ItemCF <in> <out>");
			System.exit(1);
		}
	    Job job1 = addJob("UserScore", otherArgs[0], "/temp1",
	    				UserScoreMapper.class, UserScoreReducer.class,
	    				Text.class, Text.class);
	    job1.setCombinerClass(UserScoreReducer.class);
	    if (job1.waitForCompletion(true)) {
	    	Job job2 = addJob("ItemOccurence", "/temp1", "/temp2",
	    				ItemOccurenceMapper.class, ItemOccurenceReducer.class,
	    				Text.class, IntWritable.class);
	    	job2.setCombinerClass(ItemOccurenceReducer.class);
			if (job2.waitForCompletion(true)) {
				Job job3 = addJob("ItemCF", "/temp1", otherArgs[1],
	    				RecommendMapper.class, RecommendReducer.class,
	    				Text.class, Text.class);
				job3.setMapOutputValueClass(DoubleWritable.class);
				
				job3.addCacheFile(new URI("/temp2/part-r-00000"));
				System.exit(job3.waitForCompletion(true)? 0 : 1);
			}
			
			System.exit(job2.waitForCompletion(true)? 0 : 1);
			
		}
	    System.exit(job1.waitForCompletion(true)? 0 : 1);
	
	}
}