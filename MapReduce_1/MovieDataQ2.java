import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieDataQ2 {
	
	public static class Map	extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable count = new IntWritable(1);
		private Text word = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
			String[] stringParts = value.toString().split("::");
			
			if(stringParts.length > 0)
			{
				word.set(stringParts[1] +"	"+ stringParts[2]); // set word as each input keyword
				context.write(word, count); // create a pair <keyword, empty>
			}
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
		{
			int sum = 0; // initialize the sum for each keyword
			for (IntWritable val : values) {
			sum += val.get();
			}
			result.set(sum);
			context.write(key, result); 
		}
	}


	// Driver program

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: MovieDataQ2 <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "moviedata");
		job.setJarByClass(MovieDataQ2.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		// uncomment the following line to add the Combiner
		//job.setCombinerClass(Reduce.class);
		
		// set output key type
		job.setOutputKeyClass(Text.class);
		
		// set output value type
		job.setOutputValueClass(IntWritable.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}