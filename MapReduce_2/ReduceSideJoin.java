import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReduceSideJoin {

	// Job - 1 ***
	// The Mapper classes and  reducer  code
	// Mapper to put UserId And Gender From "Users.dat"
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		private Text keyUserId = new Text();
		private Text keyGender = new Text();
		// type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			// Split the String
			String[] stringPart = value.toString().split("::");
			// stringPart[1] - Gender
			if(stringPart[1].equalsIgnoreCase("F"))
			{
				keyUserId.set(stringPart[0]);
				keyGender.set(stringPart[1]);
				context.write(keyUserId, keyGender);
			}
		}
	}
	
	
	// Mapper to put UserId, MovieId and Rating From "Ratings.dat"
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		private Text userId = new Text();
		private Text movieidRating = new Text();  // type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] stringParts = value.toString().split("::");
			System.out.println(value.toString());
			userId.set(stringParts[0]);
			// stringParts[1] - movieId
			// stringParts[2]- 	rating
			movieidRating.set(stringParts[1] +"@"+ stringParts[2]);
			context.write(userId, movieidRating);
		}
	}
	
	//The reducer class	
	public static class Reduce1 extends Reducer<Text,Text,Text,Text> {
		private Text rating = new Text();
		private Text keyMovieId = new Text();
		boolean isFemale = false;
		//note you can create a list here to store the values
		
		
		public void reduce(Text keySet, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			isFemale = false;
			ArrayList<String> valueList = new ArrayList<String>();
			// For every Value in Key
			valueList.clear();
			for(Text val:values)
			{
				valueList.add(val.toString());
				if(val.toString().equalsIgnoreCase("F"))
					isFemale = true;
			}
			
			for(String val:valueList)
			{
				if(!val.toString().equalsIgnoreCase("F") && isFemale)
				{
					String[] movieIdRating = val.toString().split("@");
					// movieIdRating[0] - MovieId
					keyMovieId.set(movieIdRating[0]);
					rating.set("::" + movieIdRating[1]);
					context.write(keyMovieId, rating);
				}
			}
		}		
	}
	
	// Job - 2 ***
	// Output File Of Job - 1  
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text>{
		private Text keyMovieId = new Text();
		private Text valueRating = new Text();
		// type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			// stringPart[1] - Gender
			if(!value.toString().equalsIgnoreCase(""))
			{
				// Split the String
				String[] stringPart = value.toString().split("::");
				// movie ID
				keyMovieId.set(stringPart[0].trim());
				// movie Rating
				valueRating.set("R#"+ stringPart[1].trim());
				context.write(keyMovieId, valueRating);
			}
		}
	}
	
	
	// Movies.dat file
	public static class Map4 extends Mapper<LongWritable, Text, Text, Text>{
		private Text movieId = new Text();
		private Text title = new Text();  // type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] stringParts = value.toString().split("::");
			// stringParts[0] - MovieId
			// stringParts[1]- 	Title
			movieId.set(stringParts[0].trim());
			title.set("T#"+ stringParts[1].trim());
			context.write(movieId, title);
		}
	}
	
	//The reducer class	
	public static class Reduce2 extends Reducer<Text,Text,Text,Text> {
		static Map<String,Double> movieIdTitle = new HashMap<String,Double>();
		
		private Text valueResult = new Text();
		private Text keyTitle = new Text();
		
		public void reduce(Text keySet, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			
			double totalCount =0,ratingSum=0;
			String title="";
			for (Text val : values) 
			{
				String[] splitted = val.toString().split("#");
				if(splitted[0].equalsIgnoreCase("R"))
				{
					ratingSum += Integer.parseInt(splitted[1].trim());
					totalCount++;
				}
				if(splitted[0].equalsIgnoreCase("T"))
				{
					title = splitted[1].trim();
				}
				
				if(totalCount!=0 && title!=null){
					double average = ratingSum / totalCount;
					movieIdTitle.put(title, average);
				}
			}
		}
		
		
		
		// Clean Up function that executed At last
		protected void cleanup(Context context) throws IOException, InterruptedException{
				
				Map<String,Double> map = sortValues(movieIdTitle);
				int topFiveMovies =0;
				for(Map.Entry<String, Double> tempValue : map.entrySet())
				{
					keyTitle.set(tempValue.getKey());
					valueResult.set(tempValue.getValue().toString());
					context.write(keyTitle,valueResult);
					topFiveMovies++;
					if(topFiveMovies >=5)
						break;
				}
		}

		private Map<String, Double> sortValues(Map<String, Double> map) {
			
			LinkedList<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(map.entrySet());
				
			// Sorting the list based on values
			Collections.sort(list,new Comparator<Map.Entry<String, Double>>() {
				public int compare(Map.Entry<String, Double> o1,Map.Entry<String, Double> o2) {
                                 return o2.getValue().compareTo(o1.getValue());
	                       }
	          	});

				// Maintaining insertion order with the help of LinkedList
				Map<String, Double> sortedValueMap = new LinkedHashMap<String, Double>();
				for (Map.Entry<String, Double> entry : list) {
					sortedValueMap.put(entry.getKey(), entry.getValue());
				}
				return sortedValueMap;
			}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// JOB 1**
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: ReduceSideJoin <users.dat> <ratings.dat> <movies.dat> <output>");
			System.exit(2);
		}

		// create a job with name "joinexc" 
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "joinReduceSide"); 
		job.setJarByClass(ReduceSideJoin.class);


		job.setReducerClass(Reduce1.class);

		// "users.dat"
		MultipleInputs.addInputPath(job,new Path(otherArgs[0]),TextInputFormat.class,Map1.class);

		// "ratings.dat"
		MultipleInputs.addInputPath(job,new Path(otherArgs[1]),TextInputFormat.class,Map2.class);

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		//set the HDFS path of the input data
		// set the HDFS path for the output 
		String outputPath = otherArgs[3] + "Job1";
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
		
		
		// JOB 2 ****
		Configuration confJob2 = new Configuration();

		// create a job with name "joinexc" 
		@SuppressWarnings("deprecation")
		Job job2 = new Job(confJob2, "joinReduceSide"); 
		job2.setJarByClass(ReduceSideJoin.class);


		job2.setReducerClass(Reduce2.class);

		// "output of Job-1" : [MovieId, Rating]
		MultipleInputs.addInputPath(job2,new Path(outputPath),TextInputFormat.class,Map3.class);

		// "Movies.dat"
		MultipleInputs.addInputPath(job2,new Path(otherArgs[2]),TextInputFormat.class,Map4.class);

		job2.setOutputKeyClass(Text.class);
		// set output value type
		job2.setOutputValueClass(Text.class);

		//set the HDFS path of the input data
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

		job2.waitForCompletion(true);

	}

}
