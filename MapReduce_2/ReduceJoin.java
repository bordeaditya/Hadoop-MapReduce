import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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


public class ReduceJoin {
	
	//The Mapper classes and  reducer  code
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		String mymovieid;
		HashMap<String,String> map;
		
		protected void setup(Context context) throws IOException, InterruptedException {
				map = new HashMap<String, String >();
				
				Path[] localPaths = context.getLocalCacheFiles();
				for(Path file:localPaths)
				{
					String line=null;
					String nameofFile= file.getName();
					File tempFile =new File(nameofFile+"");
					FileReader fr= new FileReader(tempFile);
					BufferedReader br= new BufferedReader(fr);
					line=br.readLine();
					while(line!=null)
					{
						// Split the String
						String[] stringPart = line.toString().split("::");
						// stringPart[1] - Gender
						if(stringPart[1].equalsIgnoreCase("F"))
							map.put(stringPart[0], "1");	
						
						line=br.readLine();
					}
					br.close();
				}
		}
		
		// Ratings.dat
		private Text rating;
		private Text movieid = new Text();  // type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] stringParts = value.toString().split("::");
			if(map.containsKey(stringParts[0]))
			{
				movieid.set(stringParts[1].trim());
				rating = new Text("R#" + stringParts[2]);
				context.write(movieid, rating);
			}
			
		}


	}

	// Movies.dat
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		private Text movieTitle = new Text();
		private Text movieid = new Text();  // type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] stringParts = value.toString().split("::");
			movieTitle.set("T#" + stringParts[1]);
			movieid.set(stringParts[0].trim());
			context.write(movieid, movieTitle);
		}
	}
	
	//The reducer class	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		static Map<String,Integer> movieIdTitle = new HashMap<String,Integer>();
		
		private Text valueResult = new Text();
		private Text keyTitle = new Text();
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
			int totalCount =0,ratingSum=0;
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
					int average = ratingSum / totalCount;
					movieIdTitle.put(title, average);
				}
			}
		}
		
	// Clean Up function that executed At last
	protected void cleanup(Context context) throws IOException, InterruptedException{
			
			Map<String,Integer> map = sortValues(movieIdTitle,false);
			int topFiveMovies =0;
			for(Map.Entry<String, Integer> tempValue : map.entrySet())
			{
				keyTitle.set(tempValue.getKey());
				valueResult.set(tempValue.getValue().toString());
				context.write(keyTitle,valueResult);
				topFiveMovies++;
				if(topFiveMovies >=5)
					break;
			}
		}

	private Map<String, Integer> sortValues(Map<String, Integer> map, final boolean order) {
		
		LinkedList<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(map.entrySet());
			
		// Sorting the list based on values
		Collections.sort(list,new Comparator<Map.Entry<String, Integer>>() {
                        public int compare(Map.Entry<String, Integer> o1,
                                    Map.Entry<String, Integer> o2) {
                                if (order) {
                                      return o1.getValue().compareTo(o2.getValue());
                                } else {
                                      return o2.getValue().compareTo(o1.getValue());

                                }
                        }
          	});

			// Maintaining insertion order with the help of LinkedList
			Map<String, Integer> sortedValueMap = new LinkedHashMap<String, Integer>();
			for (Map.Entry<String, Integer> entry : list) {
				sortedValueMap.put(entry.getKey(), entry.getValue());
			}
			return sortedValueMap;
		}
	}

	
	// Driver Class
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
	// get all args
	if (otherArgs.length != 3) {
		System.err.println("Usage: ReduceJoin <Input1> <Input2> <Output>");
		System.exit(2);
	}

	// create a job with name "joinexc" 
	Job job = new Job(conf, "joinReduceSide"); 
	job.setJarByClass(ReduceJoin.class);


	job.setReducerClass(Reduce.class);

	// OPTIONAL :: uncomment the following line to add the Combiner
	// job.setCombinerClass(Reduce.class);
		
	final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
	job.addCacheFile(new URI(NAME_NODE+ "/user/hue/users/users.dat"));
	
	// Rating
	MultipleInputs.addInputPath(job,new Path(otherArgs[0]),TextInputFormat.class,Map1.class);
	// Movies
	MultipleInputs.addInputPath(job,new Path(otherArgs[1]),TextInputFormat.class,Map2.class);

	job.setOutputKeyClass(Text.class);
	// set output value type
	job.setOutputValueClass(Text.class);

	//set the HDFS path of the input data
	// set the HDFS path for the output 
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

	job.waitForCompletion(true);

	}

}
