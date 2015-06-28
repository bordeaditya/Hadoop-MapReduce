import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MapJoin {
	//The Mapper classes and  reducer  code
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		String mymovieid;
		HashMap<String,String> map;
		
		// Ratings.dat file
		protected void setup(Context context) throws IOException, InterruptedException {
				map = new HashMap<String, String >();
				
				String movieId = context.getConfiguration().get("movieId");
				
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
						// stringPart[0] - userId
						// stringPart[1] - movieId
						// stringPart[2] - rating
						int rating = Integer.parseInt(stringPart[2]);
						if(stringPart[1].equalsIgnoreCase(movieId) && rating >=4)
							map.put(stringPart[0], "1");	
						
						line=br.readLine();
					}
					br.close();
				}
		}
		
		// Users.dat
		private Text valueResult;
		private Text keyUserId = new Text();  // type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] stringParts = value.toString().split("::");
			if(map.containsKey(stringParts[0]))
			{
				// stringParts[0] - userId
				// stringParts[1] - Gender
				// stringParts[2] - Age
				keyUserId.set(stringParts[0].trim());
				valueResult = new Text(stringParts[1]+" "+ stringParts[2]);
				context.write(keyUserId, valueResult);
			}
		}
	}

	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: MapJoin <Input_users.dat> <Input_ratings.dat> <Output> <MovieId>");
			System.exit(2);
		}
		
		conf.set("movieId", args[3]);

		Job job = new Job(conf, "joinMapSide"); 
		job.setJarByClass(MapJoin.class);

		//job.setReducerClass(Reduce.class);

		// OPTIONAL :: uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);
		
		// Ratings.dat
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		job.addCacheFile(new URI(NAME_NODE+ otherArgs[1]));
		
		// users.dat
		MultipleInputs.addInputPath(job,new Path(otherArgs[0]),TextInputFormat.class,Map1.class);
		
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);

		//set the HDFS path of the input data
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);

	}

}
