/*******************************************************
 *  Class name:  mapJoinHW2
 *  Author: 	 Arunkumar Manickam
 *  Description: Assignment 2
 *  			 To do Map side join
 ********************************************************/


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class MapJoin {

	// Read rating table
	public static class Mapper1
			extends
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text userID = new Text();

		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineOfFile = value.toString();
			if(lineOfFile.length()>0){
				
			String[] array = lineOfFile.split("::");
			if(array.length==4){
				//int f = Integer.parseInt(array[2]);
				// new DoubleWritable(1)
			Double rating = Double.parseDouble(array[2]);
			DoubleWritable counter = new DoubleWritable(1);            //Sending one count for each UID.
			userID.set(array[0]);
			context.write(userID, counter);
			}
			}
		}
	}

	public static class Reducer1
			extends
			org.apache.hadoop.mapreduce.Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
	
			double sumOfMovies = 0.0;
			for (DoubleWritable val : values) {
				sumOfMovies += val.get();
			}
			
			context.write(key, new DoubleWritable(sumOfMovies));
		}

	}

	
	
	
	public static class MapperII extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		int count = 0;
		HashMap<String, String> uid = new HashMap<String, String>();
		Comp comp_obj = new Comp();
		SortedMap<String, String> treeMap = new TreeMap<String, String>(comp_obj);

	public void setup(Context context) throws IOException,
				InterruptedException {
			
			
			try {
				Path[] path = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader bufferedReader = new BufferedReader(new FileReader(path[0].toString()));  //users
				String record;
				while ((record = bufferedReader.readLine()) != null) {
					
					
					String[] container = record.toString().split("::");                //splitting User
					
						String uidOfUser = container[0].trim();
						String gndr = container[1].trim();
						
						String ageUs = container[2].trim();
                        String value = ageUs +"\t"+ gndr;
                        
                        uid.put(uidOfUser, value);
						
						
				}
				bufferedReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			}
	
	
	public static class Comp implements Comparator<String> {

		@Override
		public int compare(String k1, String k2) {

			if (Float.parseFloat(k1.split("\t")[1]) > Float.parseFloat(k2
					.split("\t")[1]))
				return -1;
			else if (Float.parseFloat(k1.split("\t")[1]) < Float.parseFloat(k2
					.split("\t")[1]))
				return 1;
			else {
				if (Float.parseFloat(k1.split("\t")[0]) > Float.parseFloat(k2
						.split("\t")[0]))
					return -1;
				else
					return 1;
			}

		}
	}


		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException, NullPointerException {

			String[] line = value.toString().split("\t");
			if (line.length > 0) {

				if (uid.containsKey(line[0].trim())) {

					String s3 = line[0].trim() + "\t"
							+ uid.get(line[0].trim()) + "\t"
							+ line[1].trim();
					treeMap.put(line[0].trim() + "\t" + line[1].trim(), s3);
				}

				if (treeMap.size() > 10) {
					treeMap.remove(treeMap.firstKey());
				}

			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for(Map.Entry<String,String> entry : treeMap.entrySet()){
				try{
				String rating_movie = entry.getValue();
				context.write(NullWritable.get(),new Text(rating_movie));
				
				}
				catch (Exception e){
				
				}
			}
			}
	}

	public static class ReducerII extends
			Reducer<NullWritable, Text, NullWritable, Text> {
		int count = 0;

		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException,
				NullPointerException {
			for (Text T : values) {
				if (count < 10) {
					count++;
					try {
						context.write(NullWritable.get(), T);
					} 
					catch (Exception e) {
					}
				} 
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		
		FileSystem fs = FileSystem.get(conf);
		Path path1 = new Path(args[0]);
		Path path2 = new Path(args[2]);
		
		if(!fs.exists(path1)){
			System.out.println(args[0] + " File doesn't exist");
			System.exit(0);
		}
		if(!fs.exists(path2)){
			System.out.println(args[1] + " File doesn't exist");
			System.exit(0);
		}

		Job job1 = new Job(conf, "Map-Red1");

		job1.setJarByClass(MapJoin.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);

		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);

		// Ratings file
		TextInputFormat.setInputPaths(job1, new Path(args[0]));

		// Intermediate O/P file
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));

		while (job1.waitForCompletion(true)) {

			Job job2 = new Job(conf, "Map-Red2");
			// Movies file
			DistributedCache.addCacheFile(new Path(args[2]).toUri(),
					job2.getConfiguration());

			// Intermediate File MovieID, avg (rating)
			TextInputFormat.setInputPaths(job2, new Path(args[1]));

			// Output
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));

			job2.setMapperClass(MapperII.class);
			job2.setReducerClass(ReducerII.class);

			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			job2.setJarByClass(MapJoin.class);

			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}

}