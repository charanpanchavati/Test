import java.io.File;
import java.io.IOException;
import java.net.URI;

import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Q2 {
	
	//USER
	public static class MapTableUsers extends
	Mapper<LongWritable, Text, Text, Text> {
		private Text key;
		private Text val = new Text();
		public void map(LongWritable k, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineArray = line.split("::");
			if(lineArray[1].equalsIgnoreCase("M")){
				if(lineArray.length == 5){
			key = new Text(lineArray[0]);
			val = new Text("u" + line);
			context.write(key, val);
			}
			}
		}
	}
	
	//RATING1
	public static class MapTableRatings1 extends
	Mapper<LongWritable, Text, Text, Text> {
		private Text key;
		private Text val = new Text();
		public void map(LongWritable k, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineArray = line.split("::");
			if(lineArray.length == 4){
			key = new Text(lineArray[0]);
			val = new Text("r" + line);
			context.write(key, val);
			}
		}
	}
	//Intermediate
		public static class MapTableInter extends
		Mapper<LongWritable, Text, Text, Text> {
			private Text key;
			private Text val = new Text();
			public void map(LongWritable k, Text value, Context context)
					throws IOException, InterruptedException {
				String line = value.toString();
				String[] lineArray = line.split("::");
				key = new Text(lineArray[1]);
				val = new Text("i" + line);
				context.write(key, val);
				
			}
		}
	
	//Movies
	public static class MapTableMovies extends
	Mapper<LongWritable, Text, Text, Text> {
		private Text key;
		private Text val = new Text();
		public void map(LongWritable k, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineArray = line.split("::");
			if(lineArray.length == 3){
			if(lineArray[2].trim().contains("Action")||lineArray[2].trim().contains("Drama")){
			key = new Text(lineArray[0]);
			val = new Text("m" + line);
			context.write(key, val);
			}
			}
		}
	}
	
	
//**************************************************************************************************************	
	//REDUCER
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private static final Text EMPTY_TEXT = new Text("");
		private Text tmp = new Text();
		private static ArrayList<Text> listUser = new ArrayList<Text>();
		private static ArrayList<Text> listRating1 = new ArrayList<Text>();
		
		private static String joinType = null;
		
		
		public void setup(Context context) {
			// Get the type of join from our configuration
			joinType = context.getConfiguration().get("join.type");
		}
		
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			listUser.clear();
			listRating1.clear();
		
			
			for (Text tmp : values) {
				if (tmp.charAt(0) == 'u') {
					listUser.add(new Text(tmp.toString().substring(1)));
				} else if (tmp.charAt(0) == 'r') {
					listRating1.add(new Text(tmp.toString().substring(1)));
				}
			}
				if (!listUser.isEmpty() && !listRating1.isEmpty()) {
					for (Text A : listUser) {
						for (Text B : listRating1) {
							String temp = "::"+A;
								  context.write(B, new Text(temp));	
							
						}
					}
				}		
		
		}
	}
	
	
	
	
	
	//REDUCER2
	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		private static final Text EMPTY_TEXT = new Text("");
		private Text tmp = new Text();
		private static ArrayList<Text> listInter = new ArrayList<Text>();
		private static ArrayList<Text> listMovies = new ArrayList<Text>();
		
		private static String joinType = null;
		
		
		public void setup(Context context) {
			// Get the type of join from our configuration
			joinType = context.getConfiguration().get("join.type");
		}
		
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			listInter.clear();
			listMovies.clear();
		
			
			for (Text tmp : values) {
				if (tmp.charAt(0) == 'i') {
					listInter.add(new Text(tmp.toString().substring(1)));
				} else if (tmp.charAt(0) == 'm') {
					listMovies.add(new Text(tmp.toString().substring(1)));
				}
			}
				if (!listInter.isEmpty() && !listMovies.isEmpty()) {
					for (Text A : listInter) {
						for (Text B : listMovies) {
							String temp = "::"+B;
								  context.write(A, new Text(temp));	
							
						}
					}
				}		
		
		}
	}
	
	
	
//**************************************************************************************************************
	
	// Map1 to split the Map
	public static class Map1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split("::");
			Integer value1 = Integer.parseInt(arr[2]);
			DoubleWritable val = new DoubleWritable(value1);
			word.set(arr[9]+ "::"+arr[10]+"::"+arr[11]);
			context.write(word, val);
		}
	}
	// Reducer to find the average
	public static class Reduce1 extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			// float average;
			double sum = 0.0;
			int count = 0;
			int total = 0;
			for (DoubleWritable val : values) {
				total = (int) (total + val.get());
				sum += val.get();
				count++;
			}
			// Average rating
			double average = sum / count;
		       if(average<=4.7&&average>=4.4){
			   context.write(key, new DoubleWritable(average));
		       }
		}
	}	
	
	
	//***********************************************************************************************************
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		Job job = new Job(conf1, "Reduce Job");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(Q2.class);
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// INPUT PATH
		FileSystem fs = FileSystem.get(conf1);
		Path path1 = new Path(args[0]);
		Path path2 = new Path(args[1]);
		Path path3 = new Path(args[3]);
		
		
		if(!fs.exists(path1)){
			System.out.println(args[0] + " doesn't exist. Try again");
			System.exit(0);
		}
		if(!fs.exists(path2)){
			System.out.println(args[1] + " doesn't exist. Try again");
			System.exit(0);
		}
		if(!fs.exists(path3)){
			System.out.println(args[3] + " doesn't exist. Try again");
			System.exit(0);
		}
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, MapTableUsers.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MapTableRatings1.class);
		/*MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MapTableRatings2.class);
		MultipleInputs.addInputPath(job, new Path(args[2]),TextInputFormat.class, MapTableMovies.class);*/
		// OUTPUT PATH
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//JOIN TYPE
		//job.getConfiguration().set("join.type", args[3]);
		
		if(job.waitForCompletion(true)){
			Job job2 = new Job(conf1, "Map-red2");
			
			job2.setJarByClass(Q2.class);
			
			job2.setReducerClass(Reduce2.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			MultipleInputs.addInputPath(job2, new Path(args[2]),TextInputFormat.class, MapTableInter.class);
			MultipleInputs.addInputPath(job2, new Path(args[3]),TextInputFormat.class, MapTableMovies.class);
			FileOutputFormat.setOutputPath(job2, new Path(args[4]));
			
			if(job2.waitForCompletion(true)){
				Job job3 = new Job(conf1, "Map-red3");
				
				job3.setJarByClass(Q2.class);
				
				job3.setReducerClass(Reduce1.class);
				job3.setMapperClass(Map1.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(DoubleWritable.class);
				//job3.setOutputFormatClass(TextOutputFormat.class);
				
				//MultipleInputs.addInputPath(job3, new Path(args[4]),TextInputFormat.class, MapTableInter.class);
				
				
				
				FileInputFormat.addInputPath(job3, new Path(args[4]));
				
				FileOutputFormat.setOutputPath(job3, new Path(args[5]));
				
				System.exit(job3.waitForCompletion(true) ? 0 : 1);
				}
		}
	}
}