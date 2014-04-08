/*******************************************************
 *  Class name:  ReducerJoin
 *  Author: 	 Logeshwaran B
 *  Description: Assignment 2
 *  			 To do Reduce side join
 ********************************************************/

import java.util.ArrayList;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class ReduceSideJoin {

	public static class UserT extends
			Mapper<LongWritable, Text, Text, Text> {
		

		public void map(LongWritable k, Text value, Context context)
				 {
			String line = value.toString();
			if(line.length()>0){
			String[] lnarr = line.split("::");
			if (lnarr[1].equalsIgnoreCase("M")) {
				if (lnarr.length == 5) {
				try {
						context.write(new Text(lnarr[0]), new Text("A" + line));
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			}
		}}
	}

	public static class TRating extends
			Mapper<LongWritable, Text, Text, Text> {
		

		public void map(LongWritable k, Text value, Context context)
				 {
			String line = value.toString();
			if(line.length()>0){
			String[] lnarr = line.split("::");
			if (lnarr.length == 4) {
			try {
					context.write(new Text(lnarr[0]), new Text("B" + line));
				} catch (Exception e) {
					
				}
			}
			}
		}

	}

	// REDUCER
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private static ArrayList<Text> UserList = new ArrayList<Text>();
		private static ArrayList<Text> RatingList = new ArrayList<Text>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				 {
			UserList.clear();
			RatingList.clear();
			for (Text tmp : values) {
				if (tmp.charAt(0) == 'A') {
					UserList.add(new Text(tmp.toString().substring(1)));
				} else if (tmp.charAt(0) == 'B') {
					RatingList.add(new Text(tmp.toString().substring(1)));
				}
			}

			if (!UserList.isEmpty() && !RatingList.isEmpty()) {
				for (Text A : UserList) {
					for (Text B : RatingList) {
						try {
							context.write(B, new Text("::" + A));
						} catch (Exception e) {} 

					}
				}
			}

		}

	}

	

	public static class Temp extends
			Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable k, Text value, Context context)
				{
			String line = value.toString();
			if(line.length()>0){
			String[] lnarr = line.split("::");
			try {
				context.write(new Text(lnarr[1]), new Text("C" + line));
			} catch (Exception e) {
			
			}
			}
		}

	}

	public static class MTable extends
			Mapper<LongWritable, Text, Text, Text> {
	

		public void map(LongWritable k, Text value, Context context)
				 {
			String line = value.toString();
			if(line.length()>0){
			String[] lnarr = line.split("::");
			
			if (lnarr.length == 3) {
				String genre1 = "Action", genre2 = "Drama";
				String inarr2 = lnarr[2].trim();
				if (inarr2.contains(genre1)
						|| inarr2.contains(genre2)) {
			
					
				try {
					context.write(new Text(lnarr[0]), new Text("D" + line));
				} catch (Exception e) {
					e.printStackTrace();
				}
				}
			}}
		}

	}


		public static class ReduceII extends Reducer<Text, Text, Text, Text> {

			private static ArrayList<Text> ltemp = new ArrayList<Text>();
			private static ArrayList<Text> mlist = new ArrayList<Text>();

			public void reduce(Text key, Iterable<Text> values, Context context)
					 {

				ltemp.clear();
				mlist.clear();
				
				for (Text tmp : values) {
									
					if (tmp.charAt(0) == 'C') {
						ltemp.add(new Text(tmp.toString().substring(1)));
					} else if (tmp.charAt(0) == 'D') {
						mlist.add(new Text(tmp.toString().substring(1)));
					}
				}

				if (!ltemp.isEmpty() && !mlist.isEmpty()) {
					for (Text A : ltemp) {
						for (Text B : mlist) {
							
							try {
								context.write(A, new Text("::"+B));
							} catch (Exception e) {
							
								e.printStackTrace();
							} 

						}
					}
				}

			}

		}
		
		
	
		
	public static class MapAvg extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable key, Text value, Context context)
				 {
			String line = value.toString();
			String[] arrval = line.split("::");
			
			//
			String MovieID = arrval[9];
			String Movie = arrval[10];
			String genre = arrval[11];
						
			String IDmivieGenre = MovieID + " \t " + Movie + " \t \t \t" + genre;
			
			
			Double val1 = Double.parseDouble(arrval[2]);
			
			try {
				context.write(new Text(IDmivieGenre), new DoubleWritable(val1));
			} catch ( Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static class ReduceAvg extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context)  {

			Text k = key;
			double fvalue = 0.0;
			int icount = 0;
			for (DoubleWritable val : values) {

				fvalue = fvalue + val.get();
				icount++;
			}

			double avg = fvalue / icount;
			DoubleWritable d = new DoubleWritable(avg);

			if (avg >= 4.4 && avg <= 4.7) {
				try {
					context.write(k, d);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "firstJoin");

		MultipleInputs.addInputPath(job1, new Path(args[0]),
				TextInputFormat.class, UserT.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]),
				TextInputFormat.class, TRating.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setJarByClass(ReduceSideJoin.class);
		job1.setReducerClass(Reduce.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		int T1 = job1.waitForCompletion(true) ? 0 : 1;

		if (T1 == 0) {
			Job job2 = new Job(conf, "SecondJoin");

			job2.setJarByClass(ReduceSideJoin.class);
			job2.setReducerClass(ReduceII.class);

			FileOutputFormat.setOutputPath(job2, new Path(args[4]));

			MultipleInputs.addInputPath(job2, new Path(args[2]),
					TextInputFormat.class, Temp.class);
			MultipleInputs.addInputPath(job2, new Path(args[3]),
					TextInputFormat.class, MTable.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			int T2 = job2.waitForCompletion(true) ? 0 : 1;

			if (T2 == 0) {
				Job job3 = new Job(conf, "j3");

				job3.setJarByClass(ReduceSideJoin.class);

				job3.setReducerClass(ReduceAvg.class);
				job3.setMapperClass(MapAvg.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(DoubleWritable.class);

				FileOutputFormat.setOutputPath(job3, new Path(args[5]));
				FileInputFormat.addInputPath(job3, new Path(args[4]));

				int T3 = job3.waitForCompletion(true) ? 0 : 1;

				System.exit(T3);
			}
		}
	}

}
