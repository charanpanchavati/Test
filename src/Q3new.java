import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q3new {


	// Map1 to split the Map
	public static class Map1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			String[] user = line.split("::");

			context.write(new Text(user[4]),
					new DoubleWritable(Integer.parseInt(user[2])));

			/*
			 * StringTokenizer tokenizer = new StringTokenizer(line); while
			 * (tokenizer.hasMoreTokens()) { word.set(tokenizer.nextToken());
			 * context.write(word, one); }
			 */
		}
	}

	// Reducer to find the average
	public static class Reduce1 extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			float count = 0;
			float sum = 0;
			float avg = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			avg  =  sum/count;
			context.write(key, new DoubleWritable(avg));
		}
	}

	public static class comp implements Comparator<String>{

		@Override
		public int compare(String arg0, String arg1) {
			Float F1 = Float.parseFloat(arg0.split("=")[1]);
			Float F2 =Float.parseFloat(arg1.split("=")[1]);
			int i=0;
			float f =0;
			//Compare
			if(F1<=F2)
				return -1;
			else  
				return 1;
		}
	}
	

//Map2 to find the top 10 
	public static class Map2 extends Mapper<LongWritable, Text, NullWritable, Text> {

		
		comp compare=new comp();
		private SortedMap<String,String> resultofSortedval = new TreeMap<String,String>(compare);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] container = value.toString().split("\t");
			String movieRating = container[0].trim()+"="+container[1].trim();
			resultofSortedval.put(movieRating,movieRating);
			
			for (int i =0; i< container.length;i++){
				String [] s = null;
			//	s[i]= container[0].trim();
			}

		}
		
		//Clean up method

		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			for(Map.Entry<String,String> entry : resultofSortedval.entrySet()){
				String k = "null";
				try{
				String rating_movie = entry.getValue();
				context.write(NullWritable.get(),new Text(rating_movie));
			/*	for (int i =0; i< rating_movie.length();i++){
					for (int j =0; j< rating_movie.length();j++){
					String  str = "aa";
					k=str;
					
				//	s[i]= container[0].trim();
				}
				}*/
				}
				catch (Exception e){
					System.out.println("Inside Cleanup"+e.toString());
				}}
			}
		}
//Reducer 2 to find the top 10
	public static class Reduce2 extends Reducer<NullWritable, Text, NullWritable, Text> {
		int cnt=0;
		 public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 
			 	for(Text T:values){
			 		if(cnt<10){
			 			cnt++;
			 			try{
			 			context.write(NullWritable.get(),T);
			 			}
			 			catch (Exception e){
			 				System.out.println("Inside Reducer2"+e.toString());
			 			}
			 		}
			 		else{
			 			System.out.println("Done with the Sorting");
			 		}
			 			
			 	}
		 	}
		}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "Map-Red1");
		job1.setJarByClass(Q3new.class);
		job1.setMapperClass(Map1.class);
		job1.setCombinerClass(Reduce1.class);
		job1.setReducerClass(Reduce1.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		if (job1.waitForCompletion(true))
		{
			Job job2 = new Job(conf, "Map-red2");
		
			job2.setJarByClass(Q3new.class);
			job2.setMapperClass(Map2.class);
			
			job2.setCombinerClass(Reduce2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}
}