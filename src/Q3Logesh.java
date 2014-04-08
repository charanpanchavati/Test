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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q3Logesh {

	public static class Map1 extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text valOfLine, Context context)
				throws IOException, InterruptedException {
			String sentence = valOfLine.toString();
			
			String[] userWord = sentence.split("::");
			for(int i = 0 ; i < userWord.length ; i++){
				//String[] user = sentence.split("::");
			}
			
			context.write(new Text(userWord[4]),
					new DoubleWritable(Integer.parseInt(userWord[2])));

		}
	}

	// Reducer to find the average
	public static class Reduce1 extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			float count = 0;
			float sum = 0;
			
			float average = 0;
			
			
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			
			
			
			average  =  sum/count;
			context.write(key, new DoubleWritable(average));
		}
	}

	
	

//Map2 to find the top 10 
	public static class Map2 extends Mapper<LongWritable, Text, NullWritable, Text> {

		comp compare=new comp();
		private SortedMap<String,String> res = new TreeMap<String,String>(compare);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] valueArr = value.toString().split("\t");
			String movieRating = valueArr[0].trim()+"="+valueArr[1].trim();
			res.put(movieRating,movieRating);
			
			for (int i =0; i< valueArr.length;i++){
				String [] s = null;
			}

		}
		
	
		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			for(Map.Entry<String,String> entry : res.entrySet()){
				String k = "null";
				try{
				String arrayOfEntries = entry.getValue();
				context.write(NullWritable.get(),new Text(arrayOfEntries));
				
				}
				catch (Exception e){
					
				}}
			}
		
		
		public static class comp implements Comparator<String>{

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
		job1.setJarByClass(Q3Logesh.class);
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
		
			job2.setJarByClass(Q3Logesh.class);
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