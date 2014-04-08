import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Q33 {

	public static class Map extends
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

	// Zip code, <<Age>>
	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			int sum = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			context.write(key, new DoubleWritable(sum / count));
		}
	}

	public static class Map1 extends Mapper<Object, Text, NullWritable, Text> {

		private TreeMap<Double, Text> visitorToRecordMap = new TreeMap<Double, Text>();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] container = value.toString().split("\t");

			for (int i = 0; i < container.length; i++) {
				String add = container[i].trim();
			}

			if (container.length > 0) {
				String containerValue = container[1].trim() + "\t"
						+ container[0].trim();

				visitorToRecordMap.put(Double.parseDouble(container[1].trim()),
						new Text(containerValue));

				if (visitorToRecordMap.size() > 10) {
					visitorToRecordMap.remove(visitorToRecordMap.firstKey());
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			for (Text t : visitorToRecordMap.descendingMap().values()) {

				context.write(NullWritable.get(), t);

			}
		}
	}

	public static class Reduce1 extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Double, Text> visitorToRecordMap = new TreeMap<Double, Text>();

		public void reduce(NullWritable key, Iterable<Text> values,
				Reducer.Context context) throws IOException,
				InterruptedException {

			for (Text val : values) {

				String value = val.toString();
				String[] container = value.split("\t");

				if (container.length > 0) {
					String count = container[0].trim();
					visitorToRecordMap.put(Double.parseDouble(count), new Text(
							value));

					if (visitorToRecordMap.size() > 10) {
						visitorToRecordMap
								.remove(visitorToRecordMap.firstKey());
					}
				}
			}

			for (Text t : visitorToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(Q33.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (job.waitForCompletion(true)) {

			Job job1 = new Job(conf, "wordcount");

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			job1.setJarByClass(Q33.class);
			job1.setMapperClass(Map1.class);
			job1.setCombinerClass(Reduce1.class);
			job1.setReducerClass(Reduce1.class);

			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job1, new Path(args[1]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));

			System.exit(job1.waitForCompletion(true) ? 0 : 1);
		}
	}
}