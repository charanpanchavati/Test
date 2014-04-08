import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.NullWritable;

public class Q1 {
	
	
public static class MapperTop10 extends
Mapper<LongWritable, Text, NullWritable, Text> {

ValueComparator compare = new ValueComparator();
private SortedMap<String, String> sortingresult = new TreeMap<String, String>(
		compare);
@Override
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String[] container = value.toString().split("\t");
	String movieRating = container[0].trim() + "::"
			+ container[1].trim();
	sortingresult.put(movieRating, movieRating);
	if (sortingresult.size() > 10) {
		sortingresult.remove(sortingresult.firstKey());
	}
}

@Override
protected void cleanup(Context context) throws IOException,
		InterruptedException {
	for (Map.Entry<String, String> entry : sortingresult.entrySet()) {
		String rating = entry.getValue();
		context.write(NullWritable.get(), new Text(rating));
	}
}
}

public static class ReducerTop10 extends
	Reducer<NullWritable, Text, NullWritable, Text> {
int count = 0;

public void reduce(NullWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
	for (Text a : values) {
		if (count < 10) {
			count++;
			context.write(NullWritable.get(), a);
		}
	}
}
}





	public static class AvgMap extends Mapper<LongWritable, Text, Text, Text> {
		private static final String f = "movies.dat";
		
		private HashMap<String, String> movInformation = new HashMap<String, String>();

		public void setup(Context context) throws IOException,
				FileNotFoundException, InterruptedException {
			Path[] allFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String file = new Path(f).getName();
			if (null != allFiles && allFiles.length > 0) {
				for (Path pathOfFiles : allFiles) {
					if (pathOfFiles.getName().equals(file)) {
						BufferedReader bReader = new BufferedReader(new FileReader(pathOfFiles.toString()));
						String locFilel = "";
			        	while ((locFilel = bReader.readLine()) != null) {
						int getIn = 0;
						Text word = new Text();
						
							String[] split = locFilel.trim().split("::");
							
							
							if (split.length == 3 && split != null ) {
								StringTokenizer tokenizer = new StringTokenizer(split[2], "|");
								while (tokenizer.hasMoreTokens()) {
									word.set(tokenizer.nextToken());
									String a = word.toString().trim();
									if (a.equalsIgnoreCase("Action")) {
										getIn = 1;
									}
								}
							}
							if (getIn == 1) {
								movInformation.put(split[0], split[1]+ "::" + split[2]);
							}
						}
					}
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fromRatings = value.toString().split("::");
			if (fromRatings.length >= 3) {
				if (movInformation.containsKey(fromRatings[1])) {
					context.write(new Text(fromRatings[1]),
							new Text(movInformation.get(fromRatings[1]) + "::"
									+ fromRatings[2]));
				}
			}
		}
	}

	public static class AvgReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Double rating = 0.0;
			int count = 0;
			String[] readLine = null;
			for (Text el : values) {
				Text word = new Text(el.toString());
				readLine = word.toString().split("::");
				rating += Double.parseDouble(readLine[2]);
				count++;
			}
			Double avgR = rating / count;
			String val = readLine[0] + "::" + readLine[1] + "::"+ avgR.toString();
			String laterKey = key.toString() + "::" + avgR.toString();
			context.write(new Text(laterKey), new Text(val));
		}
	}

	public static class ValueComparator implements Comparator<String> {

		@Override
		public int compare(String arg0, String arg1) {
			Float cmp1 = Float.parseFloat(arg0.split("::")[1]);
			Float cmp2 = Float.parseFloat(arg1.split("::")[1]);
			if (cmp1 >= cmp2)
				return -1;
			else
				return 1;
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path1 = new Path(args[0]);
		Path path2 = new Path(args[1]);
		
		if(!fs.exists(path1)){
			System.out.println(args[0] + " doesn't exist. Try again");
			System.exit(0);
		}
		if(!fs.exists(path2)){
			System.out.println(args[1] + " doesn't exist. Try again");
			System.exit(0);
		}
		
		//file exist check
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf); // movies
		Job job = new Job(conf, "MapAv");
		job.setJarByClass(Q1.class);
		job.setMapperClass(AvgMap.class);
		job.setReducerClass(AvgReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1])); // ratings
		FileOutputFormat.setOutputPath(job, new Path(args[2])); // output1
		if (job.waitForCompletion(true)) {
			Job job2 = new Job(conf, "maptop10");
			job2.setJarByClass(Q1.class);
			job2.setMapperClass(MapperTop10.class);
			job2.setReducerClass(ReducerTop10.class);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job2, new Path(args[2])); // output1
			FileOutputFormat.setOutputPath(job2, new Path(args[3])); // final
			job2.waitForCompletion(true);

		}
	}
}