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
        
public class Q3 {
	
	static String toString(String [] arr){
	    String returnString = "";
		for(int i=0; i <arr.length; i++){
		    returnString = returnString + arr[i] + " ";
	}
		return returnString;
		
	}
	
	
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        String testParam = "";     
  
	@Override    
	protected void setup(Context context) throws IOException, InterruptedException
	{ 
		Configuration conf = context.getConfiguration();   
		testParam = conf.get("parameter");  
		
	}
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] testParamArray = testParam.split(" ");
		String[] spl = line.split("::");
	    String [] splitsOfTheString = spl[2].split("\\|");
	    int counter = 0;
		for(int i=2; i<testParamArray.length; i++){
			for(String el : splitsOfTheString){
			    if(testParamArray[i].trim().equalsIgnoreCase(el.trim()) ){
				     counter++;
				     break;
				
			}
			}
			}
		 
		if(counter==testParamArray.length -2){	
			word.set(line);
			context.write(word, one);	
		}		
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int s = 0;
        for (IntWritable val : values) {
            s += val.get();
        }
        context.write(key, new IntWritable(s));
    }
 }
        
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();
	conf.set("parameter", toString(args)); 
	
	
		
    Job job = new Job(conf, "Q3");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJarByClass(Q3.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
