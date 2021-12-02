package hadooptest;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
 
public class Ngrams{
	
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
	     public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
	          String words[] = value.toString().toLowerCase().replaceAll("[^A-Za-z0-9\\s]", "").trim().split("\\s+");
	          // since every value is going to be set to 1, it's more efficient to create a single int writable here than in every context.write
	          IntWritable one = new IntWritable(1);
	          for (int i=0; i<words.length-3; i++) {
	        	  String ngramToAdd = words[i] + " " + words[i+1] + " " + words[i+2] + " " + words[i+3];
	        	  value.set(ngramToAdd);
	        	  context.write(value, one);
	          }
	     }
	}

	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
	     public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
	    	 IntWritable result = new IntWritable();
	    	 int sum = 0;
	         for(IntWritable x: values) {
	        	 sum+=x.get();
	         }
	         result.set(sum);	         
	         context.write(key, result);
	     }
	           
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        Job job = new Job(conf,"Ngrams");
        //job.setNumReduceTasks(2);
        job.setJarByClass(Ngrams.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        outputPath.getFileSystem(conf).delete(outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
}
