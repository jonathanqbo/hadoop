package bq.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * Usage:
 * 
 * <pre>
1) To run this code:

BQMac:hadoop-2.7.2 qibo$ bin/hadoop jar ./input/wordcount/wordcount_v1.jar bq.hadoop.mapreduce.WordCountV1 /user/qibo/wordcount/v1/input /user/qibo/wordcount/v1/output

2) View the input:

BQMac:hadoop-2.7.2 qibo$ bin/hadoop fs -cat /user/qibo/wordcount/v1/input/*
16/06/02 01:54:35 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
hello
world
hello
hadoop
hello
qibo
Hello
World
Hello
Hadoop
Hello
Qibo
Hello,
World!
Hello,
Hadoop!
Hello,
Qibo!

3) View the result:

BQMac:hadoop-2.7.2 qibo$ bin/hadoop fs -cat /user/qibo/wordcount/v1/output/*
16/06/02 01:54:48 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Hadoop	1
Hadoop!	1
Hello	3
Hello,	3
Qibo	1
Qibo!	1
World	1
World!	1
hadoop	1
hello	3
qibo	1
world	1
BQMac:hadoop-2.7.2 qibo$  
 * </pre>
 * 
 * 
 * @author qibo
 *
 */

public class WordCountV1 {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable ONE = new IntWritable(1);
		
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				String token = tokenizer.nextToken();
				word.set(token);
				context.write(word, ONE);
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			
			result.set(sum);
			
			context.write(key, result);
		}

	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "word count v1");
		job.setJarByClass(WordCountV1.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}
	
}
