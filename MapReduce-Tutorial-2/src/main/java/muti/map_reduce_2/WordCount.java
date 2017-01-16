package muti.map_reduce_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class WordCount extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		
		BasicConfigurator.configure(); 
		Logger.getRootLogger().setLevel(Level.INFO);
		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");
		
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("yarn.resourcemanager.hostname", "192.168.177.101"); // see step 3
		conf.set("yarn.resourcemanager.address", "192.168.177.101:8050"); // see step 3
		conf.set("yarn.resourcemanager.scheduler.address", "192.168.177.101:8030");
		conf.set("mapreduce.jobtracker.address", "192.168.177.101:8021");
		conf.set("mapreduce.framework.name", "yarn"); 
		conf.set("fs.defaultFS", "hdfs://192.168.177.101:9000"); // see step 2

		conf.set("mapred.remote.os", "Linux");
		conf.set("mapreduce.app-submission.cross-platform", "true");

		conf.set("mapreduce.jobhistory.address", "192.168.177.101:10020");
	
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
	    String[] remainingArgs = optionParser.getRemainingArgs();
		
		if (remainingArgs.length != 2) {
			System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
					return -1;
		}

		Job job = Job.getInstance(conf);
		job.setJar("target/map_reduce_2-0.0.1-SNAPSHOT.jar");
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCounter");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		int returnValue = job.waitForCompletion(true) ? 0:1;

		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}

		return returnValue;
	}
}