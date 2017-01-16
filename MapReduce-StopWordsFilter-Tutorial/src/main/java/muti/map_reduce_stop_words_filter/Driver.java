package muti.map_reduce_stop_words_filter;

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

/**
 * The entry point for the WordCount example,
 * which setup the Hadoop job with Map and Reduce Class
 * 
 * @author Raman
 */
public class Driver extends Configured implements Tool{
	
	/**
	 * Main function which calls the run method and passes the args using ToolRunner
	 * @param args Two arguments input and output file paths
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		
		BasicConfigurator.configure(); 
		Logger.getRootLogger().setLevel(Level.INFO);
		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");
		
		int exitCode = ToolRunner.run(new Driver(), args);
		System.exit(exitCode);
	}
 
	/**
	 * Run method which schedules the Hadoop Job
	 * @param args Arguments passed in main function
	 */
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.177.101:9000");
		conf.set("yarn.resourcemanager.hostname", "192.168.177.101");
		conf.set("yarn.resourcemanager.address", "192.168.177.101:8050");
		conf.set("yarn.resourcemanager.scheduler.address", "192.168.177.101:8030");
		conf.set("mapreduce.jobtracker.address", "192.168.177.101:8021");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapreduce.jobhistory.address", "192.168.177.101:10020");
		conf.set("mapred.remote.os", "Linux");
	
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
	    String[] remainingArgs = optionParser.getRemainingArgs();
		
		if (remainingArgs.length != 3) {
			System.err.printf("Usage: %s needs three arguments, input and output stop-words files\n", getClass().getSimpleName());
			return -1;
		}
	
		//Initialize the Hadoop job and set the jar as well as the name of the Job
		Job job = Job.getInstance(conf);
		job.setJar("target/map-reduce-stop-words-filter-0.0.1-SNAPSHOT.jar");
		job.setJarByClass(Driver.class);
		job.setJobName("Word Counter With Stop Words Removal");
		
		//Add input and output file paths to job based on the arguments passed
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the MapClass and ReduceClass in the job
		job.setMapperClass(MapperStopWordsFilterClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.addCacheFile(new Path(args[2]).toUri());

	
		//Wait for the job to complete and print if the job was successful or not
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}
		
		return returnValue;
	}
}
