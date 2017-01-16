package muti.map_reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {

	
	public static void main(String[] args) throws Exception{

		BasicConfigurator.configure(); 
		Logger.getRootLogger().setLevel(Level.INFO);
		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");

		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

		//		args = new String[2];
		//	
		//		// paths sull'hdfs
		//		args[0] = "/user/Andrea/resources/input.txt";
		//		args[1] = "/user/Andrea/resources/output";

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
		

		// ---------


		JobConf jconf = new JobConf(conf);
		jconf.setJobName("MyWordCountJob"); 
		
		jconf.setJar("target/map-reduce-0.0.1-SNAPSHOT.jar");
		
		jconf.setOutputKeyClass(Text.class);
		jconf.setOutputValueClass(IntWritable.class); 
		
		jconf.setMapperClass(MapperClass.class); 
		jconf.setCombinerClass(ReducerClass.class); 
		jconf.setReducerClass(ReducerClass.class); 
		jconf.setJarByClass(WordCount.class);
		
		jconf.setInputFormat(TextInputFormat.class); 
		jconf.setOutputFormat(TextOutputFormat.class); 

		FileInputFormat.setInputPaths(jconf, new Path(args[0])); 
		FileSystem.get(conf).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(jconf, new Path(args[1])); 

		RunningJob j = JobClient.runJob(jconf); 
		
		//--------

		System.out.println("sumbitting job");

		if(j.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!j.isSuccessful()) {
			System.out.println("Job was not successful");
		}

		return 0;
	}


}