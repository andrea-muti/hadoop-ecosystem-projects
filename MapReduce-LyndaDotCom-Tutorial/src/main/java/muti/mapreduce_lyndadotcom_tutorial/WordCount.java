package muti.mapreduce_lyndadotcom_tutorial;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
// mapred are form MapReduce 1
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
// mapreduce are from MapReduce 2
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

@SuppressWarnings("deprecation")
public class WordCount {


	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		static enum Counters { INPUT_WORDS }
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();
		
		private long numRecords = 0;
		private String inputFile;
		
		
		// using a distributed cache
		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
			inputFile = job.get("map.input.file");
			
			if (job.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try{
					patternsFiles = DistributedCache.getLocalCacheFiles(job);
				}
				catch (IOException e) {
					System.err.println("Caught exception while getting cached files: "+StringUtils.stringifyException(e));
				}
				
				for (Path pattersFile : patternsFiles){
					parseSkipFile(pattersFile);
				}
			}
		}
		
		private void parseSkipFile(Path pattersFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(pattersFile.toString()));
				String pattern = null;
				
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
				
				fis.close();
			}catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '"+pattersFile+"' : "+StringUtils.stringifyException(ioe));
			}
		}
		
		/**
		 * mapper (filename, file-contents):
		 * 	for each word in file-contents
		 * 		emit (word,1)
		 */
		public void map(@SuppressWarnings("unused") LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException, InterruptedException{
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			
			for (String pattern : patternsToSkip) {
				line = line.replaceAll(pattern, "");
			}
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				output.collect(word, one);
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
			}
			
			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records from the input file: "+inputFile);
			}
			
		}
	}
	
	
	/**
	 * reducer (word, values):
	 * 	sum = 0
	 * 	for each value in values:
	 * 		sum = sum + value
	 * 	emit (word, sum)
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, @SuppressWarnings("unused") Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		// create configuration
		Configuration conf = new Configuration();
		
		List<String> other_args = new ArrayList<String>();
		for (int i=0; i < args.length; i++){
			if (args[i].equals("-skip")) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
				conf.setBoolean("wordcount.skip.patterns", true);
			}
			else {
				other_args.add(args[i]);
			}
		}
		
		// create JobRunnerInstance
		Job job = new Job(conf, "wordcount");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// call MapInstance on JobInstance
		job.setMapperClass(Map.class);
		
		// call ReduceInstance on JobInstance
		job.setReducerClass(Reduce.class);
		
		
		String pathInput = "";
		String pathOutput = "";
		FileInputFormat.addInputPath(job, new Path(pathInput));
		FileOutputFormat.setOutputPath(job, new Path(pathOutput));
		
		
		job.waitForCompletion(true);
		
	}	

}
