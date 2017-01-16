package muti.map_reduce_stop_words_filter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map Class which extends MaReduce.Mapper class
 * Map is passed a single line at a time, it splits the line based on space
 * and generated the token which are output by map with value as one to be consumed
 * by reduce class
 * @author Raman
 */
public class MapperStopWordsFilterClass extends Mapper<LongWritable, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Set<String> stopWords = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		try{
			URI[] stopWordsFiles = Job.getInstance(conf).getCacheFiles();
			
			if ( stopWordsFiles.length == 0 ) {
				throw new FileNotFoundException("Distributed cache file not found.");
			}

			if(stopWordsFiles != null && stopWordsFiles.length > 0) {
				for(URI stopWordFile : stopWordsFiles) {
					readFile(stopWordFile);
				}
			}
		} catch(IOException ex) {
			System.err.println("Exception in mapper setup: " + ex.getMessage());
		}
	}

	/**
	 * map function of Mapper parent class takes a line of text at a time
	 * splits to tokens and passes to the context as word along with value as one
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line,".,; ");

		while(st.hasMoreTokens()){
			String wordText = st.nextToken();

			if(!stopWords.contains(wordText.toLowerCase())) {	
				word.set(wordText);
				context.write(word,one);
			}
		}

	}

	private void readFile(URI stopWordURI) {
		 Path stopWordPath = new Path(stopWordURI.getPath());
         String stopWordFileName = stopWordPath.getName().toString();
		try{	
			BufferedReader bufferedReader = new BufferedReader(new FileReader(stopWordFileName));
			String stopWord = null;
			while ((stopWord = bufferedReader.readLine()) != null) {
				stopWords.add(stopWord.toLowerCase());
			}
			bufferedReader.close();
		} catch(IOException ex) {
			System.err.println("Exception while reading stop words file: " + ex.getMessage());
		}
	}
}