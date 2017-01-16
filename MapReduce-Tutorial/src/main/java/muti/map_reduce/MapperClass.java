package muti.map_reduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * MapperClass
 * 
 * @author Andrea Muti
 * created: 29 dic 2016
 *
 */
public class MapperClass extends MapReduceBase implements Mapper<
	LongWritable ,	/* Input key Type */ 
	Text,           /* Input value Type */ 
	Text,           /* Output key Type */ 
	IntWritable>    /* Output value Type */ 
	{ 
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line," ");

		while (st.hasMoreTokens()) {
			word.set(st.nextToken());
			output.collect(word, one); 
		}

	}
}