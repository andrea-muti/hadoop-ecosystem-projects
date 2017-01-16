package muti.map_reduce_log_processor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Map Class which extends MaReduce.Mapper class
 * Map is passed a single line at a time, it splits the line based on space
 * and calculates the number of page visits(each line contains the number corresponding to page number)
 * So total length of the split array are the no. of pages visited in that session
 * If pages are more then 500 then write the line to the context.
 * 
 * @author andrea-muti
 */
public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	 
    private Text selectedLine = new Text();
    private IntWritable noOfPageVisited = new IntWritable();
    
    /**
     * map function of Mapper parent class takes a line of text at a time
     * performs the operation and passes to the context as word along with value as one
     */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] pagesVisited = line.split(" ");
		
		if (pagesVisited.length > 500) {
			selectedLine.set(line);
			noOfPageVisited.set(pagesVisited.length);
			context.write(selectedLine, noOfPageVisited);
		}
	}
}