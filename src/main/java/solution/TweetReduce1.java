package solution;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the TweetReduce1 class
 */ 
public class TweetReduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable wordCountIntWritable = new IntWritable();
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  int wordCount = 0;
	  for (IntWritable value : values) {
		  wordCount += value.get();
	  }
	  wordCountIntWritable.set(wordCount);
	  context.write(key, wordCountIntWritable);
  }
}