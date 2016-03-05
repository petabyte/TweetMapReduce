package solution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This is the TweetMapper2.
 */
public class TweetMapper2 extends Mapper<Text, Text, IntWritable, Text> {

    static enum Tweet {
        BAD_RECORD
    };

    IntWritable keyIntWritable = new IntWritable();
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            keyIntWritable.set(Integer.parseInt(value.toString()));
            context.write(keyIntWritable, key);
        }catch(NumberFormatException nfe){
            context.getCounter(Tweet.BAD_RECORD).increment(1);
        }
    }
}