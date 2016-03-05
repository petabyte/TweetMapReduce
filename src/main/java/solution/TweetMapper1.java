package solution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This is the TweetMapper1.
 */
public class TweetMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    static enum Tweet {
        NUM_TWEETS,
        BAD_RECORD,
        HASH_TAG,
        NOT_CONSIDERED_A_WORD
    };

    Text wordText = new Text();
    IntWritable countIntWritable = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        //Split the records tab delimited
        String[] recordSplit = line.split("\\t+");
        try {
            String tweetMessage = recordSplit[1];
            //Increment NUM_TWEETS - if it was not split properly an Exception will be thrown
            context.getCounter(Tweet.NUM_TWEETS).increment(1);
            //Loop thru the tweet split by any white space character
            for (String word : tweetMessage.split("\\s+")) {
                if (word.length() > 0) {
                    String wordLowerCase = word.toLowerCase();
                    //Don't count hashtags
                    if(wordLowerCase.matches("^#\\w+")){
                        context.getCounter(Tweet.HASH_TAG).increment(1);
                    //Match words that are 3 letters and above
                    //Make sure it is not an @mention too as well
                    }else if(wordLowerCase.matches("^[^#@]\\w{2,}")){
                        //make sure that the word just contains word characters - no qoutes
                        //double quotation
                        String cleanWord = wordLowerCase.replaceAll("[^\\w+]","");
                        wordText.set(cleanWord);
                        context.write(wordText, countIntWritable);
                    }else{
                        //If it does not match a hashtag or a what we consider a word
                        //increment this
                        context.getCounter(Tweet.NOT_CONSIDERED_A_WORD).increment(1);
                    }
                }
            }
        } catch (ArrayIndexOutOfBoundsException aie) {
            context.getCounter(Tweet.BAD_RECORD).increment(1);
            System.err.println("Ignoring corrupt input: " + value);
        }
    }
}