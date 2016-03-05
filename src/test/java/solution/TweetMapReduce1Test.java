package solution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TweetMapReduce1Test {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and
     * a mapper and a reducer working together.
     */
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver1;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver1;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver1;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        TweetMapper1 mapper = new TweetMapper1();
        mapDriver1 = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver1.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
        TweetReduce1 reducer = new TweetReduce1();
        reduceDriver1 = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver1.setReducer(reducer);
    
    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver1 = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver1.setMapper(mapper);
        mapReduceDriver1.setReducer(reducer);
    }

    /*
     * Test the mappers.
     *
     */
    @Test
    public void testMapper() throws IOException {
        mapDriver1.withInput(new LongWritable(1), new Text("ScreenName\tRT @DarrenDalrymple: dog “dog cat cat\ten\tThu Sep 26 08:50:21 CDT 2013\tnull"))
                .withInput(new LongWritable(1), new Text("ScreenName\tRT @DarrenDalrymple: #dog dog cat cat\ten\tThu Sep 26 08:50:21 CDT 2013\tnull"))
                .withInput(new LongWritable(1), new Text("Did you check for clean data?"));
        mapDriver1.withOutput(new Text("dog"), new IntWritable(1));
        mapDriver1.withOutput(new Text("dog"), new IntWritable(1));
        mapDriver1.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver1.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver1.withOutput(new Text("dog"), new IntWritable(1));
        mapDriver1.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver1.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver1.runTest();
    }


    /*
     * Test the reducer.
     */
    @Test
    public void testReducer() throws IOException {

        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver1.withInput(new Text("dog"), values);
        reduceDriver1.withOutput(new Text("dog"), new IntWritable(4));
        reduceDriver1.runTest();

    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce() throws IOException {

        mapReduceDriver1.withInput(new LongWritable(1), new Text("ScreenName\tRT @DarrenDalrymple: dog dog cat cat\ten\tThu Sep 26 08:50:21 CDT 2013\tnull"))
                .withInput(new LongWritable(1), new Text("ScreenName\tRT @DarrenDalrymple: #dog dog cat cat\ten\tThu Sep 26 08:50:21 CDT 2013\tnull"))
                .withInput(new LongWritable(1), new Text("Did you check for clean data?"));
        mapReduceDriver1.withOutput(new Text("cat"), new IntWritable(4));
        mapReduceDriver1.withOutput(new Text("dog"), new IntWritable(3));
        mapReduceDriver1.runTest();

    }
}
