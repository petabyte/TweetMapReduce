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

public class TweetMapReduce2Test {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and
     * a mapper and a reducer working together.
     */
    MapDriver<Text, Text, IntWritable, Text> mapDriver2;
    ReduceDriver<IntWritable, Text, IntWritable, Text > reduceDriver2;
    MapReduceDriver<Text, Text, IntWritable, Text, IntWritable, Text > mapReduceDriver2;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        TweetMapper2 mapper = new TweetMapper2();
        mapDriver2 = new MapDriver<Text, Text, IntWritable, Text>();
        mapDriver2.setMapper(mapper);


    /*
     * Set up the reducer test harness.
     */
        TweetReduce2 reducer = new TweetReduce2();
        reduceDriver2 = new ReduceDriver<IntWritable, Text, IntWritable, Text >();
        reduceDriver2.setReducer(reducer);
    
    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver2 = new MapReduceDriver<Text, Text, IntWritable, Text, IntWritable, Text >();
        mapReduceDriver2.setMapper(mapper);
        mapReduceDriver2.setReducer(reducer);
        mapReduceDriver2.setKeyComparator(new IntComparator());
    }

    /*
     * Test the mappers.
     *
     */
    @Test
    public void testMapper() throws IOException {
        mapDriver2.withInput(new Text("cat"), new Text("12"))
        .withInput(new Text("dog"), new Text("10"))
        .withInput(new Text("squirrel"), new Text("11"));

        mapDriver2.withOutput(new IntWritable(12), new Text("cat"))
        .withOutput(new IntWritable(10), new Text("dog"))
        .withOutput(new IntWritable(11), new Text("squirrel"));

        mapDriver2.runTest();
    }


    /*
     * Test the reducer.
     */
    @Test
    public void testReducer() throws IOException {

        List<Text> values = new ArrayList<Text>();
        values.add(new Text("dog"));
        reduceDriver2.withInput(new IntWritable(10), values);
        reduceDriver2.withOutput(new IntWritable(10),new Text("dog"));
        reduceDriver2.runTest();

    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver2.withInput(new Text("cat"), new Text("12"))
                .withInput(new Text("dog"), new Text("10"))
                .withInput(new Text("squirrel"), new Text("11"));
        mapReduceDriver2.withOutput(new IntWritable(10), new Text("dog"))
                .withOutput(new IntWritable(11), new Text("squirrel"))
                .withOutput(new IntWritable(12), new Text("cat"));
        mapReduceDriver2.withKeyOrderComparator(new IntComparator());
        mapReduceDriver2.runTest();

    }
}
