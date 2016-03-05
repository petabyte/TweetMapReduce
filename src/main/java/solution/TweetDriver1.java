package solution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by George on 2/27/2016.
 */
public class TweetDriver1 extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: TweetDriver1 <input dir> <output dir>\n");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(TweetDriver1.class);
        job.setJobName("Tweet Job 1");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TweetMapper1.class);
        job.setReducerClass(TweetReduce1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /*
     * The main method calls the ToolRunner.run method, which
     * calls an options parser that interprets Hadoop command-line
     * options and puts them into a Configuration object.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new TweetDriver1(), args);
        System.exit(exitCode);
    }
}
