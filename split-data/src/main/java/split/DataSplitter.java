package split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DataSplitter extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(split.DataSplitter.class);
    public enum splitCounters {
        numTrain,
        numTest
    }

    public static class OddEvenMapper extends Mapper<Object, Text, Text, NullWritable> {
        private MultipleOutputs mos = null;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            double rand = Math.random();
            if (rand > 0.3) {
                mos.write("train", value, NullWritable.get());
                context.getCounter(splitCounters.numTrain).increment(1);
            }
            else {
                mos.write("test", value, NullWritable.get());
                context.getCounter(splitCounters.numTest).increment(1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }



    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Data Slitter");
        job.setJarByClass(split.DataSplitter.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
        // ================
        job.setMapperClass(OddEvenMapper.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "train", TextOutputFormat.class, IntWritable.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class, IntWritable.class, NullWritable.class);

        boolean result = job.waitForCompletion(true);
        Counters counters = job.getCounters();
        long numTrainRecords = counters.findCounter(splitCounters.numTrain).getValue();
        long numTestRecords = counters.findCounter(splitCounters.numTest).getValue();
        logger.info("Number of train records: " + numTrainRecords);
        logger.info("Number of test records: " + numTestRecords);
        return result ? 0 : 1;
    }



    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new split.DataSplitter(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
