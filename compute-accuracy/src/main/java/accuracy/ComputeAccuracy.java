package accuracy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ComputeAccuracy extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ComputeAccuracy.class);

    public enum accuracyCounters {
        correct,
        total
    }

    public static class AccuracyMapper extends Mapper<Object, Text, Text, NullWritable> {
        private int delta = 0;

        @Override
        protected void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            delta = configuration.getInt("delta", 0);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String dataStr = value.toString();
            String[] fieldVals = dataStr.split(",");
            int actualDelayGroup = Integer.parseInt(fieldVals[8]);
            int predictedDelayGroup = Integer.parseInt(fieldVals[13]);
            if (predictedDelayGroup >= actualDelayGroup - delta && predictedDelayGroup <= actualDelayGroup + delta) {
                context.getCounter(accuracyCounters.correct).increment(1);
            }
            context.getCounter(accuracyCounters.total).increment(1);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Data Slitter");
        job.setJarByClass(accuracy.ComputeAccuracy.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        jobConf.setInt("delta", Integer.parseInt(args[2]));
        // Delete output directory, only to ease local development; will not work on AWS. ===========
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        // ================
        job.setMapperClass(AccuracyMapper.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        Counters counters = job.getCounters();
        long correctPredictions = counters.findCounter(accuracyCounters.correct).getValue();
        long totalPredictions = counters.findCounter(accuracyCounters.total).getValue();
        logger.info("Number of correct predictions = " + correctPredictions);
        logger.info("Number of total predictions = " + totalPredictions);
        double accuracy = ((double)correctPredictions/(double)totalPredictions) * 100;
        logger.info("Accuracy = " + accuracy);
        return result ? 0 : 1;
    }



    public static void main(final String[] args) {
        // Checking whether 3 arguments are passed or not
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<predicted-data-dir> <output-dir> delta(allowed error in prediction)");
        }

        try {
            // Running implementation class
            ToolRunner.run(new ComputeAccuracy(), args);
        } catch (final Exception e) {
            logger.error("MR Job Exception", e);
        }
    }
}
