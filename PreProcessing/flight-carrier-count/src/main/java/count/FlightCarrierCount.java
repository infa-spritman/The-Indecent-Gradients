package count;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class FlightCarrierCount extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(count.FlightCarrierCount.class);

    public static class TupleMapper extends Mapper<Object, Text, FlightCarrierKey, IntWritable> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String dataStr = value.toString();
            String[] fieldVals = dataStr.split(",");
            int distance = (int) Float.parseFloat(fieldVals[12]);
            int carrier = Integer.parseInt(fieldVals[4]);
            int time = Integer.parseInt(fieldVals[7]);

            FlightCarrierKey k = new FlightCarrierKey(distance, carrier, time);

            context.write(k, new IntWritable(1));
        }
    }

    public static class IntSumReducer extends Reducer<FlightCarrierKey, IntWritable, FlightCarrierKey, IntWritable> {
        @Override
        public void reduce(final FlightCarrierKey key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int c = 0;
            for (IntWritable i : values) {
                c += i.get();
            }
            context.write(key, new IntWritable(c));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Flight carrier count");
        job.setJarByClass(count.FlightCarrierCount.class);
        final Configuration jobConf =   job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
        // ================
        job.setMapperClass(TupleMapper.class);
        job.setMapOutputKeyClass(FlightCarrierKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new FlightCarrierCount(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
