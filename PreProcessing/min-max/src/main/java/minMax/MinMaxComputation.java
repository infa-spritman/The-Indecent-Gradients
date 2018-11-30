package minMax;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MinMaxComputation extends Configured implements Tool {
    public static final Logger logger = LogManager.getLogger(minMax.MinMaxComputation.class);

    public static class MinMaxMapper extends Mapper<Object, Text, NullWritable, FloatWritable> {
        private float max = Float.MIN_VALUE;
        private float min = Float.MAX_VALUE;

        @Override
        public void map(final Object key, final Text value, final Context context) {
            String dataStr = value.toString();
            String[] fieldVals = dataStr.split(",");
            float distance = Float.parseFloat(fieldVals[12]);
            max = Float.max(distance, max);
            min = Float.min(distance, min);
        }

        @Override
        public void cleanup(final Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), new FloatWritable(max));
            context.write(NullWritable.get(), new FloatWritable(min));
        }
    }

    public static class MinMaxReducer extends Reducer<NullWritable, FloatWritable, Text, FloatWritable> {
        private float max = Float.MIN_VALUE;
        private float min = Float.MAX_VALUE;

        @Override
        public void reduce(final NullWritable key, final Iterable<FloatWritable> values, final Context context) throws IOException, InterruptedException {
            for (FloatWritable distance : values) {
                max = Float.max(max, distance.get());
                min = Float.min(min, distance.get());
            }
            context.write(new Text("MAX"), new FloatWritable(max));
            context.write(new Text("MIN"), new FloatWritable(min));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "min-max");
        job.setJarByClass(minMax.MinMaxComputation.class);
        final Configuration jobConf =   job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        // ================
        job.setMapperClass(MinMaxMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

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
            ToolRunner.run(new minMax.MinMaxComputation(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
