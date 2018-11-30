package normalize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

public class Normalizer extends Configured implements Tool {
    public static final Logger logger = LogManager.getLogger(normalize.Normalizer.class);

    public static class NormalizerMapper extends Mapper<Object, Text, NullWritable, FlightWritable> {
        private float min;
        private float max;

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String dataStr = value.toString();
            String[] fieldVals = dataStr.split(",");

            String year = fieldVals[0];
            String month = fieldVals[1];
            String dayOfMonth = fieldVals[2];
            String dayOfWeek = fieldVals[3];
            String carrier = fieldVals[4];
            String originAirport = fieldVals[5];
            String destAirport = fieldVals[6];
            String departTime = fieldVals[7];
            String departDelay = fieldVals[8];
            String arriveTime = fieldVals[9];
            String arriveDelay = fieldVals[10];
            String schecduledFlightTime = fieldVals[11];
            String distance = fieldVals[12];

            float dist = Float.parseFloat(distance);
            dist = (dist - min)/(max - min);
            distance = dist + "";

            FlightWritable normalized = new FlightWritable(year,
                    month,
                    dayOfMonth,
                    dayOfWeek,
                    carrier,
                    originAirport,
                    destAirport,
                    departTime,
                    departDelay,
                    arriveTime,
                    arriveDelay,
                    schecduledFlightTime,
                    distance);

            context.write(NullWritable.get(), normalized);
        }

        @Override
        public void setup(final Context context) {
            Configuration conf = context.getConfiguration();
            String maxParam = conf.get("MAX");
            String minParam = conf.get("MIN");
            max = Float.parseFloat(maxParam);
            min = Float.parseFloat(minParam);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("MAX", "4983.00");
        conf.set("MIN", "28.0");
        final Job job = Job.getInstance(conf, "Distance Normalization");
        job.setJarByClass(normalize.Normalizer.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        // ================
        job.setMapperClass(NormalizerMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FlightWritable.class);
        job.setNumReduceTasks(0);

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
            ToolRunner.run(new normalize.Normalizer(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
