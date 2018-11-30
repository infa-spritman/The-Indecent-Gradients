package projection;

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

public class CategoricalProjection extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(projection.CategoricalProjection.class);

    public static class ProjectionMapper extends Mapper<Object, Text, NullWritable, FlightWritable> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String dataStr = value.toString();
            String[] fieldVals = dataStr.split(",");
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

            FlightWritable projected = new FlightWritable(month,
                    dayOfMonth,
                    dayOfWeek,
                    carrier,
                    originAirport,
                    destAirport,
                    departTime,
                    departDelay,
                    arriveTime,
                    arriveDelay);
            context.write(NullWritable.get(), projected);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Data Cleanup");
        job.setJarByClass(projection.CategoricalProjection.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        // ================
        job.setMapperClass(ProjectionMapper.class);
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
            ToolRunner.run(new projection.CategoricalProjection(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
