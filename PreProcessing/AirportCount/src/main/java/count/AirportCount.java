package count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class AirportCount extends Configured implements Tool {
    public static final Logger logger = LogManager.getLogger(count.AirportCount.class);

    public static class AirportCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Schema : "YEAR","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","OP_UNIQUE_CARRIER","ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID","CRS_DEP_TIME","DEP_DELAY_NEW","CRS_ARR_TIME","ARR_DELAY_NEW","CRS_ELAPSED_TIME","DISTANCE",
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String dataStr = value.toString();
            if (dataStr.contains("YEAR"))
                return ;
            if (dataStr.contains(",,"))
                return ;
            String[] fieldVals = dataStr.split(",");
            String srcAirport = fieldVals[5];
            String destAirport = fieldVals[6];

            context.write(new Text(srcAirport), new IntWritable(1));
            context.write(new Text(destAirport), new IntWritable(1));
        }
    }

    public static class AirportCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(final Text airportKey, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int c = 0;
            for (IntWritable i : values) {
                c += i.get();
            }
            context.write(airportKey, new IntWritable(c));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Carrier Count");
        job.setJarByClass(count.AirportCount.class);
        final Configuration jobConf =   job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
        // ================
        job.setMapperClass(AirportCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(AirportCountReducer.class);
        job.setReducerClass(AirportCountReducer.class);
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
            ToolRunner.run(new count.AirportCount(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
