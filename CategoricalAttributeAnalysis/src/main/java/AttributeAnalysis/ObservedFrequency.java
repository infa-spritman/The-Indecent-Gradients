package AttributeAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class ObservedFrequency  extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ObservedFrequency.class);

    @Override
    public int run(final String[] args) throws Exception {

        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Categorical Attributes Frequencies With Delay");
        job.setJarByClass(ObservedFrequency.class);
        final Configuration jobConf = job.getConfiguration();
        // Sets output delimeter for each line
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        // FileInputFormat takes up TextInputFormat as default
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new ObservedFrequency(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
