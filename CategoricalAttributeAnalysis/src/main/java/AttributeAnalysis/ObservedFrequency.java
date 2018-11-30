package AttributeAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class ObservedFrequency  extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ObservedFrequency.class);

    public static class Mapper1 extends Mapper<Object, Text, IntTextPair, IntWritable> {
        private IntWritable ansVal = new IntWritable();

        @Override
        public void map(final Object key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            String link[] = value.toString().split(",");
            IntTextPair ansKey = new IntTextPair();
            ansVal.set(Integer.parseInt(link[link.length-1]));
            for(int i = 0; i < link.length-1; i++){
                ansKey.set(i, link[i]);
                context.write(ansKey, ansVal);
            }
        }
    }

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
