package AttributeAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CategoricalCorrelation extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(CategoricalCorrelation.class);

	public static class CommaTokenizingMapper extends Mapper<Object, Text, IntTextPair, BooleanWritable> {

        @Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String link[] = value.toString().split(",");
            if(link[link.length-1].length() != 0){
                IntTextPair ansKey = new IntTextPair();
                BooleanWritable ansValue = new BooleanWritable(Integer.parseInt(link[link.length-1]) == 0);
                for(int i = 0; i < link.length-1; i++){
                    ansKey.set(i, link[i]);
                    context.write(ansKey, ansValue);
                }
            }
		}
	}

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntTextPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntTextPair ip1 = (IntTextPair) w1;
            IntTextPair ip2 = (IntTextPair) w2;
            int cmp = ip1.getFirst().compareTo(ip2.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            return ip1.getSecond().compareTo(ip2.getSecond());
        }
    }

	/*
    Reduces a set of values of a single key
    to a single key-value pair
	 */
	public static class AttributeDelayReducer extends Reducer<IntTextPair, BooleanWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();
        private final Text ansKey = new Text();

		@Override
		public void reduce(final IntTextPair key, final Iterable<BooleanWritable> values, final Context context) throws IOException, InterruptedException {
			int zeroDelayCount = 0;
			int nonZeroDelayCount = 0;
			for (final BooleanWritable val : values) {
			    if(val.get())
			        zeroDelayCount++;
			    else
			        nonZeroDelayCount++;
			}
			result.set(zeroDelayCount);
			ansKey.set(key.toString() + ",0");
			context.write(ansKey, result);
			result.set(nonZeroDelayCount);
            ansKey.set(key.toString() + ",1");
			context.write(ansKey, result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {

		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Categorical Attributes Frequencies With Delay");
		job.setJarByClass(CategoricalCorrelation.class);
		final Configuration jobConf = job.getConfiguration();
		// Sets output delimeter for each line
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job.setMapperClass(CommaTokenizingMapper.class);
		job.setReducerClass(AttributeDelayReducer.class);
        //job.setSortComparatorClass(KeyComparator.class);
        job.setMapOutputKeyClass(IntTextPair.class);
        job.setMapOutputValueClass(BooleanWritable.class);
        job.setOutputKeyClass(Text. class);
		job.setOutputValueClass(IntWritable.class);

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
			ToolRunner.run(new CategoricalCorrelation(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}