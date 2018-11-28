package AttributeAnalysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class FrequencyOfDomainValues extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(FrequencyOfDomainValues.class);

	public static class CommaTokenizingMapper extends Mapper<Object, Text, IntTextPair, IntWritable> {

	    private HashMap<IntTextPair, Integer> mapping;
	    private IntWritable value = new IntWritable();

	    public void setup(Context context) throws IOException, InterruptedException{
	        mapping = new HashMap<IntTextPair, Integer>();
        }

        @Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String link[] = value.toString().split(",");
            for(int i = 0; i < link.length; i++){
                IntTextPair ansKey = new IntTextPair();
                ansKey.set(i, link[i]);
                if(mapping.containsKey(ansKey)){
                    mapping.put(ansKey, mapping.get(ansKey) + 1);
                }
                else{
                    mapping.put(ansKey, 1);
                }
            }
		}

        protected void cleanup(Context context) throws IOException, InterruptedException {
	        for(Map.Entry<IntTextPair, Integer> entry : mapping.entrySet()){
	            value.set(entry.getValue());
	            context.write(entry.getKey(), value);
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
	public static class AttributeFrequencyReducer extends Reducer<IntTextPair, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();
        private final Text ansKey = new Text();

		@Override
		public void reduce(final IntTextPair key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int count = 0;
			for (final IntWritable val : values) {
			    count+=val.get();
			}
			result.set(count);
			ansKey.set(key.toString());
			context.write(ansKey, result);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {

		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Categorical Attributes Frequencies");
		job.setJarByClass(FrequencyOfDomainValues.class);
		final Configuration jobConf = job.getConfiguration();
		// Sets output delimeter for each line
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job.setMapperClass(CommaTokenizingMapper.class);
		job.setReducerClass(AttributeFrequencyReducer.class);
        job.setMapOutputKeyClass(IntTextPair.class);
        job.setMapOutputValueClass(IntWritable.class);
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
			ToolRunner.run(new FrequencyOfDomainValues(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}