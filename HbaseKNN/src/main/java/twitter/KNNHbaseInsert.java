package twitter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class KNNHbaseInsert extends Configured implements Tool {
	// Declaring the logger for the program
	private static final Logger logger = LogManager.getLogger(KNNHbaseInsert.class);

	// Mapper for Edges which emits (y,1) for every row (x,y)
	public static class EdgesMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static Text rowkey = new Text();
		private final IntWritable departDelayGroup = new IntWritable();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			// Parsing on comma
			final String[] row = value.toString().split(",");
            Double v = Double.parseDouble(row[12]);

            String departTIme = row[7];
			String carrierId = row[4];

			String stringToSet = v.intValue() + "," + departTIme + "," + carrierId;

			rowkey.set(stringToSet);
			departDelayGroup.set(Integer.parseInt(row[8]));

			context.write(rowkey,departDelayGroup);

		}
	}
//


	public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
		public static final byte[] CF = "a".getBytes();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			final Map<Integer,Long> delayGroupCount = new HashMap();
			StringBuilder sb = new StringBuilder();
			for(int i=0 ;i<=13 ; i++) {
                delayGroupCount.put(i,0L);

			}
			final String[] row = key.toString().split(",");
			System.out.println("Row" + key.toString());
			logger.info("Row:" + key.toString());
			long distance = Long.parseLong(row[0]);
			long departTIme = Long.parseLong(row[1]) * 100L;
			String carrierId = row[2];

			for (IntWritable val : values) {
				int delayGroup = val.get();
				long count = delayGroupCount.get(delayGroup);
				count++;
				delayGroupCount.put(delayGroup,count);

			}

			Put put = new Put(Bytes.add(Bytes.toBytes(distance), Bytes.toBytes(departTIme)));
			AtomicReference<String> prefix = new AtomicReference<>("");
			// sb.append(distance + "," + departTIme + "," + distance + "" + departTIme + ",");
			delayGroupCount.forEach( (k,v) -> {
			    sb.append(prefix.get());
                prefix.set(",");
			    String temp = k + ":" + v;
                sb.append(temp);

            });



            put.addColumn(CF, carrierId.getBytes(), Bytes.toBytes(sb.toString()));

            context.write(null, put);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {

		// Initiating configuration for the job
		Configuration conf =  HBaseConfiguration.create();
		final Job job = Job.getInstance(conf, "Twitter Followers Count");
		job.setJarByClass(KNNHbaseInsert.class);

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);

		job.setMapperClass(EdgesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		TableMapReduceUtil.initTableReducerJob("Flights",
				MyTableReducer.class, job);
		job.setReducerClass(MyTableReducer.class);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}


		return b ? 0 :1;

	}

	public static void main(final String[] args) {
		// Checking whether 3 arguments are passed or not
		if (args.length != 3) {
			throw new Error("Two arguments required:\n<input-node-dir> <input-edges-directory> <output-dir>");
		}

		try {
			// Running implementation class
			ToolRunner.run(new KNNHbaseInsert(), args);
		} catch (final Exception e) {
			logger.error("MR Job Exception", e);
		}
	}

}