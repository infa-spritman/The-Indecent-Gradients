package cleanup;

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

public class Cleanup extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(cleanup.Cleanup.class);

	// Mapper
	// Remove dirty values
	// Change Unique Carrier to number
	// CRS Dept time to just hour
	// Dep_Delay_New to groups
	// CRS Arr time to just hour
	// Arr_Delay_New to groups
	// CRS Elapsed time to same
	// Distance to same
	// Schema : "YEAR","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","OP_UNIQUE_CARRIER","ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID","CRS_DEP_TIME","DEP_DELAY_NEW","CRS_ARR_TIME","ARR_DELAY_NEW","CRS_ELAPSED_TIME","DISTANCE",
	public static class CleanUpMapper extends Mapper<Object, Text, NullWritable, FlightWritable> {

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String dataStr = value.toString();
			if (dataStr.contains("YEAR"))
				return ;
			if (dataStr.contains(",,"))
				return ;

			String[] fieldVals = dataStr.split(",");
			String year = fieldVals[0];
			String month = fieldVals[1];
			String dayOfMonth = fieldVals[2];
			String dayOfWeek = fieldVals[3];
			String carrier = CarrierLookup.getCarrier(fieldVals[4].replace("\"",""));
			String originAirport = AirportLookup.getAirport(fieldVals[5]);
			String destAirport = AirportLookup.getAirport(fieldVals[6]);
			String departTime = getHour(fieldVals[7].replace("\"",""));
			String departDelay = getDelayGroup(fieldVals[8]);
			String arriveTime = getHour(fieldVals[9].replace("\"",""));
			String arriveDelay = getDelayGroup(fieldVals[10]);
			String schecduledFlightTime = fieldVals[11];
			String distance = fieldVals[12];

			FlightWritable cleaned = new FlightWritable(year,
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

			context.write(NullWritable.get(), cleaned);
		}
	}

	public static String getHour(String time) {
        final int mid = time.length() / 2;
        return time.substring(0, mid);
    }

    public static String getDelayGroup(String delay) {
	    String[] parts = delay.split("\\.");
	    int minutes = Integer.parseInt(parts[0]);
	    if (minutes == 0)
	        return "0"; // No delay
        else if (minutes > 0 && minutes < 15)
            return "1";
        else if (minutes >= 15 && minutes < 30)
            return "2";
        else if (minutes >= 30 && minutes < 45)
            return "3";
        else if (minutes >= 45 && minutes < 60)
            return "4";
        else if (minutes >= 60 && minutes < 75)
            return "5";
        else if (minutes >= 75 && minutes < 90)
            return "6";
        else if (minutes >= 90 && minutes < 105)
            return "7";
        else if (minutes >= 105 && minutes < 120)
            return "8";
        else if (minutes >= 120 && minutes < 135)
            return "9";
        else if (minutes >= 135 && minutes < 150)
            return "10";
        else if (minutes >= 150 && minutes < 165)
            return "11";
        else if (minutes >= 165 && minutes < 180)
            return "12";
        else return "13";
    }


	// run
	@Override
	public int run(final String args[]) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Data Cleanup");
        job.setJarByClass(cleanup.Cleanup.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        // ================
        job.setMapperClass(CleanUpMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(FlightWritable.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
	}


	// main
	public static void main(String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
            ToolRunner.run(new cleanup.Cleanup(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
	}
}