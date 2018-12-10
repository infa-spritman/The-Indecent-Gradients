package twitter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KNNHbase extends Configured implements Tool {
    // Declaring the logger for the program
    private static final Logger logger = LogManager.getLogger(KNNHbase.class);

    // Mapper for Edges which emits (y,1) for every row (x,y)
    public static class KNNMapper extends Mapper<Object, Text, Text, IntWritable> {
        public static final byte[] CF = "a".getBytes();

        private final static Text rowkey = new Text();
        private final IntWritable predictDepartDelayGroup = new IntWritable();

        private Table flightTable;

        private int K;


        public void setup(Context context) {
            // In here create a Connection to the cluster and save it or use the Connection
            // from the existing table
            Connection connection = null;
            try {
                Configuration configuration = context.getConfiguration();
                connection = ConnectionFactory.createConnection(configuration);
                flightTable = connection.getTable(TableName.valueOf("Flights"));
                K = configuration.getInt("KValue", 100);
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

           Scan scan = new Scan();
           scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
           scan.setCacheBlocks(false);
            // Parsing on comma
            final String[] row = value.toString().split(",");
            Double distance = Double.parseDouble(row[12]);
            long ldistance = distance.intValue();
            long departTIme = Long.parseLong(row[7]) * 100L;
            String carrierId = row[4];

            // Setting up Map

            final Map<Integer, Long> delayGroupCount = new HashMap();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i <= 13; i++) {
                delayGroupCount.put(i, 0L);

            }

            //

            long offsetTop = ldistance;
            long offsetBottom = ldistance;
            long resultSize = 0;
            int i = 0;

            do {

                if (i == 0) {
                    Get g = new Get(Bytes.add(Bytes.toBytes(ldistance), Bytes.toBytes(departTIme)));
                    g.addColumn(CF, carrierId.getBytes());
                    Result rr = flightTable.get(g);

                    if (!rr.isEmpty()) {


                        String[] delayValues = Bytes.toString(rr.getValue(CF, carrierId.getBytes())).split(",");
                        for (String delayValueMap : delayValues) {
                            String[] split = delayValueMap.split(":");
                            int delayGroup = Integer.parseInt(split[0]);
                            long count = delayGroupCount.get(delayGroup);
                            long delayCount = Long.parseLong(split[1]);
                            resultSize = resultSize + delayCount;
                            count = count + delayCount;
                            delayGroupCount.put(delayGroup, count);

                        }


                    }

                    i++;
                    offsetTop--;
                    offsetBottom++;
                } else {

                    if (offsetTop >= 0) {
                        Get g = new Get(Bytes.add(Bytes.toBytes(offsetTop), Bytes.toBytes(departTIme)));
                        g.addColumn(CF, carrierId.getBytes());
                        Result rr = flightTable.get(g);

                        if (!rr.isEmpty()) {


                            String[] delayValues = Bytes.toString(rr.getValue(CF, carrierId.getBytes())).split(",");
                            for (String delayValueMap : delayValues) {
                                String[] split = delayValueMap.split(":");
                                int delayGroup = Integer.parseInt(split[0]);
                                long delayCount = Long.parseLong(split[1]);
                                long count = delayGroupCount.get(delayGroup);
                                resultSize = resultSize + delayCount;
                                count = count + delayCount;
                                delayGroupCount.put(delayGroup, count);

                            }


                        }

                    }

                    if (offsetBottom <= 5000) {
                        Get g = new Get(Bytes.add(Bytes.toBytes(offsetBottom), Bytes.toBytes(departTIme)));
                        g.addColumn(CF, carrierId.getBytes());
                        Result rr = flightTable.get(g);

                        if (!rr.isEmpty()) {


                            String[] delayValues = Bytes.toString(rr.getValue(CF, carrierId.getBytes())).split(",");
                            for (String delayValueMap : delayValues) {
                                String[] split = delayValueMap.split(":");
                                int delayGroup = Integer.parseInt(split[0]);
                                long delayCount = Long.parseLong(split[1]);
                                long count = delayGroupCount.get(delayGroup);
                                resultSize = resultSize + delayCount;
                                count = count + delayCount;
                                delayGroupCount.put(delayGroup, count);

                            }


                        }

                    }

                    i++;
                    offsetTop--;
                    offsetBottom++;

                }


            } while (resultSize < K && (offsetTop >= 0 || offsetBottom <= 5000));



            predictDepartDelayGroup.set(Collections.max(delayGroupCount.entrySet(), Map.Entry.comparingByValue()).getKey());
            rowkey.set(value.toString());

            context.write(rowkey, predictDepartDelayGroup);

        }
    }


    @Override
    public int run(final String[] args) throws Exception {

        // Initiating configuration for the job
        Configuration conf = HBaseConfiguration.create();
        final Job job = Job.getInstance(conf, "Twitter Followers Count");
        job.setJarByClass(KNNHbase.class);
        final Configuration jobConf = job.getConfiguration();
        // Sets output delimeter for each line
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        jobConf.setInt("KValue", Integer.parseInt(args[2]));


        job.setMapperClass(KNNMapper.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);
        // FileInputFormat takes up TextInputFormat as default
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }


        return b ? 0 : 1;

    }

    public static void main(final String[] args) {
        // Checking whether 3 arguments are passed or not
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<test-data-directory> <output-dir> K");
        }

        try {
            // Running implementation class
            ToolRunner.run(new KNNHbase(), args);
        } catch (final Exception e) {
            logger.error("MR Job Exception", e);
        }
    }

}