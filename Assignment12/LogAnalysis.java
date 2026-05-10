package Assignment12;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class LogAnalysis {

    // Mapper
    public static class Map
            extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.contains("INFO")) {
                context.write(new Text("INFO"),
                        new IntWritable(1));
            }

            else if (line.contains("ERROR")) {
                context.write(new Text("ERROR"),
                        new IntWritable(1));
            }

            else if (line.contains("WARNING")) {
                context.write(new Text("WARNING"),
                        new IntWritable(1));
            }
        }
    }

    // Reducer
    public static class Reduce
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(),
                "Log Analysis");

        job.setJarByClass(LogAnalysis.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
