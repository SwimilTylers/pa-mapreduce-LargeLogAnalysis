package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MultipleLineInputCheckHelper extends Configured {
    public static class LineCountMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
        private String buffer = null;
        private LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            buffer = value.toString();
            context.write(new LongWritable(buffer.split("\n").length), one);
        }
    }

    public static class LineCountReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable value:values
                 ) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Multiple Line Counter");
        job.setJarByClass(MultipleLineInputCheckHelper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        // MultipleLineInputFormat.asInputFormat(job, 1000);

        job.setMapperClass(LineCountMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setCombinerClass(LineCountReducer.class);

        job.setReducerClass(LineCountReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
