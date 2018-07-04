package Utils;

import com.sun.istack.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogFormatCheckHelper extends Configured {
    public static class ExceptionMarker implements WritableComparable<ExceptionMarker>{
        Text orig = null;
        Text where = null;
        Text message = null;

        @Override
        public int compareTo(ExceptionMarker o) {
            return orig.compareTo(o.orig);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            orig.write(out);
            where.write(out);
            message.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            orig = new Text();
            orig.readFields(in);

            where = new Text();
            where.readFields(in);

            message = new Text();
            message.readFields(in);
        }

        @Override
        public int hashCode(){
            return orig.hashCode();
        }
    }

    public static class GeneralCombiner extends Reducer<Object, IntWritable, Object, IntWritable>{
        public void reduce(Object key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:
                 values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class ExceptionMapper extends Mapper<LongWritable, Text, ExceptionMarker, IntWritable>{
        private LogEntryParser parser = null;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                parser = new LogEntryParser(value.toString());
                String time = String.format("%02d", parser.getTimeSplits()[0]);

                int stateCode = parser.getState_code();
            }catch(Exception e){
                ExceptionMarker marker = new ExceptionMarker();
                marker.orig = value;
                marker.where = new Text(((FileSplit)context.getInputSplit()).getPath().toString());
                marker.message = new Text(e.toString());
                context.write(marker, new IntWritable(1));
            }
        }
    }

    public static class MessageReducer extends Reducer<ExceptionMarker, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(ExceptionMarker key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:
                 values) {
                sum += value.get();
            }
            context.write(new Text(key.orig+"\n\tin "+key.where+"\n\t["+key.message+"]"), new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Format Check");
        job.setJarByClass(LogFormatCheckHelper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(ExceptionMapper.class);
        job.setMapOutputKeyClass(ExceptionMarker.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(GeneralCombiner.class);
        job.setReducerClass(MessageReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
