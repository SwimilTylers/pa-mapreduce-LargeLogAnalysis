package UrlVisitorCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class uvcReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    private String current_url = null;
    private String current_time = null;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String url = key.toString().split("#")[0];
        String time = key.toString().split("#")[1];

        MultipleOutputs mos = new MultipleOutputs(context);
        mos.write(null, null, null);
    }
}
