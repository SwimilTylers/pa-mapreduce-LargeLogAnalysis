package UrlVisitorCounter;

import Utils.LogEntryParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class uvcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);
    private LogEntryParser parser = null;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            parser = new LogEntryParser(value.toString());
            String request = parser.getHttp_request();
            String url = request.split(" ")[1];
            int[] time = parser.getTimeSplits();
            String stime = String.format("%02d:%02d:%02d", time[0], time[1], time[2]);

            context.write(new Text(url + "#" + stime), one);
            context.write(new Text(url + "#$"), one);
        }catch (ArrayIndexOutOfBoundsException e){

        }
    }
}
