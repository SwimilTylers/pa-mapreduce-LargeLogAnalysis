package UrlVisitorCounter;

import Utils.TimeStampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class uvcReducer extends Reducer<Text, IntWritable, TimeStampWritable, NullWritable> {
    private MultipleOutputs<TimeStampWritable, NullWritable> mos;

    @Override
    public void setup(Context context){
        mos = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String url = key.toString().split("#")[0];
        String time = key.toString().split("#")[1];

        int sum = 0;
        for (IntWritable value:values
             ) {
            sum += value.get();
        }

        if (!url.equals("null")){
            if (time.equals("$")){
                TimeStampWritable tsw = new TimeStampWritable(url+": "+sum);
                mos.write(tsw, NullWritable.get(), url.substring(1).replace("/", "-"));
            }
            else{
                String[] times = time.split(":");
                int hour = Integer.parseInt(times[0]);
                int minute = Integer.parseInt(times[1]);
                int second = Integer.parseInt(times[2]);

                int[] start_time = new int[]{hour, minute, second};
                ++second;
                if (second + 1 >= 60) {
                    minute++;
                    second = 0;
                }
                if (minute >= 60) {
                    hour++;
                    minute = 0;
                }
                hour = hour >= 24 ? 0 : hour;
                int[] end_time = new int[]{hour, minute, second};

                TimeStampWritable tsw = new TimeStampWritable(" "+sum, start_time, end_time);
                mos.write(tsw, NullWritable.get(), url.substring(1).replace("/", "-"));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
