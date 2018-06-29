package StateCodeCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class sccMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] entrySplit = value.toString().split(" ");

        String time = entrySplit[1];
        int start_idx = time.indexOf(':') + 1;
        time = time.substring(start_idx, time.indexOf(':', start_idx));

        String stateCode = entrySplit[4];

        context.write(new Text(time+"#"+stateCode), one);
        context.write(new Text("_#"+stateCode), one);
    }
}
