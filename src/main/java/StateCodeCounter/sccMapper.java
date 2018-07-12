package StateCodeCounter;

import Utils.LogEntryParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class sccMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private LogEntryParser parser = null;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            parser = new LogEntryParser(value.toString());
            String time = String.format("%02d", parser.getTimeSplits()[0]);

            int stateCode = parser.getState_code();

            context.write(new Text(time + "#" + stateCode), one);
            context.write(new Text("$#" + stateCode), one);

        }catch (ArrayIndexOutOfBoundsException | StringIndexOutOfBoundsException ignored){}
    }
}
