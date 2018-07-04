package StateCodeCounter;

import Utils.TimeStampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class sccReducer extends Reducer<Text, IntWritable, TimeStampWritable, NullWritable> {
    private String current_time = null;
    private int[] state_code_count = new int[3];

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String time = key.toString().split("#")[0];
        int stateCode = Integer.parseInt(key.toString().split("#")[1]);

        if (current_time != null && !current_time.equals(time)){
            StringBuilder stringBuilder = new StringBuilder();
            if (!current_time.equals("$")){
                stringBuilder.append(" 200:"+state_code_count[0]);
                stringBuilder.append(" 404:"+state_code_count[1]);
                stringBuilder.append(" 500:"+state_code_count[2]);
                context.write(new TimeStampWritable(stringBuilder.toString(), Integer.parseInt(current_time)), NullWritable.get());
            }
            else{
                stringBuilder.append(" 200:"+state_code_count[0]+'\n');
                stringBuilder.append(" 404:"+state_code_count[1]+'\n');
                stringBuilder.append(" 500:"+state_code_count[2]);
                context.write(new TimeStampWritable(stringBuilder.toString()), NullWritable.get());
            }


            state_code_count = new int[]{0,0,0};
        }

        int sum = 0;
        for (IntWritable value:values) {
            sum += value.get();
        }

        switch (stateCode){
            case 200: state_code_count[0] += sum;
            case 404: state_code_count[1] += sum;
            case 500: state_code_count[2] += sum;
        }
        current_time = time.toString();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        if (!current_time.equals("$")){
            stringBuilder.append("200:"+state_code_count[0]);
            stringBuilder.append("404:"+state_code_count[1]);
            stringBuilder.append("500:"+state_code_count[2]);
            context.write(new TimeStampWritable(stringBuilder.toString(), Integer.parseInt(current_time)), NullWritable.get());
        }
        else{
            stringBuilder.append(" 200:"+state_code_count[0]+'\n');
            stringBuilder.append(" 404:"+state_code_count[1]+'\n');
            stringBuilder.append(" 500:"+state_code_count[2]);
            context.write(new TimeStampWritable(stringBuilder.toString()), NullWritable.get());
        }
    }
}
