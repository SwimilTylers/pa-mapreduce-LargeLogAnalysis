package predictAccess;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InterFreqCounter {
    public static class myMapper extends Mapper<Object,Text,Text,IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获取文件名
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            filename = filename.substring(0,filename.length()-4);
            String[] times = filename.split("-");

            List<String> values = Common.getInfoFromLine(value.toString());
            if(values.size()!=3) return;
            Text outKey=new Text(values.get(1)+"--"+times[2]+"#"+values.get(2));
            context.write(outKey,new IntWritable(1));
        }
    }

    public static class myCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for( IntWritable val:values) sum+=val.get();
            context.write(key,new IntWritable(sum));
        }
    }

    public static class myPartitioner extends HashPartitioner<Text,IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String time = key.toString().split("--")[0];
            return super.getPartition(new Text(time),value,numReduceTasks);
        }
    }

    public static class myReducer extends Reducer<Text,IntWritable,Text,Text>{
        private String currentTime = " ";
        private List<String> urlInfoList = new ArrayList<>();
        private MultipleOutputs<Text,Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException{
            multipleOutputs = new MultipleOutputs<Text,Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String time = key.toString().split("#")[0];
            String url = key.toString().split("#")[1];

            int sumOfUrl = 0;
            for(IntWritable val:values) sumOfUrl+=val.get();
            String urlInfo = url+":"+sumOfUrl;

            if(!currentTime.equals(time)&&!currentTime.equals(" ")){
                StringBuilder out = new StringBuilder();
                for(String p:urlInfoList){
                    out.append(p); out.append(";");
                }
                String date = currentTime.split("--")[1];
                multipleOutputs.write(new Text(currentTime),new Text(out.toString()),date);
                urlInfoList = new ArrayList<>();
            }

            currentTime = time;
            urlInfoList.add(urlInfo);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            for(String p:urlInfoList){
                out.append(p); out.append(";");
            }
            String date = currentTime.split("--")[1];
            multipleOutputs.write(new Text(currentTime),new Text(out.toString()),date);
            urlInfoList = new ArrayList<>();

            multipleOutputs.close();
        }
    }

    /*
    public static class SaveByDateOutputFormat extends MultipleTextOutputFormat<Text,Text> {
        @Override
        protected String generateFileNameForKeyValue(Text key,Text value,String filename){
            return filename;
        }
    }
    */


}
