package IPCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import Utils.LogEntryParser;
import Utils.CustomizedFileNameTextOutputFormat;

public class IPCounter {
    /*
        map使用LogEntryParser解析每一条log信息，并把IP+time作为key传递参数，值都为1
     */
    public static class IPCounterMapper extends Mapper<Object,Text,Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private LogEntryParser parser=null;
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            try {
                parser = new LogEntryParser(value.toString());
                String time = String.format("%02d", parser.getTimeSplits()[0]);
                //time只取hours
                String IP = parser.getIp();
                context.write(new Text(IP + "#" + time), one);
            }catch (ArrayIndexOutOfBoundsException | StringIndexOutOfBoundsException ignored){}
            
        }
    }

    /*
        combiner将同一节点key值相同的项合并，减少网络开销。
     */
    public static class SumCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    /*
        自定义Partitioner，把key中ip相同的分配到同一个reducer节点
     */
    public static class IPCounterPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions){
            String ip = key.toString().split("#")[0]; //??????
            return super.getPartition(new Text(ip),value,numPartitions);
        }

    }

    public static class IPCounterReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
        //通过MultipleOutputs实现多文件输出，
        private  MultipleOutputs <Text, NullWritable> multipleOutputs;
        @Override
        protected void setup(Context context) throws IOException ,InterruptedException
        {
                multipleOutputs = new MultipleOutputs< Text, NullWritable>(context);
            //由于目标输出格式的文件名是ip，而内容中则无ip，都是对应的values，所以第二个参数设置nullwritable，可输出空。
        }

        static String currentIP =" ";
        static String temp=" ";
        static List<String> timeInfoList = new ArrayList<String>();

        /*
            timeinfolist存储同一ip，不同时间段的出现次数，一个时间段的信息为一个元素。
            reduce之前会自动排序，按照时间递增。
            当同一ip的数据全部统计后，把数组元素全部添加到一个字符串中，并在此循环中统计出现总数。
         
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sumoftotalkey = 0;
            String IP=key.toString().split("#")[0];
            String time = key.toString().split("#")[1];
            int tp=Integer.parseInt(time);
            int tp2=tp+1;
            time=String.valueOf(tp)+":00-"+String.valueOf(tp2)+":00";
            for (IntWritable val : values) {
                sumoftotalkey += val.get();
            }
            temp=time+" "+sumoftotalkey+"$";
            if(!currentIP.equals(IP) && !currentIP.equals(" "))
            {
                StringBuilder out=new StringBuilder();
                long count=0;
                for(String p:timeInfoList)
                {
                    count+=Long.parseLong(p.substring(p.indexOf(" ")+1,p.indexOf("$")));
                    p=p.replace("$","");
                    out.append(p);
                    out.append("\n");
                }
                out.insert(0,count+"\n");
                out.deleteCharAt(out.length() - 1);
                if(count>0)
               //     context.write(new Text(currentIP),new Text(out.toString()));
                    multipleOutputs.write(new Text(out.toString()),NullWritable.get(),currentIP);

                timeInfoList=new ArrayList<String>();
            }
            currentIP = IP;
            timeInfoList.add(temp);

        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            StringBuilder out = new StringBuilder();
            long count = 0;
            for (String p : timeInfoList) {
                count += Long.parseLong(p.substring(p.indexOf(" ") + 1, p.indexOf("$")));
                p = p.replace("$", "");
                out.append(p);
                out.append("\n");
            }
            out.insert(0,  + count + "\n");
            out.deleteCharAt(out.length() - 1);
            if (count > 0)
          //      context.write(new Text(currentIP), new Text(out.toString()));
                multipleOutputs.write(new Text(out.toString()),NullWritable.get(),currentIP);
            multipleOutputs.close();
        }
    }

    public static int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"IP Visitor Counter");
        job.setJarByClass(IPCounter.class);
        job.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job,CustomizedFileNameTextOutputFormat.class);

        job.setMapperClass(IPCounterMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(IPCounterPartitioner.class);
        job.setReducerClass(IPCounterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;

    }
    public static void main(String[] args) throws Exception{
        IPCounter driver=new IPCounter();
        System.exit(driver.run(args));
    }
}
