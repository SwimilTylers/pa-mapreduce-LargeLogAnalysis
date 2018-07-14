package DelayCounter;

import Utils.CustomizedFileNameTextOutputFormat;
import Utils.LogEntryParser;
import Utils.TimeStampWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

public class DelayCounter {
    /*
        map使用LogEntryParser解析每一条log信息，并把端口+time作为key传递参数，值为响应时间delay
     */
    public static class InterfaceCountMapper extends Mapper<Object,Text,Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private LogEntryParser parser=null;
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            try {
                parser = new LogEntryParser(value.toString());

                String time = String.format("%02d", parser.getTimeSplits()[0]);

                String http_req = parser.getHttp_request();
                String url = http_req.split(" ")[1];

                int delay = parser.getDelay();

                context.write(new Text(url + "#" + time), new IntWritable(delay));
           //     context.write(new Text(url + "#$"), new IntWritable(delay));
            }catch (ArrayIndexOutOfBoundsException | StringIndexOutOfBoundsException ignored){}

        }
    }

    /*
       不需要combiner
     */
    /*
    public static class InterfaceCountCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int sum = 0;

            for (IntWritable val : values){
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

   */

    /*
     自定义Partitioner，把key中相同的端口分配到同一个reducer节点
     */
    public static class InterfaceCountPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions){
            String http_req = key.toString().split("#")[0];
            return super.getPartition(new Text(http_req),value,numPartitions);
        }

    }

    public static class InterfaceCountReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
         //通过MultipleOutputs实现多文件输出，
        private  MultipleOutputs <Text, NullWritable> mos;
        @Override
        protected void setup(Context context) throws IOException ,InterruptedException
        {
            mos = new MultipleOutputs< Text, NullWritable>(context);
        }

        static String currentURL =" ";
        static String temp=" ";
        static List<String> timeInfoList = new ArrayList<String>();

        /*
         timeinfolist数组存储同一端口，不同时间段的delay平均值，一个时间段的信息为一个元素。
         reduce之前会自动排序，按照时间递增。
         为了统计总平均值，数组中还需添加一些信息。
         当同一端口的数据全部统计后，把数组元素中信息提取，并在此循环中统计出现总数，然后舍去和输出无关的信息，全部添加到一个字符串中。
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String URL=key.toString().split("#")[0];
            String time = key.toString().split("#")[1];
            //time fix

            int tp=Integer.parseInt(time);
            int tp2=tp+1;
            time=String.valueOf(tp)+":00-"+String.valueOf(tp2)+":00";

            //url fix
            URL=URL.replace("/","-");
            if(URL.substring(0,1).equals("-"))
                URL=URL.replaceFirst("-","");
            //oneaverage fix
            long sumofkey = 0;
            int number=0;
            for (IntWritable val : values) {
                sumofkey += val.get();
                number+=1;
            }
            double  average=sumofkey/(double)number;
            String str_average=String.format("%.3f",average);
            //begin
/*
            if (!URL.equals("null")){
                if (time.equals("$")){
                    TimeStampWritable tsw = new TimeStampWritable(str_average);
                    mos.write(tsw, NullWritable.get(), URL);
                }
                else{
                    int hour=Integer.parseInt(time);
                    int[] start_time = new int[]{hour, 0 ,0};
                    int[] end_time = new int[]{hour+1, 0 ,0};
                    TimeStampWritable tsw = new TimeStampWritable(" "+str_average, start_time, end_time);
                    mos.write(tsw, NullWritable.get(),URL);
                }
            }*/
            //end

            String oneaverage=String.format("%.3f",average);
            temp=time+" "+oneaverage+"$"+sumofkey+"|"+number+"#";
            if(!currentURL.equals(URL) && !currentURL.equals(" "))
            {
                StringBuilder out=new StringBuilder();
                long sumofall=0;
                long numberall=0;
                double averageall=0;
                for(String p:timeInfoList)
                {
                    long sumofkey_in= Long.valueOf(p.substring(p.indexOf("$")+1,p.indexOf("|")));
                    long number_in=Long.valueOf(p.substring(p.indexOf("|")+1,p.indexOf("#")));
                    sumofall+=sumofkey_in;
                    numberall+=number_in;

                    p=p.substring(0,p.indexOf("$"));
                    out.append(p);
                    out.append("\n");
                }
                averageall=sumofall/(double)numberall;
                String str_averageall=String.format("%.3f",averageall);
                out.insert(0,str_averageall+"\n");
                out.deleteCharAt(out.length() - 1);
                if(sumofall>0) {
                    //   context.write(new Text(currentURL),new Text(out.toString()));
                    //context.write(new Text(currentURL +"/n" +out.toString()), NullWritable.get());
                    mos.write(new Text(out.toString()),NullWritable.get(),currentURL);
                }
                timeInfoList=new ArrayList<String>();
            }
            currentURL= URL;
            timeInfoList.add(temp);

        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            StringBuilder out=new StringBuilder();
            long sumofall=0;
            long numberall=0;
            double averageall=0;
            for(String p:timeInfoList)
            {
                long sumofkey_in= Long.valueOf(p.substring(p.indexOf("$")+1,p.indexOf("|")));
                long number_in=Long.valueOf(p.substring(p.indexOf("|")+1,p.indexOf("#")));
                sumofall+=sumofkey_in;
                numberall+=number_in;

                p=p.substring(0,p.indexOf("$"));
                out.append(p);
                out.append("\n");
            }
            averageall=sumofall/(double)numberall;
            String str_averageall=String.format("%.3f",averageall);
            out.insert(0,str_averageall+"\n");
            out.deleteCharAt(out.length() - 1);
            if(sumofall>0) {
                //   context.write(new Text(currentURL),new Text(out.toString()));
                //context.write(new Text(currentURL +"/n" +out.toString()), NullWritable.get());
                mos.write(new Text(out.toString()),NullWritable.get(),currentURL);
            }

            mos.close();
        }
    }


    public static int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Expected Delay Counter");
        job.setJarByClass(DelayCounter.class);
        job.setInputFormatClass(TextInputFormat.class);

        LazyOutputFormat.setOutputFormatClass(job, CustomizedFileNameTextOutputFormat.class);

        job.setMapperClass(InterfaceCountMapper.class);
  //      job.setCombinerClass(InterfaceCountCombiner.class);
        job.setPartitionerClass(InterfaceCountPartitioner.class);
        job.setReducerClass(InterfaceCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
       DelayCounter driver =new DelayCounter();
       System.exit(driver.run(args));
    }
}
