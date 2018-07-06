package com.task4;

import Utils.CustomizedFileNameTextOutputFormat;
import Utils.LogEntryParser;
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

public class InterfaceCount {
    public static class InterfaceCountMapper extends Mapper<Object,Text,Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private LogEntryParser parser=null;
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            parser=new LogEntryParser(value.toString());

            String time=String.format("%02d",parser.getTimeSplits()[0]);

            String http_req=parser.getHttp_request();
            http_req=http_req.split(" ")[1];

            int delay=parser.getDelay();

            context.write(new Text(http_req+"#"+time),new IntWritable(delay));

        }
    }

    /*
        dont add up,
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

    public static class InterfaceCountPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions){
            String http_req = key.toString().split("#")[0];
            return super.getPartition(new Text(http_req),value,numPartitions);
        }

    }

    public static class InterfaceCountReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
        /*
              static String currentWord =" ";
              static List<String> fileInfoList = new ArrayList<String>();
      */
        private  MultipleOutputs <Text, NullWritable> multipleOutputs;
        @Override
        protected void setup(Context context) throws IOException ,InterruptedException
        {
            multipleOutputs = new MultipleOutputs< Text, NullWritable>(context);
        }

        static String currentURL =" ";
        static String temp=" ";
        static List<String> timeInfoList = new ArrayList<String>();

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
            String oneaverage=String.format("%.3f",average);
            //

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
                    multipleOutputs.write(new Text(out.toString()),NullWritable.get(),currentURL);
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
                multipleOutputs.write(new Text(out.toString()),NullWritable.get(),currentURL);
            }
            multipleOutputs.close();
        }
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"figure log");
        job.setJarByClass(InterfaceCount.class);
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
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
