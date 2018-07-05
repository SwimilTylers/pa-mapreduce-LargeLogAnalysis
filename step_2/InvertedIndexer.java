package step_2;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndexer {

    public static class InvertedIndexerMapper extends Mapper<Object,Text,Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private LogEntryParser parser=null;
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            parser=new LogEntryParser(value.toString());
            String time=String.format("%02d",parser.getTimeSplits()[0]);
            String IP=parser.getIp();
            context.write(new Text(IP+"#"+time),one);
          /*
            Text word = new Text();
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,"\n");
            String temp;
            while (tokenizer.hasMoreTokens()) {
                temp=tokenizer.nextToken();
                if(temp.substring(0,1).equals("-"))
                    continue;
                word.set(temp.substring(0,12)+"#"+temp.substring(26,28)); //ip
                //  word2.set("_#"+temp.substring(0,12));
                context.write(word, one);
                //           context.write(word2, one);
            }
            */
            //    context.write(word,new IntWritable(1));
        }
    }

    
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

   
    public static class InvertedIndexerPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions){
            String ip = key.toString().split("#")[0]; //??????
            return super.getPartition(new Text(ip),value,numPartitions);
        }

    }

    public static class InvertedIndexerReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
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

        static String currentIP =" ";
        static String temp=" ";
        static List<String> timeInfoList = new ArrayList<String>();


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


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"figure log");
        job.setJarByClass(InvertedIndexer.class);
        job.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);

        job.setMapperClass(InvertedIndexerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(InvertedIndexerPartitioner.class);
        job.setReducerClass(InvertedIndexerReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
