package predictAccess;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;

public class InterFreqCounter {
    public static class myMapper extends Mapper<Text,Text,Text,IntWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class myCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static class myPartitioner extends HashPartitioner<Text,IntWritable>{
        public int getPartition(Text key, IntWritable value, int numReduceTasks){

        }
    }

    public static class myReducer extends Reducer<Text,IntWritable,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }
}
