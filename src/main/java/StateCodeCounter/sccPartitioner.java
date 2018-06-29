package StateCodeCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class sccPartitioner extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions){
        return super.getPartition(new Text(key.toString().split("#")[0]), value, numPartitions);
    }
}
