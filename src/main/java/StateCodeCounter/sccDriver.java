package StateCodeCounter;

import Utils.CustomFileNameTextOutputFormat;
import Utils.TimeStampWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class sccDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "State Code Counter");
        job.setJarByClass(sccDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(sccMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(sccCombiner.class);
        job.setPartitionerClass(sccPartitioner.class);

        job.setReducerClass(sccReducer.class);
        job.setOutputKeyClass(TimeStampWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TaskOneTextOutput.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        sccDriver driver = new sccDriver();
        System.exit(driver.run(args));
    }
}
