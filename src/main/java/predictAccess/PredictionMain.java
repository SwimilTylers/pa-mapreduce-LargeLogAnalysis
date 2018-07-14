package predictAccess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.List;


public class PredictionMain {

    /**
     *
     * @param inputPath single file in HDFS
     * @param outputPath
     * @return
     * @throws Exception
     */
    private static boolean runURLCounter(String inputPath,String outputPath) throws Exception{
        System.out.println("URL Counter Job of "+inputPath);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"URL Counter of "+inputPath);
        job.setJarByClass(InterFreqCounter.class);
        job.setMapperClass(InterFreqCounter.myMapper.class);
        job.setCombinerClass(InterFreqCounter.myCombiner.class);
        job.setPartitionerClass(InterFreqCounter.myPartitioner.class);
        job.setReducerClass(InterFreqCounter.myReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
        return job.waitForCompletion(true);
    }


    /**
     *
     * @param inputPath
     * @param outputPath
     * @return
     * @throws Exception
     */
    private static boolean runURLCounterBySingleFile(String inputPath,String outputPath) throws Exception{
       String beginName  = inputPath+"/2015-09-",endName=".log";
       boolean successRun =true;
       for(int i=8;i<=22;i++){
           String date= String.format("%02d",i);
           if(!runURLCounter(beginName+date+endName,outputPath+"/"+date))
               successRun=false;
       }
        return successRun;
    }

    private static boolean runURLPredict(String inputPath,String outputPath) throws Exception{
        System.out.println("URL Predict Job");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"URL Predict");
        job.setJarByClass(URLPredict.class);
        job.setMapperClass(URLPredict.myMapper.class);
        job.setPartitionerClass(URLPredict.myPartitioner.class);
        job.setReducerClass(URLPredict.myReducer.class);


        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

    //    FileInputFormat.setInputPaths(job,new Path(inputPath));
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
        return job.waitForCompletion(true);

/*
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"URL Predict");
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
  */

    }

    private static boolean runURLPredictBySingleFile(String inputPath,String outputPath) throws Exception{
        String inputPaths = "";
        for(int i=8;i<22;i++){
            String date = String.format("%02d",i);
            inputPaths+=inputPath+"/"+date+",";
        }
        inputPaths+=inputPath+"/22";
        return runURLPredict(inputPaths,outputPath);
    }

    private static void updateFileStructure(String inputPath, String outputPath)throws Exception{
        Common.rumCmd("hdfs dfs -mkdir "+outputPath);
        for(int i=8;i<=22;i++){
            String date = String.format("%02d",i);
            Common.rumCmd("hdfs dfs -cp "+inputPath+"/"+date+"/* "+outputPath);
        }
    }

    public static void main(String[] args)throws Exception{
        runURLCounter(args[0],"urlCounter");
        runURLPredict("urlCounter",args[1]);

      //  runURLCounterBySingleFile(args[0],args[1]+"URLCounter");
     //   updateFileStructure(args[1]+"URLCounter","urlCounter");
     //   runURLPredict("urlCounter",args[1]);
    }
}
