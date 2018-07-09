package Utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import java.io.IOException;

public class MultipleLineInputFormat extends NLineInputFormat {
    public static final String MAX_LINE = "mapreduce.input.linerecordreader.line.maxlength";

    public static void asInputFormat(Job job, int max_line){
        job.getConfiguration().setInt(MultipleLineInputFormat.MAX_LINE, max_line);
        job.setInputFormatClass(MultipleLineInputFormat.class);
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        context.setStatus(split.toString());
        return new MultipleLineRecordReader();
    }
}
