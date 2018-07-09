package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

import static org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.getNumLinesPerSplit;

public class MultipleLineRecordReader extends RecordReader<LongWritable, Text> {
    private int N_LINES_TO_PROCESS;
    private LineReader in;
    private LongWritable key;
    private Text value = new Text();
    private long start = 0;
    private long end = 0;
    private long pos = 0;
    private int max_line_length;
    private final Text delimiter = new Text("\n");

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
        N_LINES_TO_PROCESS = getNumLinesPerSplit(context);
        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();
        this.max_line_length = conf.getInt(MultipleLineInputFormat.MAX_LINE,Integer.MAX_VALUE);
        FileSystem fs = file.getFileSystem(conf);
        start = split.getStart();
        end= start + split.getLength();
        boolean skipFirstLine = false;
        FSDataInputStream filein = fs.open(split.getPath());

        if (start != 0){
            skipFirstLine = true;
            --start;
            filein.seek(start);
        }
        in = new LineReader(filein,conf);
        if(skipFirstLine){
            start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();

        int newSize = 0;
        for(int i = 0; i< N_LINES_TO_PROCESS; i++){
            Text v = new Text();
            while (pos < end) {
                newSize = in.readLine(v, max_line_length, Math.max((int)Math.min(Integer.MAX_VALUE, end-pos), max_line_length));
                value.append(v.getBytes(),0, v.getLength());
                value.append(delimiter.getBytes(),0, delimiter.getLength());
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                if (newSize < max_line_length) {
                    break;
                }
            }
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
}
