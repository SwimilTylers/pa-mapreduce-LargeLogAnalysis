package Utils;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

abstract public class CustomFileNameTextOutputFormat<K extends WritableComparable<K>,V extends Writable> extends FileOutputFormat<K, V> {
    abstract protected String getFileName(TaskAttemptContext job);

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.newInstance(job.getConfiguration());
        final FSDataOutputStream result_filename = fs.create(new Path(getFileName(job)));
        RecordWriter<K, V> costumed = new RecordWriter<K, V>() {
            @Override
            public void write(K key, V value) throws IOException, InterruptedException {
                key.write(result_filename);
                value.write(result_filename);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                if (result_filename != null)
                    result_filename.close();
            }
        };
        return costumed;
    }
}
