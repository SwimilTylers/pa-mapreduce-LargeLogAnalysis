package predictAccess;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class CombineFileInput {
    public static class myCombineFileInputFormat<K,V> extends CombineFileInputFormat<K,V> {
        @Override
        public RecordReader<K,V> createRecordReader(InputSplit split, TaskAttemptContext context)throws IOException {
            return new CombineFileRecordReader((CombineFileSplit)split,context,CombineLineRecordReader.class);
        }
    }

    public static class CombineLineRecordReader<K,V> extends RecordReader<K,V>{
        private CombineFileSplit split;
        private TaskAttemptContext context;
        private int index;
        private RecordReader<K,V> rr;

        public CombineLineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
            this.index = index;
            this.split = (CombineFileSplit) split;
            this.context = context;

            this.rr = (RecordReader<K, V>) ReflectionUtils.newInstance(LineRecordReader.class, context.getConfiguration());
        }

        @Override
        public void initialize(InputSplit curSplit, TaskAttemptContext curContext) throws IOException, InterruptedException {
            this.split = (CombineFileSplit) curSplit;
            this.context = curContext;

            if (null == rr) {
                rr = ReflectionUtils.newInstance(SequenceFileRecordReader.class, context.getConfiguration());
            }

            FileSplit fileSplit = new FileSplit(this.split.getPath(index),
                    this.split.getOffset(index), this.split.getLength(index),
                    this.split.getLocations());

            this.rr.initialize(fileSplit, this.context);
        }

        @Override
        public void close()throws IOException{
            if (null != rr) {
                rr.close();
                rr = null;
            }
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return rr.getProgress();
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return rr.getCurrentKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return rr.getCurrentValue();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return rr.nextKeyValue();
        }
    }
}
