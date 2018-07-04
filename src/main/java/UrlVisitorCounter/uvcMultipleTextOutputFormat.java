package UrlVisitorCounter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class uvcMultipleTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {
    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        return key.toString();
    }
}
