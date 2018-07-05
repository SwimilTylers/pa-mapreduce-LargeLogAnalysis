package Utils;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TextMultipleOutputs<K,V> extends MultipleOutputs<K,V> {
    /**
     * Creates and initializes multiple outputs support,
     * it should be instantiated in the Mapper/Reducer setup method.
     *
     * @param context the TaskInputOutputContext object
     */
    public TextMultipleOutputs(TaskInputOutputContext context) {
        super(context);
    }


}
