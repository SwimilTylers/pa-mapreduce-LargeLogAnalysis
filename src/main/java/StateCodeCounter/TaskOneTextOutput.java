package StateCodeCounter;

import Utils.CustomFileNameTextOutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TaskOneTextOutput extends CustomFileNameTextOutputFormat {

    @Override
    protected String getFileName(TaskAttemptContext job) {
        return job.getConfiguration().get(OUTDIR)+"/1.txt";
    }
}
