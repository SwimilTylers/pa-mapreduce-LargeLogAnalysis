package StateCodeCounter;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeStampWritable implements WritableComparable<TimeStampWritable> {
    private boolean is_statistics;
    private int start_time;
    private String mark;

    public TimeStampWritable(String info, int time){
        is_statistics = false;
        start_time = time;
        mark = info;
    }

    public TimeStampWritable(String info){
        is_statistics = true;
        start_time = -1;
        mark = info;
    }

    public int compareTo(TimeStampWritable o) {
        if (is_statistics == o.is_statistics){
            if (start_time == o.start_time)
                return mark.compareTo(o.mark);
            else
                return Integer.compare(start_time, o.start_time);
        }
        else
            return is_statistics ? -1 : 1;
    }

    public boolean isIs_statistics() {
        return is_statistics;
    }

    public int getStart_time() {
        return start_time;
    }

    public String getMark() {
        return mark;
    }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(is_statistics);
        out.writeInt(start_time);
        out.writeUTF(mark);
    }

    public void readFields(DataInput in) throws IOException {
        is_statistics = in.readBoolean();
        start_time = in.readInt();
        mark = in.readUTF();
    }

    @Override
    public String toString(){
        int print_time = start_time;
        if (start_time == 0)
            print_time = 12;
        if (is_statistics)
            return mark;
        else
            return String.format("02%d:00:00-02%d:00:00 ", print_time, print_time+1)+mark;
    }
}
