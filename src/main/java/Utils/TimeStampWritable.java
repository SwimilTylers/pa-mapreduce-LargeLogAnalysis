package Utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimeStampWritable implements WritableComparable<TimeStampWritable> {
    private boolean is_statistics;
    private int[] start_time = {0,0,0};
    private int[] end_time = {0,0,0};
    private String mark;

    public TimeStampWritable(){

    }

    public TimeStampWritable(String info, int[] start_time, int[] end_time){
        is_statistics = false;
        this.start_time = start_time;
        this.end_time = end_time;
        mark = info;
    }

    public TimeStampWritable(String info){
        is_statistics = true;
        mark = info;
    }

    public int compareTo(TimeStampWritable o) {
        if (is_statistics == o.is_statistics){
            if (start_time == o.start_time)
                return mark.compareTo(o.mark);
            else {
                if (start_time[0] == end_time[0])
                    if (start_time[1] == end_time[1])
                        if (start_time[2] == start_time[2])
                            return mark.compareTo(o.mark);
                        else
                            return Integer.compare(start_time[2], o.start_time[2]);
                    else
                        return Integer.compare(start_time[1], o.start_time[1]);
                else
                    return Integer.compare(start_time[0], o.start_time[0]);
            }
        }
        else
            return is_statistics ? -1 : 1;
    }

    public boolean isIs_statistics() {
        return is_statistics;
    }

    public int[] getStart_time() {
        return start_time;
    }

    public String getMark() {
        return mark;
    }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(is_statistics);
        out.writeInt(start_time[0]);
        out.writeInt(start_time[1]);
        out.writeInt(start_time[2]);
        out.writeUTF(mark);
    }

    public void readFields(DataInput in) throws IOException {
        is_statistics = in.readBoolean();
        start_time = new int[3];
        start_time[0] = in.readInt();
        start_time[1] = in.readInt();
        start_time[2] = in.readInt();
        mark = in.readUTF();
    }

    @Override
    public String toString(){
        if (is_statistics)
            return mark;
        else
            return String.format("%02d:00:00-%02d:00:00",
                    start_time[0], start_time[0]+1
            ) + mark;
    }
}
