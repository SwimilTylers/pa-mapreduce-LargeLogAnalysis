package Utils;

import com.sun.istack.NotNull;

import java.util.Scanner;

public class LogEntryParser {
    private String ip;
    private String time;
    private String http_request;
    private String http_method;
    private int state_code;
    private int response_length;
    private int delay;

    public LogEntryParser(String line){
        ip = line.substring(0, line.indexOf(" "));
        time = line.substring(line.indexOf("["), line.indexOf("]")+1);
        String[] splits = line.split("\"");
        http_request = splits[1];
        line = splits[2];
        splits = line.split(" ");
        http_method = splits[1];
        state_code = Integer.parseInt(splits[2]);
        response_length = Integer.parseInt(splits[3]);
        delay = Integer.parseInt(splits[4]);
    }

    public int[] getTimeSplits(){
        String parse_time = time.substring(time.indexOf(":")+1, time.indexOf(" "));
        int[] time_splits = new int[]{0,0,0};
        String[] buf = parse_time.split(":");
        for (int i = 0; i < time_splits.length; i++) {
            time_splits[i] = Integer.parseInt(buf[i]);
        }
        return time_splits;
    }

    public int getState_code() {
        return state_code;
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("line: ");
        LogEntryParser parser = new LogEntryParser(scanner.nextLine());
        int[] time = parser.getTimeSplits();
        System.out.println("hour: "+ String.format("%02d", time[0]) + " minute: " + time[1] + " second: " + time[2]);
        System.out.println("state code "+parser.getState_code());
    }
}
