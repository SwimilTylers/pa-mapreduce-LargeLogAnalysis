package predictAccess;

import java.util.ArrayList;
import java.util.List;

public class Common {
    /**
     * @author eillon
     * @return ip time(hour) url
     * @param str full line from log
     *         -- 172.22.49.44 [08/Sep/2015:00:19:16 +0800] "GET /tour/category/query HTTP/1.1" GET 200 124 8
     */
    public static List<String> getInfoFromLine(String str){
        List<String> ans = new ArrayList<>();
        String[] splits = str.split(" ");
        ans.add(splits[0]);
        String time = splits[1];
        String[] timeSplits = time.split(":");
        ans.add(timeSplits[1]);
        ans.add(splits[4]);
        return ans;
    }


}
