package predictAccess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Common {
    /**
     * @author eillon
     * @return ip time(hour) url
     * @param str full line from log
     *         -- 172.22.49.44 [08/Sep/2015:00:19:16 +0800] "GET /tour/category/query HTTP/1.1" GET 200 124 8
     */
    protected static List<String> getInfoFromLine(String str){
        List<String> ans = new ArrayList<>();
        String[] splits = str.split(" ");
        if(splits.length<5){
            System.out.println("ERRORINFO: "+str);
            return ans;
        }
        ans.add(splits[0]);
        String time = splits[1];
        String[] timeSplits = time.split(":");
        if(timeSplits.length<2){
            System.out.println("ERRORINFO: "+str);
            return ans;
        }
        ans.add(timeSplits[1]);
        ans.add(splits[4]);
        return ans;
    }

    /**
     * @author eillon
     * @return fileList
     * @param pathName folder
     */
    protected static List<String> getFiles(String pathName)throws Exception{
        List<String> ans = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(pathName),conf);
        RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path(pathName),true);
      //  FileStatus[] status = fs.listStatus(new Path(pathName));
        while(fileList.hasNext()){
            ans.add(fileList.next().getPath().toString());
        }
        fs.close();
        return ans;
    }

}
