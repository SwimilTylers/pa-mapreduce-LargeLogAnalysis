package predictAccess;

import Jama.Matrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
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

    protected static void rumCmd(String cmd)throws Exception{
        Process process = Runtime.getRuntime().exec(cmd);
        System.out.println(cmd);
        BufferedReader strCon = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line=strCon.readLine())!=null){
            System.out.println(line);
        }
    }

    /**
     * 最小二乘拟合， y=ax+b
     * @param timesOfDate
     * @return
     */
    protected static int preByLinearFit(double[] timesOfDate){
        int a00=0,a01=0,a11=0;
        int y0=0,y1=0;

        // 系数矩阵
        for(int i=13;i>=0;i--){
            if(timesOfDate[i]==0) continue;
            a00+=1;
            a01+=i;
            a11+=i*i;
            y0+=timesOfDate[i];
            y1+=timesOfDate[i]*i;
        }

        // 求解
        double a,b;
        if((a01*a01-a00*a11)==0|| a00 ==0){
            b=0;a=timesOfDate[13];
        }
        else {
            b = (a01 * y0 - a00 * y1) / (a01 * a01 - a00 * a11);
            a = (y0 - a01 * b) / a00;
        }

        int ans = (int)(14*b+a);
        if(ans<0) ans =0;
        return  ans;
    }

    /**
     * 最小二乘拟合， y=ax^2+bx+c
     * @param timesOfDate
     * @return
     */
    protected static int preByQuadraticFit(double[] timesOfDate){
        int a00=0,a01=0, a02=0, a11=0, a12=0, a22=0;
        int y0=0,y1=0,y2=0;

        // 系数矩阵
        for(int i=13;i>=0;i--){
            if(timesOfDate[i]==0) continue;
            a00+=1;
            a01+=i;
            a02+=i*i;
            a11+=i*i;
            a12+=i*i*i;
            a22+=i*i*i*i;
            y0+=timesOfDate[i];
            y1+=timesOfDate[i]*i;
            y2+=timesOfDate[i]*i*i;
        }

        // 求解
        double a=0,b=0,c=0;
        double aa[][] ={{a00,a01,a02},{a01,a11,a12},{a02,a12,a22}};
        double bb[][] = {{y0,0,0},{y1,0,0},{y2,0,0}};
        Matrix A = new Matrix(aa);
        Matrix B = new Matrix(bb);
        try {
            Matrix C = A.inverse().times(B);
            c = C.get(0, 0);
            b = C.get(1, 0);
            a = C.get(2, 0);
        }catch (Exception e){
            a = 0; b= 0;c=0;
        }
        int ans = (int)(14*14*a+14*b+c);
        if(ans<=0) ans =(int)timesOfDate[14];
        return  ans;
    }

    /**
     * 均值
     * @param timesOfDate
     * @return
     */
    protected static int preByMean(double[] timesOfDate){
        int mean = 0;
        int n = 0;  // 因为可能只以后几个计数，所以设置n值
        for(int i=13;i>=10;i--){
            mean+=timesOfDate[i];
            n++;
        }
        mean/=n;

        int ans = (int)(mean);
        if(ans<0) ans =0;
        return  ans;
    }



}
