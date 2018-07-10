package predictAccess;

import Jama.Matrix;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.xerces.impl.xpath.XPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class URLPredict {
    public static class myMapper extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value 形式：hour--date \t url:times;url:times;
            String[] tmp=value.toString().split("\t");
            String[] time=tmp[0].split("--");
            String hour=time[0],date=time[1];
            String[] urlTimes=tmp[1].split(";");
            for(String urltime:urlTimes){
                String[] t=urltime.split(":");
                String url=t[0],times=t[1];
                Text ourKey = new Text(hour+"#"+url+"#"+date);
                context.write(ourKey,new Text(times));
            }
        }
    }

    /**
     * 处理得到 RMSE
     */
    public static class finalMapper extends Mapper<Text,Text,Text,Text>{
        double RMSE = 0.0;
        int hourNum = 0;

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String evlString = key.toString().split("#")[1];
            hourNum++;
            RMSE+=Double.parseDouble(evlString);
            context.write(key,value);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            RMSE=RMSE/hourNum;
            System.out.println(RMSE);
            context.write(new Text("RMSE"),new Text(RMSE+""));
        }
    }

    public static class myPartitioner extends HashPartitioner<Text,Text>{
        @Override
        public int getPartition(Text key,Text value,int numReduceTasks){
            String hour = key.toString().split("#")[0];
            return super.getPartition(new Text(hour),value,numReduceTasks);
        }
    }

    public static class myReducer extends Reducer<Text,Text,Text,Text>{
        private String currentHour = " ";
        private String currentURL = " ";
        private List<String> urlList = new ArrayList<>();
        private List<String> dateList = new ArrayList<>();
        private double evl = 0.0;
        private int urlNum = 0;
        private double RMSE = 0.0;
        private int hourNum = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keys=key.toString().split("#");
            String url=keys[1],hour=keys[0],date=keys[2];

         //   if(date.equals("22")) return;

            String times = "";
            for(Text val:values) times+=val.toString();

            if(!currentHour.equals(hour)&&!currentHour.equals(" ")){
                // 处理最后一个URL
                int predictTimes = predictNext(dateList);
                urlList.add(currentURL+":"+predictTimes);

                // 评估
                int size = dateList.size();
                String lastDate = dateList.get(size-1);
                String realTimesString = lastDate.split(":")[1];
                int realTimes = Integer.parseInt(realTimesString);
                urlNum++;
                evl+=(realTimes-predictTimes)*(realTimes-predictTimes);

                dateList = new ArrayList<>();

                // 输出
                StringBuilder out = new StringBuilder();
                for(String p:urlList){
                    out.append(p); out.append(";");
                }

                evl=Math.sqrt(evl/urlNum);
                if(Double.isNaN(evl)) evl=0;
                RMSE+=evl;
                hourNum++;

                String evlString=String.format("%.2f",evl);
                context.write(new Text(currentHour+"#"+evlString),new Text(out.toString()));

                evl=0;urlNum=0;
                urlList = new ArrayList<>();
            }
            else if(!currentURL.equals(url)&&!currentURL.equals(" ")){
                // 预测
                int predictTimes = predictNext(dateList);
                urlList.add(currentURL+":"+predictTimes);

                // 评估
                int size = dateList.size();
                String lastDate = dateList.get(size-1);
                int realDate = Integer.parseInt(lastDate.split(":")[0]);
                int realTimes;
                if(realDate==22) {
                    String realTimesString = lastDate.split(":")[1];
                    realTimes = Integer.parseInt(realTimesString);
                }else{
                    realTimes=0;
                }
                urlNum++;
                evl+=(realTimes-predictTimes)*(realTimes-predictTimes);

                dateList = new ArrayList<>();
            }

            // 更新 dateList： date:times;date:times;
            currentHour = hour;
            currentURL = url;
            dateList.add(date+":"+times);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 处理url
            // 预测
            int predictTimes = predictNext(dateList);
            urlList.add(currentURL+":"+predictTimes);

            // 评估
            int size = dateList.size();
            String lastDate = dateList.get(size-1);
            int realDate = Integer.parseInt(lastDate.split(":")[0]);
            int realTimes;
            if(realDate==22) {
                String realTimesString = lastDate.split(":")[1];
                realTimes = Integer.parseInt(realTimesString);
            }else{
                realTimes=0;
            }
            urlNum++;
            evl+=(realTimes-predictTimes)*(realTimes-predictTimes);

            dateList = new ArrayList<>();

            // 处理hour
            StringBuilder out = new StringBuilder();
            for(String p:urlList){
                out.append(p); out.append(";");
            }

            evl=Math.sqrt(evl/urlNum);
            if(Double.isNaN(evl)) evl=0;
            RMSE+=evl;
            hourNum++;

            String evlString=String.format("%.2f",evl);
            context.write(new Text(currentHour+"#"+evlString),new Text(out.toString()));

            String RMSEString = String.format("%.2f",RMSE/hourNum);
            System.out.println("RMSE: "+RMSEString);
            context.write(new Text("RMSE"),new Text(RMSEString));

            urlList = new ArrayList<>();
        }

        // 根据dateList预测times   date:times;date:times;date:times
        private int predictNext(List<String> dateList){
            // 最小二乘拟合， y=ax+b
            int[] timesOfDate = new int[15];
            for(int i=0;i<15;i++) timesOfDate[i]=0;

            for(String datelist:dateList){
                String[] tmp = datelist.split(":");
                int date = Integer.parseInt(tmp[0]);
                timesOfDate[date-8]=Integer.parseInt(tmp[1]);
            }

            int a00=0,a01=0,a11=0;
            int y0=0,y1=0;

            for(int i=13;i>=0;i--){
                if(timesOfDate[i]==0) continue;
                a00+=1;
                a01+=i;
                a11+=i*i;
                y0+=timesOfDate[i];
                y1+=timesOfDate[i]*i;
            }

            System.out.println(a00+" "+a01+" "+a11+" "+y0+" "+y1);

          //  double[][] a={{a00,a01},{a01,a11}};
          //  double[] b = {y0,y1};

           // Matrix A = new Matrix(a);
           // Matrix B = new Matrix(b);

            double a,b;
            if((a01*a01-a00*a11)==0|| a00 ==0){
                b=0;a=timesOfDate[13];
            }
            else {
                b = (a01 * y0 - a00 * y1) / (a01 * a01 - a00 * a11);
                a = (y0 - a01 * b) / a00;
            }
         //   b=0;a=timesOfDate[14];
            int ans = (int)(14*b+a);
            if(ans<0) ans =0;
            return ans;
        }
    }


}
