package MainDriver;
import DelayCounter.DelayCounter;
import IPCounter.IPCounter;
import StateCodeCounter.sccDriver;
import UrlVisitorCounter.uvcDriver;
public class MainDriver {
    public static void main(String[] args) throws Exception{
        String []args1=new String[2];
        String []args2=new String[2];
        String []args3=new String[2];
        String []args4=new String[2];
        args1[0]=args[0];args1[1]=args[1];
        args2[0]=args[0];args2[1]=args[2];
        args3[0]=args[0];args3[1]=args[3];
        args4[0]=args[0];args4[1]=args[4];
        sccDriver driver1=new sccDriver();
        IPCounter driver2=new IPCounter();
        uvcDriver driver3=new uvcDriver();
        DelayCounter driver4 =new DelayCounter();
        driver1.run(args1);
        driver2.run(args2);
        driver3.run(args3);
        driver4.run(args4);
        //System.exit(driver1.run(args));

    }
}
