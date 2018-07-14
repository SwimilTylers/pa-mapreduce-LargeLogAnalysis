import DelayCounter.DelayCounter;
import IPCounter.IPCounter;
import StateCodeCounter.sccDriver;
import UrlVisitorCounter.uvcDriver;
import predictAccess.PredictionMain;

public class Main {
    public static void main(String[] args) throws Exception{

        if (args.length == 5){
            sccDriver task_one = new sccDriver();
            IPCounter task_two = new IPCounter();
            uvcDriver task_three = new uvcDriver();
            DelayCounter task_four = new DelayCounter();

            task_one.run(new String[]{args[0], args[1]});
            task_two.run(new String[]{args[0], args[2]});
            task_three.run(new String[]{args[0], args[3]});
            task_four.run(new String[]{args[0], args[4]});
        }
        else if (args.length == 2){
            PredictionMain.main(args);
        }
    }
}
