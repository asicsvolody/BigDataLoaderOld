package ru.yakimov;

import com.sun.xml.internal.ws.api.FeatureListValidatorAnnotation;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import ru.yakimov.Jobs.Job;
import ru.yakimov.Jobs.JobRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BootProcessMain {

    public static void main(String[] args) throws InterruptedException {
        ArrayList<Callable<Integer>> jobs = Assets.getInstance().getJobList();

        ExecutorService service = Executors.newCachedThreadPool();
        List<Future<Integer>> featureList = service.invokeAll(jobs);

        jobs.forEach(service::submit);

        service.shutdown();

        service.awaitTermination(10, TimeUnit.HOURS);

        featureList.forEach(printResults(););


//        jobs.forEach(BootProcessMain:: printResults);

        System.out.println("BootProsesMain has finished.");
    }



    private static void printResults(Job job){
        StringBuilder str = new StringBuilder(job.getJobConfig().getJobFile());
        switch (job.getJobResult()){
            case 0:
                str.append(" completed successfully.");
                break;
            case 1:
                str.append(" completed with error");
                break;
        }

        System.out.println(str.toString());
    }




}
