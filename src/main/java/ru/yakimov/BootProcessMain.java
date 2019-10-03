package ru.yakimov;

import ru.yakimov.Jobs.Job;
import ru.yakimov.Jobs.JobRunnable;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BootProcessMain {

    public static void main(String[] args) {
        ArrayList<Job> jobs = Assets.getInstance().getJobList();

        ExecutorService service = Executors.newCachedThreadPool();

        jobs.forEach(service::submit);

        service.shutdown();

        try {
            service.awaitTermination(10, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        jobs.forEach(BootProcessMain:: printResults);

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
