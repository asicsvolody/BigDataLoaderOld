package ru.yakimov;

import ru.yakimov.Jobs.Job;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BootProcessMain {

    public BootProcessMain() {
        ArrayList<Job> jobs = null;
        try {
            jobs = Assets.getInstance().getJobList();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(jobs == null){
            System.out.println("Jobs not found");
            return;
        }

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

        try {
            Assets.getInstance().closeResources();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printResults(Job job){
        StringBuilder str = new StringBuilder(job.getJobConfig().getJobIdentifier());
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

    public static void main(String[] args) {
        new BootProcessMain();
    }




}
