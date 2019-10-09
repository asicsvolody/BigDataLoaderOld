/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.yakimov.Assets;
import ru.yakimov.JobContextConfiguration;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.config.RootJobConfiguration;

import java.util.*;
import java.util.concurrent.*;

public class RootJob implements Callable<Integer> {

    public static final Class CONTEXT_CLASS = JobContextConfiguration.class;


    private static final ApplicationContext context = new AnnotationConfigApplicationContext(CONTEXT_CLASS);

    private String rootJobName;

    private Map<Integer, ArrayList<Job>> jobsMap;

    public RootJob(RootJobConfiguration jobConfiguration) {
        this.rootJobName = jobConfiguration.getRootJobName();
        this.jobsMap = jobConfigsToTreeMap(jobConfiguration.getJobConfigurations());
    }

    @Override
    public Integer call() throws Exception {
        int workRes = 0;

        if(jobsMap.isEmpty()){
            return workRes;
        }

        ExecutorService service = Executors.newFixedThreadPool(5);

        for (Integer stage : jobsMap.keySet()) {

            List<Future<Integer>> futures = new ArrayList<>();

            for (Job job : jobsMap.get(stage)) {
                futures.add(service.submit(job));
            }

            service.awaitTermination(10, TimeUnit.HOURS);

            for (Future<Integer> future : futures) {
                workRes += future.get();
            }

            if(workRes != 0){
                break;
            }
        }

        service.shutdown();

        return workRes;
    }

    public Map<Integer, ArrayList<Job>> jobConfigsToTreeMap(List<JobConfiguration> jobConfigs){
        Map<Integer, ArrayList<Job>> resMap = new TreeMap<>();
        for (JobConfiguration jobConfig : jobConfigs) {
            int stage = jobConfig.getStage();
            Job job = null;
            try {
                job = (Job) context.getBean(Class.forName(jobConfig.getJobClass()));
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            job.setJobConfigs(jobConfig);
            if(!resMap.containsKey(stage)){
                resMap.put(stage, new ArrayList<>());
            }
            resMap.get(stage).add(job);
        }
        return resMap;
    }


    public String getRootJobName() {
        return rootJobName;
    }
}


