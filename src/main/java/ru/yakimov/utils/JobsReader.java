package ru.yakimov.utils;


import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.yakimov.Jobs.Job;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.config.ConfigXmlLoader;

import java.io.File;
import java.util.ArrayList;

public class JobsReader {

    private ApplicationContext context;


    public JobsReader(Class contextClass) {

        context = new AnnotationConfigApplicationContext(contextClass);

    }

    public ArrayList<Job> getJobs(String jobsDir) {
        ArrayList<Job> resJob = new ArrayList<>();
        ArrayList<JobConfiguration> jobConfigs = new ArrayList<>();

        String[] jobFiles = new File(jobsDir).list();

        if(jobFiles == null)
            return null;


        for (String file : jobFiles) {
            if(file.endsWith(".jxml")){
                jobConfigs.add(ConfigXmlLoader.readConfJob(jobsDir+"/"+file));
            }
        }

        for (JobConfiguration jobConfig : jobConfigs) {
            resJob.add(getJobForConf(jobConfig));
        }
        return resJob;
    }

    private Job getJobForConf(JobConfiguration config){


        Job job = null;
        try {
            job = (Job) context.getBean(Class.forName(config.getJobClassName()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        assert job != null;
        job.setJobConfig(config);
        return job;
    }

}
