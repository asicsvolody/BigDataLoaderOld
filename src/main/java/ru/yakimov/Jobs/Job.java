package ru.yakimov.Jobs;

import ru.yakimov.JobConfXML.JobConfiguration;


public abstract class Job extends Thread {

    private final int defaultRes = -1;

    private JobConfiguration jobConfig;
    private int jobResult = defaultRes;

    public JobConfiguration getJobConfig() {
        return jobConfig;
    }
    public void setJobConfig(JobConfiguration jobConfig) {
        this.jobConfig = jobConfig;
    }

    public void setJobResult(int jobResult) {
        this.jobResult = jobResult;
    }

    public int getJobResult() {
        return jobResult;
    }
}
