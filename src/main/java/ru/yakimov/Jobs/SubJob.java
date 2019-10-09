/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import ru.yakimov.config.RootJobConfiguration;

import java.util.concurrent.Callable;


public abstract class SubJob implements Callable<Integer> {

    private final int defaultRes = -1;

    private RootJobConfiguration jobConfig;
    private int jobResult = defaultRes;

    public RootJobConfiguration getJobConfig() {
        return jobConfig;
    }
    public void setJobConfig(RootJobConfiguration jobConfig) {
        this.jobConfig = jobConfig;
    }

    public void setJobResult(int jobResult) {
        this.jobResult = jobResult;
    }

    public int getJobResult() {
        return jobResult;
    }
}
