/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.config.RootJobConfiguration;

import java.util.ArrayList;
import java.util.concurrent.Callable;

public abstract class Job implements Callable<Integer> {

    JobConfiguration jobConfigs;

    public void setJobConfigs(JobConfiguration jobConfigs) {
        this.jobConfigs = jobConfigs;
    }
}

