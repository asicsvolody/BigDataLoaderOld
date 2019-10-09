/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.config;

import java.util.ArrayList;

public class JobConfiguration{

    private String rootJobName;
    private String jobName;
    private String jobClass;
    private int stage;
    private String dirFrom;
    private String dirTo;
    private ArrayList<String> partitions;
    private DBConfiguration dbConfiguration;

    public JobConfiguration(String rootJobName) {
        this.rootJobName = rootJobName;
    }

    public String getRootJobName() {
        return rootJobName;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobClass() {
        return jobClass;
    }

    public int getStage() {
        return stage;
    }

    public String getDirFrom() {
        return dirFrom;
    }

    public String getDirTo() {
        return dirTo;
    }

    public String[] getPartitions() {
        return partitions.toArray(new String[0]);
    }

    public DBConfiguration getDbConfiguration() {
        return dbConfiguration;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setJobClass(String jobClass) {
        this.jobClass = jobClass;
    }

    public void setStage(int stage) {
        this.stage = stage;
    }

    public void setDirFrom(String dirFrom) {
        this.dirFrom = dirFrom;
    }

    public void setDirTo(String dirTo) {
        this.dirTo = dirTo;
    }

    public void addPartitions(String partition) {
        if(partitions == null)
            partitions = new ArrayList<>();
        this.partitions.add(partition);
    }

    public void setDbConfiguration(DBConfiguration dbConfiguration) {
        this.dbConfiguration = dbConfiguration;
    }

}
