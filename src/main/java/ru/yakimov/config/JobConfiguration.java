/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class JobConfiguration implements MySqlConfigMapHaver {

    private String jobClassName;

    private String jobIdentifier;

    private String jobTmpDir;

    private String jobDirTo;

    private ArrayList<String> partitionCols;



    private String jobFile;


    private Map <String , MysqlConfiguration> mysqlConfMap;

    public JobConfiguration() {
        this.partitionCols = new ArrayList<>();
        this.mysqlConfMap = new HashMap<>();
    }

    public void setJobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
    }

    public void setJobDirTo(String jobDirTo) {
        this.jobDirTo = jobDirTo;
    }

    public void setPartition(String partitionCol) {
        this.partitionCols.add(partitionCol);
    }

    @Override
    public void setMysqlConf(String target, MysqlConfiguration mysqlConf) {
        this.mysqlConfMap.put(target,mysqlConf);
    }

    public void setJobIdentifier(String jobIdentifier) {
        this.jobIdentifier = jobIdentifier;
    }

    public void setJobTmpDir(String jobTmpDir) {
        this.jobTmpDir = jobTmpDir;
    }

    public String getJobClassName() {
        return jobClassName;
    }

    public String getJobFile() {
        return jobFile;
    }

    public String getJobDirTo() {
        return jobDirTo;
    }

    public String getJobIdentifier() {
        return jobIdentifier;
    }

    public void setJobFile(String jobFile) {
        this.jobFile = jobFile;
    }

    public String[] getPartitionCols() {
        return partitionCols.toArray(new String[0]);
    }

    public MysqlConfiguration getMysqlConf(String target) {
        return mysqlConfMap.get(target);
    }

    public String getJobTmpDir() {
        return jobTmpDir;
    }

    @Override
    public String toString() {
        return "JobConfiguration{" +
                "jobClassName='" + jobClassName + '\'' +
                ", jobDirTo=" + jobDirTo.toString() +
                ", partitionCols=" + partitionCols.toString() +
                ", mysqlConfMap=" + mysqlConfMap.toString() +
                '}';
    }

}
