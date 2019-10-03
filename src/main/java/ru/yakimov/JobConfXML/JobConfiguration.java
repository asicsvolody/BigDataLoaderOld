package ru.yakimov.JobConfXML;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JobConfiguration {

    private String jobClassName;

    private Path hdfsDirTo;

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

    public void setHdfsDirTo(String hdfsDirTo) {
        this.hdfsDirTo = new Path(hdfsDirTo);
    }

    public void setPartition(String partitionCol) {
        this.partitionCols.add(partitionCol);
    }

    public void setMysqlConf(String target, MysqlConfiguration mysqlConf) {
        this.mysqlConfMap.put(target,mysqlConf);
    }

    public String getJobClassName() {
        return jobClassName;
    }
    public String getJobFile() {
        return jobFile;
    }

    public Path getHdfsDirTo() {
        return hdfsDirTo;
    }

    public void setJobFile(String jobFile) {
        this.jobFile = jobFile;
    }

    public Iterator<String> getPartitionCols() {
        return partitionCols.iterator();
    }

    public MysqlConfiguration getMysqlConf(String target) {
        return mysqlConfMap.get(target);
    }

    @Override
    public String toString() {
        return "JobConfiguration{" +
                "jobClassName='" + jobClassName + '\'' +
                ", hdfsDirTo=" + hdfsDirTo.toString() +
                ", partitionCols=" + partitionCols.toString() +
                ", mysqlConfMap=" + mysqlConfMap.toString() +
                '}';
    }
}
