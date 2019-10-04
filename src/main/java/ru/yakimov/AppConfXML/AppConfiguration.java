package ru.yakimov.AppConfXML;


import org.apache.hadoop.fs.Path;
import ru.yakimov.JobConfXML.MysqlConfiguration;

import java.util.HashMap;
import java.util.Map;

public class AppConfiguration implements MySqlConfigMapHaver {

    private String hdfsHost;
    private String hdfsPort;

    private Map<String, MysqlConfiguration> mysqlConfigMap;

    private Path tempDir;
    private Path jobsDir;

    public AppConfiguration() {
        this.mysqlConfigMap = new HashMap<>();
    }

    public String getHdfsHost() {
        return hdfsHost;
    }

    public String getHdfsPort() {
        return hdfsPort;
    }

    public Path getTmpDir() {
        return tempDir;
    }

    public Path getJobsDir() {
        return jobsDir;
    }

    public MysqlConfiguration getMysqlConf(String target) {
        return mysqlConfigMap.get(target);
    }

    public void setHdfsHost(String hdfsHost) {
        this.hdfsHost = hdfsHost;
    }

    public void setHdfsPort(String hdfsPort) {
        this.hdfsPort = hdfsPort;
    }

    public void setTmpDir(String tempDir) {
        this.tempDir = new Path(tempDir);
    }

    public void setJobsDir(String jobsDir) {
        this.jobsDir = new Path(jobsDir);
    }

    @Override
    public void setMysqlConf(String target, MysqlConfiguration mysqlConfig) {
        this.mysqlConfigMap.put(target, mysqlConfig);
    }

    @Override
    public String toString() {
        return "AppConfiguration{" +
                "hdfsHost='" + hdfsHost + '\'' +
                ", hdfsPort='" + hdfsPort + '\'' +
                ", mysqlConfigMap=" + mysqlConfigMap.toString() +
                ", tempDir=" + tempDir.toString() +
                ", jobsDir=" + jobsDir.toString() +
                '}';
    }
}
