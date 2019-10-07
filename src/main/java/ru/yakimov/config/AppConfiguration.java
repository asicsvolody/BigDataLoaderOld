/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.config;

import java.util.HashMap;
import java.util.Map;

public class AppConfiguration implements MySqlConfigMapHaver {

    private String hdfsHost;
    private String hdfsPort;

    private Map<String, MysqlConfiguration> mysqlConfigMap;

    private String tempDir;
    private String jobsDir;
    private String logsDir;

    public AppConfiguration() {
        this.mysqlConfigMap = new HashMap<>();
    }

    public String getHdfsHost() {
        return hdfsHost;
    }

    public String getHdfsPort() {
        return hdfsPort;
    }

    public String getTmpDir() {
        return tempDir;
    }

    public String getJobsDir() {
        return jobsDir;
    }

    public String getLogsDir() {
        return logsDir;
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
        this.tempDir = tempDir;
    }

    public void setJobsDir(String jobsDir) {
        this.jobsDir = jobsDir;
    }

    public void setLogsDir(String logsDir) {
        this.logsDir = logsDir;
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
