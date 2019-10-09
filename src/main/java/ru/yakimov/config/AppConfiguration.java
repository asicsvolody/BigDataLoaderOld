/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.config;

public class AppConfiguration {

    private String hdfsHost;
    private String hdfsPort;

    private DBConfiguration logDbConfig;

    private String tempDir;
    private String jobsDir;
    private String logsDir;


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

    public DBConfiguration getLogDbConfig() {
        return logDbConfig;
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

    public void setLogDbConfig(DBConfiguration logDbConfig) {
        this.logDbConfig = logDbConfig;
    }


}
