package ru.yakimov.HDFSConfXML;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class HDFSConfiguration {
    private String host;
    private String port;
    private Path tempDir;
    private Path jobsDir;

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public Path getTempDir() {
        return tempDir;
    }

    public Path getJobsDir() {
        return jobsDir;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setTempDir(String tempDir) {
        this.tempDir = new Path(tempDir);
    }

    public void setJobsDir(String jobsDir) {
        this.jobsDir = new Path(jobsDir);
    }

    @Override
    public String toString() {
        return "HDFSConfiguration{" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", tempDir=" + tempDir +
                ", jobsDir=" + jobsDir +
                '}';
    }
}
