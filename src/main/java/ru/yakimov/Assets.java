package ru.yakimov;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import ru.yakimov.HDFSConfXML.HDFSConfXmlLoader;
import ru.yakimov.HDFSConfXML.HDFSConfiguration;
import ru.yakimov.JobConfXML.JobsReader;
import ru.yakimov.Jobs.Job;

import java.io.IOException;
import java.util.ArrayList;

public class Assets {

    private ArrayList<Job> jobList;


    private HDFSConfiguration conf;


    private final String CONF_FILE_PATH = "conf.xml";
    private final String JOBS_DIR = "./jobs";
    private final Class contextClass = JobContextConfiguration.class;

    private Logger prosLogger;

    private final SparkSession spark;

    private final FileSystem fs;

    private final Runtime rt;


    private static Assets instance;


    public static Assets getInstance(){
        Assets localInstance = instance;
        if(localInstance == null){
            synchronized (Assets.class){
                localInstance = instance;
                if(localInstance == null){
                    try {
                        localInstance = instance = new Assets();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return  localInstance;
    }

    public ArrayList<Job> getJobList() {
        return jobList;
    }

    private Assets() throws IOException {


        this.prosLogger = Logger.getLogger("prosLogger");


        this.conf = HDFSConfXmlLoader.readConfig(CONF_FILE_PATH);


        this.rt = Runtime.getRuntime();

        SparkContext context = new SparkContext(
                new SparkConf().setAppName("spark-App").setMaster("local[*]")
                .set("spark.hadoop.fs.default.name", String.format("hdfs://%s:%s", conf.getHost(), conf.getPort()))
                .set("spark.hadoop.fs.defaultFS", String.format("hdfs://%s:%s",conf.getHost(), conf.getPort()))
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));

        context.setLogLevel("WARN");

        this.spark = SparkSession.builder().sparkContext(context).getOrCreate();

        this.fs = FileSystem.get(context.hadoopConfiguration());

        this.jobList = new JobsReader(contextClass).getJobs(JOBS_DIR);

    }

    public HDFSConfiguration getConf() {
        return conf;
    }

    public Logger getProsLogger() {
        return prosLogger;
    }

    public SparkSession getSpark() {
        return spark;
    }

    public FileSystem getFs() {
        return fs;
    }

    public Runtime getRt() {
        return rt;
    }
}
