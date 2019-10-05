package ru.yakimov;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import ru.yakimov.config.AppConfiguration;
import ru.yakimov.utils.JobsReader;
import ru.yakimov.Jobs.Job;
import ru.yakimov.db.MySqlDb;
import ru.yakimov.config.ConfigXmlLoader;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class Assets {


    private ArrayList<Job> jobList;


    private AppConfiguration conf;

    public static final String SEPARATOR = "/";

    private final String CONF_FILE_PATH = "conf.xml";

    private final Class CONTEXT_CLASS = JobContextConfiguration.class;

    private final SparkSession spark;

    private final FileSystem fs;

    private final Runtime rt;


    private static Assets instance;


    public static Assets getInstance() throws Exception {
        Assets localInstance = instance;
        if(localInstance == null){
            synchronized (Assets.class){
                localInstance = instance;
                if(localInstance == null){
                    localInstance = instance = new Assets();
                }
            }
        }
        return  localInstance;
    }

    public ArrayList<Job> getJobList() {
        if(jobList == null){
            jobList = new JobsReader(CONTEXT_CLASS).getJobs(conf.getJobsDir().toString());
        }
        return jobList;
    }

    private Assets() throws Exception {


        this.conf = ConfigXmlLoader.readConfigApp(CONF_FILE_PATH);


        try {
            MySqlDb.initConnection(conf.getMysqlConf("LogDataBase"));
        } catch (Exception e) {
            throw new Exception("Exception connection to LogDataBase");
        }


        this.rt = Runtime.getRuntime();

        SparkContext context = new SparkContext(
                new SparkConf().setAppName("spark-App").setMaster("local[*]")
                .set("spark.hadoop.fs.default.name", String.format("hdfs://%s:%s", conf.getHdfsHost(), conf.getHdfsPort()))
                .set("spark.hadoop.fs.defaultFS", String.format("hdfs://%s:%s",conf.getHdfsHost(), conf.getHdfsPort()))
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));

        context.setLogLevel("WARN");

        this.spark = SparkSession.builder().sparkContext(context).getOrCreate();

        try {
            this.fs = FileSystem.get(context.hadoopConfiguration());
        } catch (IOException e) {
            throw new IOException("Exception connection to Hadoop file system");
        }

    }

    public AppConfiguration getConf() {
        return conf;
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

    public void closeResources(){
        try {
            MySqlDb.closeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        rt.exit(0);
        spark.close();
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        instance = null;

    }
}
