/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import ru.yakimov.config.AppConfiguration;

import ru.yakimov.config.AppXmlLoader;
import ru.yakimov.db.MySqlDb;

import java.io.IOException;


public class Assets {

    public static final String SEPARATOR = "/";

    private final String CONF_FILE_PATH = "conf.xml";


    private AppConfiguration conf;

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


    private Assets() throws Exception {


        this.conf = AppXmlLoader.readConfigApp(CONF_FILE_PATH);


        try {
            MySqlDb.initConnection(conf.getLogDbConfig());
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Exception connection to log databese");
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

    public static void closeResources(){

        MySqlDb.closeConnection();

        instance = null;
    }

}
