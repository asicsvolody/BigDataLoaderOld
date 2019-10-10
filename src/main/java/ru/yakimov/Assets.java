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
import ru.yakimov.config.JobXmlLoader;
import ru.yakimov.logDb.Log;
import ru.yakimov.logDb.MySqlDb;

import javax.xml.stream.XMLStreamException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;


public class Assets {

    public static final String SEPARATOR = "/";

    private final String CONF_FILE_PATH = "conf.xml";

    public static final String MAIN_PROS = JobXmlLoader.createNameWithData("SYSTEM_PROSES");


    private AppConfiguration conf;

    private final SparkSession spark;

    private final FileSystem fs;

    private final Runtime rt;


    private static Assets instance;


    public static Assets getInstance() throws XMLStreamException, IOException, SQLException {
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


    private Assets() throws IOException, XMLStreamException, SQLException {


        this.conf = AppXmlLoader.readConfigApp(CONF_FILE_PATH);



        try {
            MySqlDb.initConnection(conf.getLogDbConfig());
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Exception connection to log database");
        }

        Log.writeRoot(MAIN_PROS, "System configuration have redden");

        Log.writeRoot(MAIN_PROS, "Log dataBase have connected");



        this.rt = Runtime.getRuntime();

        Log.writeRoot(MAIN_PROS, "System Runtime have gotten ");


        SparkContext context = new SparkContext(
                new SparkConf().setAppName("spark-App").setMaster("local[*]")
                .set("spark.hadoop.fs.default.name", String.format("hdfs://%s:%s", conf.getHdfsHost(), conf.getHdfsPort()))
                .set("spark.hadoop.fs.defaultFS", String.format("hdfs://%s:%s",conf.getHdfsHost(), conf.getHdfsPort()))
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));

        context.setLogLevel("WARN");

        this.spark = SparkSession.builder().sparkContext(context).getOrCreate();

        Log.writeRoot(MAIN_PROS, "Spark have configured ");

        try {
            this.fs = FileSystem.get(context.hadoopConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException("Exception connection to Hadoop file system");
        }

        Log.writeRoot(MAIN_PROS, "Hadoop file system have gotten ");


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
