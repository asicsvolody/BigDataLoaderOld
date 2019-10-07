/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.yakimov.Assets;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.config.MysqlConfiguration;
import ru.yakimov.db.Log;
import sun.jvm.hotspot.tools.PStack;

import java.io.*;

public class SqoopIE {
    private final static String PASSWORD_FILE_PATH_DIR = "./";
    private final static Path PASSWORD_HADOOP_PATH_DIR = new Path("/user/password");



    public static int importDbToTmp(JobConfiguration jobConfig, String targetDB) throws Exception {

        Log.write(jobConfig.getJobIdentifier(), "Sqoop get task import");


        MysqlConfiguration mysqlConfig = jobConfig.getMysqlConf(targetDB);

        Log.write(jobConfig.getJobIdentifier(), "Start creating password file");

        createPassword(mysqlConfig.getPassword(), jobConfig.getJobIdentifier());


        HdfsUtils.deleteDirWithLog(jobConfig.getJobIdentifier(), jobConfig.getJobTmpDir());

        Log.write(jobConfig.getJobIdentifier(), "Sqoop run process");


        Process process = Assets.getInstance().getRt().exec(String.format("sqoop import " +
                "--connect jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL " +
                "--username %s " +
                "--password-file %s " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by %s " +
                "--as-parquetfile"
                ,mysqlConfig.getHost()
                ,mysqlConfig.getPort()
                ,mysqlConfig.getSchema()
                ,mysqlConfig.getUser()
                ,getHadoopPasswordPath(jobConfig.getJobIdentifier())
                ,mysqlConfig.getTable()
                ,jobConfig.getJobTmpDir()
                ,mysqlConfig.getPrimaryKeys().next()
        ));

        Log.write(jobConfig.getJobIdentifier(), "Waiting of end sqoop process");

        process.waitFor();

        printProcessMessageStream(process, jobConfig.getJobIdentifier());

        writeResSqoop(jobConfig, process.exitValue());

        deletePassword(jobConfig);

        return process.exitValue();
    }

    private static void exportTmpToDb(JobConfiguration jobConfig, String targetDB) throws Exception {
        Log.write(jobConfig.getJobIdentifier(), "Sqoop get task export");

        MysqlConfiguration mysqlConfig = jobConfig.getMysqlConf(targetDB);

        Log.write(jobConfig.getJobIdentifier(), "Start creating password file");

        createPassword(mysqlConfig.getPassword(), jobConfig.getJobIdentifier());



        Process process = Assets.getInstance().getRt().exec(String.format("sqoop export " +
                "--connect \"jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username %s " +
                "--password-file %s " +
                "--table %s " +
                "--export-dir %s " +
                "--validate"
                ,mysqlConfig.getHost()
                ,mysqlConfig.getPort()
                ,mysqlConfig.getSchema()
                ,mysqlConfig.getUser()
                ,getHadoopPasswordPath(jobConfig.getJobIdentifier())
                ,mysqlConfig.getTable()
                ,jobConfig.getJobTmpDir())
        );

        Log.write(jobConfig.getJobIdentifier(), "Waiting of end sqoop process");

        process.waitFor();

        printProcessMessageStream(process, jobConfig.getJobIdentifier());

        writeResSqoop(jobConfig, process.exitValue());

        deletePassword(jobConfig);

        HdfsUtils.deleteDirWithLog(jobConfig.getJobIdentifier(), jobConfig.getJobTmpDir());


    }


    private static void deletePassword(JobConfiguration jobConfig) throws Exception {
        String jobIdentifier = jobConfig.getJobIdentifier();
        if(HdfsUtils.deleteFromHadoop(jobIdentifier)){
            Log.write(jobConfig.getJobIdentifier(), "Delete password: "+getPasswordFileName(jobIdentifier));
        }
    }

    private static void writeResSqoop(JobConfiguration jobConfig, int prosExitValue) throws Exception {
        if (prosExitValue == 0) {
            Log.write(jobConfig.getJobIdentifier(), "Sqoop successfully");
        } else {
            Log.write(jobConfig.getJobIdentifier(), "Sqoop error");
        }
    }


    public static void printProcessMessageStream(Process process, String jobIdentifier){
        String line;
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while((line = input.readLine()) != null){
                Log.write(jobIdentifier, line, Log.Level.DEBUG);
            }
            input.close();
        }

        catch(Exception e)
        {
            e.printStackTrace();
        }
    }




    /**
     * Создание sqoop пароля
     *      p.s. переделать под создание сразу в hdfs

     */

    private static void createPassword(String password,String jobIdentifier) throws Exception {
        File passwordFile = new File(PASSWORD_FILE_PATH_DIR+Assets.SEPARATOR+getPasswordFileName(jobIdentifier));

        FileSystem fs = Assets.getInstance().getFs();


        try(FileWriter fw = new FileWriter(passwordFile)) {
            fw.write(password);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(passwordFile.exists()){
            Log.write(jobIdentifier, "Mysql-password have been creat");
        }

        if(!fs.exists(PASSWORD_HADOOP_PATH_DIR)) {
            fs.mkdirs(PASSWORD_HADOOP_PATH_DIR);
            Log.write(jobIdentifier, "Creating password directory "+PASSWORD_HADOOP_PATH_DIR.toString());

        }

        if(HdfsUtils.deleteFromHadoop(jobIdentifier)){
            Log.write(jobIdentifier, "old password have been delete ");
        }

        Log.write(jobIdentifier, "Copy password to hdfs : "+ getHadoopPasswordPath(jobIdentifier));
        fs.copyFromLocalFile(new Path(passwordFile.toString()), PASSWORD_HADOOP_PATH_DIR);
        if(fs.exists(getHadoopPasswordPath(jobIdentifier))){
            Log.write(jobIdentifier, "password have been load to hdfs "+ getHadoopPasswordPath(jobIdentifier));
        }

        Log.write(jobIdentifier, "Delete password from local file system");
        passwordFile.deleteOnExit();
    }



    private static String getPasswordFileName(String jobIdentifier){
        return jobIdentifier+".password";
    }

    private static Path getHadoopPasswordPath(String jobIdentifier){
        return new Path(PASSWORD_HADOOP_PATH_DIR+Assets.SEPARATOR+getPasswordFileName(jobIdentifier));
    }

}
