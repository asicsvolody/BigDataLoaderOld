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
import ru.yakimov.logDb.Log;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.sql.SQLException;

public class SqoopUtils {

    private final static String PASSWORD_FILE_PATH_DIR = "./";
    private final static Path PASSWORD_HADOOP_PATH_DIR = new Path("/user/password");

    public static void createPassword(String password, JobConfiguration config) throws XMLStreamException, IOException, SQLException {

        File passwordFile = new File(PASSWORD_FILE_PATH_DIR+ Assets.SEPARATOR+getPasswordFileName(config.getJobName()));

        Log.write(config,"Wold be created password file: "+ passwordFile.getPath());



        FileSystem fs = Assets.getInstance().getFs();


        try(FileWriter fw = new FileWriter(passwordFile)) {
            fw.write(password);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(passwordFile.exists()){
            Log.write(config, "DB-password have been creat");
        }

        if(!fs.exists(PASSWORD_HADOOP_PATH_DIR)) {
            fs.mkdirs(PASSWORD_HADOOP_PATH_DIR);
            Log.write(config, "Creating password directory "+PASSWORD_HADOOP_PATH_DIR.toString());

        }

        if(HdfsUtils.deleteFromHadoop(config.getJobName())){
            Log.write(config, "old password have been delete ");
        }

        Log.write(config, "Copy password to hdfs : "+ getHadoopPasswordPath(config.getJobName()));
        fs.copyFromLocalFile(new Path(passwordFile.toString()), PASSWORD_HADOOP_PATH_DIR);
        if(fs.exists(new Path(getHadoopPasswordPath(config.getJobName())))){
            Log.write(config, "password have been load to hdfs "+ getHadoopPasswordPath(config.getJobName()));
        }

        Log.write(config, "Delete password from local file system");
        passwordFile.deleteOnExit();
    }

    public static void writeProcessMessageStream(Process process, JobConfiguration config){
        String line;
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while((line = input.readLine()) != null){
                Log.write(config, line, Log.Level.DEBUG);
            }
            input.close();
        }

        catch(Exception e)
        {
            e.printStackTrace();
        }
    }



    private static String getPasswordFileName(String jobIdentifier){
        return jobIdentifier+".password";
    }

    public static String getHadoopPasswordPath(String jobIdentifier){
        return PASSWORD_HADOOP_PATH_DIR+Assets.SEPARATOR+getPasswordFileName(jobIdentifier);
    }

    public static void writeResSqoop(JobConfiguration jobConfig, int prosExitValue) throws SQLException {
        if (prosExitValue == 0) {
            Log.write(jobConfig, "Sqoop successfully");
        } else {
            Log.write(jobConfig, "Sqoop error");
        }
    }
}
