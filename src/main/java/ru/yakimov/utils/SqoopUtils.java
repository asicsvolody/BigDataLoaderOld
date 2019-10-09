/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.yakimov.Assets;
import ru.yakimov.db.Log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SqoopUtils {

    private final static String PASSWORD_FILE_PATH_DIR = "./";
    private final static Path PASSWORD_HADOOP_PATH_DIR = new Path("/user/password");

    public static void createPassword(String password,String jobIdentifier) throws Exception {
        File passwordFile = new File(PASSWORD_FILE_PATH_DIR+ Assets.SEPARATOR+getPasswordFileName(jobIdentifier));

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
        if(fs.exists(new Path(getHadoopPasswordPath(jobIdentifier)))){
            Log.write(jobIdentifier, "password have been load to hdfs "+ getHadoopPasswordPath(jobIdentifier));
        }

        Log.write(jobIdentifier, "Delete password from local file system");
        passwordFile.deleteOnExit();
    }



    private static String getPasswordFileName(String jobIdentifier){
        return jobIdentifier+".password";
    }

    public static String getHadoopPasswordPath(String jobIdentifier){
        return PASSWORD_HADOOP_PATH_DIR+Assets.SEPARATOR+getPasswordFileName(jobIdentifier);
    }
}
