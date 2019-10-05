package ru.yakimov.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.yakimov.Assets;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.config.MysqlConfiguration;
import ru.yakimov.db.Log;

import java.io.*;

public class SqoopDataLoader {
    private final String PASSWORD_FILE_PATH_DIR = "./";
    private final Path PASSWORD_HADOOP_PATH_DIR = new Path("/user/password");

    private final Assets assets = Assets.getInstance();

    public SqoopDataLoader() throws Exception {
    }


    public int importToHdfsInAvroFiles(JobConfiguration jobConfig, String targetName) throws Exception {

        Log.write(jobConfig.getJobIdentifier(), "Sqoop get task import");

        Path hdfsDirPath = jobConfig.getHdfsDirTo();

        MysqlConfiguration mysqlConfig = jobConfig.getMysqlConf(targetName);

        Log.write(jobConfig.getJobIdentifier(), "Start creating password file");

        createPassword(mysqlConfig.getPassword(), jobConfig.getJobIdentifier());



        if(deleteFromHadoop(hdfsDirPath)){
           Log.write(jobConfig.getJobIdentifier(), "Delete "+ hdfsDirPath.toString() );
        }

        Log.write(jobConfig.getJobIdentifier(), "Sqoop run process");

        String str = String.format("sqoop import " +
                        "--connect jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL " +
                        "--username %s " +
                        "--password-file %s " +
                        "--table %s " +
                        "--target-dir %s " +
                        "--split-by %s  " +
                        "--as-avrodatafile"
                ,mysqlConfig.getHost()
                ,mysqlConfig.getPort()
                ,mysqlConfig.getSchema()
                ,mysqlConfig.getUser()
                ,getHadoopPasswordPath(jobConfig.getJobIdentifier())
                ,mysqlConfig.getTable()
                ,hdfsDirPath
                ,mysqlConfig.getPrimaryKeys().next()
        );

        Process process = assets.getRt().exec(String.format("sqoop import " +
                "--connect jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL " +
                "--username %s " +
                "--password-file %s " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by %s " +
                "--as-avrodatafile"
                ,mysqlConfig.getHost()
                ,mysqlConfig.getPort()
                ,mysqlConfig.getSchema()
                ,mysqlConfig.getUser()
                ,getHadoopPasswordPath(jobConfig.getJobIdentifier())
                ,mysqlConfig.getTable()
                ,hdfsDirPath
                ,mysqlConfig.getPrimaryKeys().next()
        ));

        Log.write(jobConfig.getJobIdentifier(), "Waiting end sqoop process");

        process.waitFor();

        printProcessMessageStream(process, jobConfig.getJobIdentifier());

        if(process.exitValue() == 0) {
            Log.write(jobConfig.getJobIdentifier(), "Sqoop successfully");
        }
        if(deleteFromHadoop(getHadoopPasswordPath(jobConfig.getJobIdentifier()))){
            Log.write(jobConfig.getJobIdentifier(), "Delete password: "+getPasswordFileName(jobConfig.getJobIdentifier()));
        }

        return process.exitValue();
    }


    public void printProcessMessageStream(Process process, String jobIdentifier){
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

    private boolean deleteFromHadoop(Path path) throws IOException {
        if(assets.getFs().exists(path)){
            assets.getFs().delete(path, true);
            return true;
        }
        return false;
    }


    /**
     * Создание sqoop пароля
     *      p.s. переделать под создание сразу в hdfs

     */

    private void createPassword(String password,String jobIdentifier) throws Exception {
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

        if(deleteFromHadoop(getHadoopPasswordPath(jobIdentifier))){
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



    private String getPasswordFileName(String jobIdentifier){
        return jobIdentifier+".password";
    }

    private Path getHadoopPasswordPath(String jobIdentifier){
        return new Path(PASSWORD_HADOOP_PATH_DIR+Assets.SEPARATOR+getPasswordFileName(jobIdentifier));
    }

}
