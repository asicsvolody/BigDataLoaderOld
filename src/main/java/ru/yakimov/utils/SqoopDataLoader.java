package ru.yakimov.utils;

import org.apache.hadoop.fs.Path;
import ru.yakimov.Assets;
import ru.yakimov.JobConfXML.JobConfiguration;
import ru.yakimov.JobConfXML.MysqlConfiguration;
import ru.yakimov.db.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SqoopDataLoader {

    private final Assets assets = Assets.getInstance();

    public SqoopDataLoader() throws Exception {
    }


    public int importToHdfsInAvroFiles(JobConfiguration jobConfig, String targetName) throws Exception {

        Log.write(jobConfig.getJobIdentifier(), "Sqoop get task import");

        Path hdfsDirPath = jobConfig.getHdfsDirTo();

        MysqlConfiguration mysqlConf = jobConfig.getMysqlConf(targetName);

        if(deleteHadoopDirectory(hdfsDirPath)){
           Log.write(jobConfig.getJobIdentifier(), "Delete "+ hdfsDirPath.toString() );
        }

        Log.write(jobConfig.getJobIdentifier(), "Sqoop run process");

        Process process = assets.getRt().exec(String.format("sqoop import " +
                "--connect jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL " +
                "--username %s " +
                "--password-file /user/sqoop.password " +
                "--table %s " +
                "--target-dir %s " +
                "--split-by user_id  " +
                "--as-avrodatafile"
                ,mysqlConf.getHost()
                ,mysqlConf.getPort()
                ,mysqlConf.getSchema()
                ,mysqlConf.getUser()
                ,mysqlConf.getTable()
                ,hdfsDirPath
        ));

        Log.write(jobConfig.getJobIdentifier(), "Waiting end sqoop process");

        process.waitFor();

        printProcessErrorStream(process, jobConfig.getJobIdentifier());

        if(process.exitValue() == 0) {
            Log.write(jobConfig.getJobIdentifier(), "Sqoop successfully");
        }

        return process.exitValue();
    }

    private void sqoopExportTable(String nameDB, String tableName, String exportFromPath) throws Exception {

        System.out.println("Sqoop exports to table:" +tableName+" of database " + nameDB+ "from path directory: "+ exportFromPath );

        Assets.getInstance().getRt().exec(String.format("sqoop export " +
                "--connect \"jdbc:mysql://localhost:3306/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username vladimir  " +
                "--password-file /user/sqoop.password " +
                "--table %s " +
                "--export-dir %s " +
                "--validate",nameDB , tableName, exportFromPath))
                .waitFor();
    }


    public void printProcessErrorStream(Process process, String jobIdentifer){
        String line;
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while((line = input.readLine()) != null){
                Log.write(jobIdentifer, line, Log.Level.DEBUG);
            }
            input.close();
        }

        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    private boolean deleteHadoopDirectory(Path path) throws IOException {
        if(assets.getFs().exists(path)){
            assets.getFs().delete(path, true);
            return true;
        }
        return false;
    }

}
