package ru.yakimov;

import org.apache.hadoop.fs.Path;
import ru.yakimov.JobConfXML.JobConfiguration;
import ru.yakimov.JobConfXML.MysqlConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SqoopDataLoader {

    private final Assets assets = Assets.getInstance();


//    public int importTableToHDFS() throws IOException, InterruptedException {
////        deleteHadoopDirectory(new Path(assets.getHDFSDir()));
//
//        ProcessBuilder builder = new ProcessBuilder(
//                "sqoop"
//                ,"import"
//                ,"--connect",
//                String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL"
//                        ,assets.getMysqlDomain()
//                        ,assets.getPort()
//                        ,assets.getMysqlDatabase()
//                )
//                ,"--username"
//                ,assets.getUser()
//                ,"--password-file","/user/sqoop.password"
//                ,"--target-dir"
//                ,assets.getHDFSDir()
//                ,"--table"
//                ,assets.getArgsMap().get(Assets.Args.MYSQL_TABLE)
//                ,"--split-by","user_id"
////                ,"--columns","TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH"
////                ,"--where",String.format("TABLE_NAME='%s' AND TABLE_SCHEMA='%s'",tableName,nameDB)
//                ,"--as-parquetfile"
//        );
//
//        for (String s : builder.command()) {
//            System.out.println(s);
//        }
//        Process process = builder.start();
//        process.waitFor();
//        printProcessErrorStream(process);
//        return process.exitValue();
//    }




    public int importToHdfsInAvroFiles(JobConfiguration jobConfig, String targetName) throws IOException, InterruptedException {

        Path hdfsDirPath = jobConfig.getHdfsDirTo();
        MysqlConfiguration mysqlConf = jobConfig.getMysqlConf(targetName);

        deleteHadoopDirectory(hdfsDirPath);

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
        process.waitFor();
        printProcessErrorStream(process);

        return process.exitValue();
    }

    private void sqoopExportTable(String nameDB, String tableName, String exportFromPath) throws IOException, InterruptedException {

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


    public void printProcessErrorStream(Process process){
        String line;
        try
        {
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            while((line = input.readLine()) != null)
            {
                if(process.exitValue() == 0) {
                    assets.getProsLogger().debug(line);
                }else{
                    assets.getProsLogger().error(line);
                }
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
