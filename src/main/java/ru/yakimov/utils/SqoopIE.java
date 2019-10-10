/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.hadoop.fs.Path;

public class SqoopIE {
    private final static String PASSWORD_FILE_PATH_DIR = "./";
    private final static Path PASSWORD_HADOOP_PATH_DIR = new Path("/user/password");



//    public static int importDbToTmp(RootJobConfiguration jobConfig, String targetDB) throws Exception {

//        Log.write(jobConfig.getJobIdentifier(), "Sqoop get task import");
//
//
//        DBConfiguration mysqlConfig = jobConfig.getMysqlConf(targetDB);
//
//        Log.write(jobConfig.getJobIdentifier(), "Start creating password file");
//
//        createPassword(mysqlConfig.getPassword(), jobConfig.getJobIdentifier());
//
//
//        HdfsUtils.deleteDirWithLog(jobConfig.getJobIdentifier(), jobConfig.getJobTmpDir());
//
//        Log.write(jobConfig.getJobIdentifier(), "Sqoop run process");
//
//
//        Process process = Assets.getInstance().getRt().exec(String.format("sqoop import " +
//                "--connect jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL " +
//                "--username %s " +
//                "--password-file %s " +
//                "--table %s " +
//                "--target-dir %s " +
//                "--split-by %s " +
//                "--as-parquetfile"
//                ,mysqlConfig.getHost()
//                ,mysqlConfig.getPort()
//                ,mysqlConfig.getSchema()
//                ,mysqlConfig.getUser()
//                ,getHadoopPasswordPath(jobConfig.getJobIdentifier())
//                ,mysqlConfig.getTable()
//                ,jobConfig.getJobTmpDir()
//                ,mysqlConfig.getPrimaryKeys().next()
//        ));
//
//        Log.write(jobConfig.getJobIdentifier(), "Waiting of end sqoop process");
//
//        process.waitFor();
//
//        writeProcessMessageStream(process, jobConfig.getJobIdentifier());
//
//        writeResSqoop(jobConfig, process.exitValue());
//
//        deletePassword(jobConfig);
//
//        return process.exitValue();
//    }
//
//    private static void exportTmpToDb(RootJobConfiguration jobConfig, String targetDB) throws Exception {
//        Log.write(jobConfig.getJobIdentifier(), "Sqoop get task export");
//
//        DBConfiguration mysqlConfig = jobConfig.getMysqlConf(targetDB);
//
//        Log.write(jobConfig.getJobIdentifier(), "Start creating password file");
//
//        createPassword(mysqlConfig.getPassword(), jobConfig.getJobIdentifier());
//
//
//
//        Process process = Assets.getInstance().getRt().exec(String.format("sqoop export " +
//                "--connect \"jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
//                "--username %s " +
//                "--password-file %s " +
//                "--table %s " +
//                "--export-dir %s " +
//                "--validate"
//                ,mysqlConfig.getHost()
//                ,mysqlConfig.getPort()
//                ,mysqlConfig.getSchema()
//                ,mysqlConfig.getUser()
//                ,getHadoopPasswordPath(jobConfig.getJobIdentifier())
//                ,mysqlConfig.getTable()
//                ,jobConfig.getJobTmpDir())
//        );
//
//        Log.write(jobConfig.getJobIdentifier(), "Waiting of end sqoop process");
//
//        process.waitFor();
//
//        writeProcessMessageStream(process, jobConfig.getJobIdentifier());
//
//        writeResSqoop(jobConfig, process.exitValue());
//
//        deletePassword(jobConfig);
//
//        HdfsUtils.deleteDirWithLog(jobConfig.getJobIdentifier(), jobConfig.getJobTmpDir());
//
//
//    }
//
//
//    private static void deletePassword(RootJobConfiguration jobConfig) throws Exception {
//        String jobIdentifier = jobConfig.getJobIdentifier();
//        if(HdfsUtils.deleteFromHadoop(jobIdentifier)){
//            Log.write(jobConfig.getJobIdentifier(), "Delete password: "+getPasswordFileName(jobIdentifier));
//        }
//    }
//
//    private static void writeResSqoop(RootJobConfiguration jobConfig, int prosExitValue) throws Exception {
//        if (prosExitValue == 0) {
//            Log.write(jobConfig.getJobIdentifier(), "Sqoop successfully");
//        } else {
//            Log.write(jobConfig.getJobIdentifier(), "Sqoop error");
//        }
//    }
//
//
//    public static void writeProcessMessageStream(Process process, String jobIdentifier){
//        String line;
//        try {
//            BufferedReader input = new BufferedReader(new InputStreamReader(process.getErrorStream()));
//            while((line = input.readLine()) != null){
//                Log.write(jobIdentifier, line, Log.Level.DEBUG);
//            }
//            input.close();
//        }
//
//        catch(Exception e)
//        {
//            e.printStackTrace();
//        }
//    }

}
