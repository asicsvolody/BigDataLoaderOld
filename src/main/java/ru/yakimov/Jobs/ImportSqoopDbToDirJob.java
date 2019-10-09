/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.config.DBConfiguration;
import ru.yakimov.db.Log;
import ru.yakimov.utils.HdfsUtils;
import ru.yakimov.utils.SqoopUtils;


@Component
public class ImportSqoopDbToDirJob extends Job {


    @Override
    public Integer call() throws Exception {
        Log.write(jobConfigs.getJobName(), "Sqoop get task import");


        DBConfiguration dbConfig = jobConfigs.getDbConfiguration();

        Log.write(jobConfigs.getJobName(), "Start creating password file");

        SqoopUtils.createPassword(dbConfig.getPassword(), jobConfigs.getJobName());


        HdfsUtils.deleteDirWithLog(jobConfigs.getJobName(), jobConfigs.getDirTo());

        Log.write(jobConfigs.getJobName(), "Sqoop run process");


        Process process = Assets.getInstance().getRt().exec(String.format("sqoop import " +
                        "--connect jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL " +
                        "--username %s " +
                        "--password-file %s " +
                        "--table %s " +
                        "--target-dir %s " +
                        "--split-by %s " +
                        "--as-parquetfile"
                ,dbConfig.getHost()
                ,dbConfig.getPort()
                ,dbConfig.getSchema()
                ,dbConfig.getUser()
                ,SqoopUtils.getHadoopPasswordPath(jobConfigs.getJobName())
                ,dbConfig.getTable()
                ,jobConfigs.getDirTo()
                ,dbConfig.getPrimaryKeys().next()
        ));

        Log.write(jobConfigs.getRootJobName(), "Waiting of end sqoop process");

        process.waitFor();

//        printProcessMessageStream(process, jobConfig.getJobIdentifier());

//        writeResSqoop(jobConfig, process.exitValue());

        HdfsUtils.deleteDirWithLog(jobConfigs.getJobName(), SqoopUtils.getHadoopPasswordPath(jobConfigs.getJobName()));

        return process.exitValue();
    }
}
