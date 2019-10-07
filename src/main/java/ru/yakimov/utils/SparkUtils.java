/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import ru.yakimov.Assets;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.db.Log;

public class SparkUtils {

    public static void parsistData(JobConfiguration conf) throws Exception {
        Log.write(conf.getJobIdentifier(), "Checking for a directory ");

        HdfsUtils.deleteDirWithLog(conf.getJobIdentifier(),conf.getJobDirTo() );

        Log.write(conf.getJobIdentifier(), "Spark read and rewrite partition data");

        Assets.getInstance().getSpark()
                .read()
                .parquet(conf.getJobTmpDir()+Assets.SEPARATOR+"*.parquet")
                .write()
                .partitionBy(conf.getPartitionCols())
                .parquet(conf.getJobDirTo());

        Log.write(conf.getJobIdentifier(), "Delete tmp directory");

        HdfsUtils.deleteDirWithLog(conf.getJobIdentifier(), conf.getJobTmpDir());

    }
}
