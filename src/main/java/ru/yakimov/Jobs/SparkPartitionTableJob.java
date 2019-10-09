/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.db.Log;
import ru.yakimov.utils.HdfsUtils;

@Component
public class SparkPartitionTableJob extends Job {

    @Override
    public Integer call() {
        try {

            Log.write(jobConfigs.getRootJobName(), "Checking for a directory ");

            HdfsUtils.deleteDirWithLog(jobConfigs.getRootJobName(), jobConfigs.getDirTo());

            Log.write(jobConfigs.getRootJobName(), "Spark read and rewrite partition data");

            Assets.getInstance().getSpark()
                    .read()
                    .parquet(jobConfigs.getDirFrom() + Assets.SEPARATOR + "*.parquet")
                    .write()
                    .partitionBy(jobConfigs.getPartitions())
                    .parquet(jobConfigs.getDirTo());

            Log.write(jobConfigs.getRootJobName(), "Delete tmp directory");

            HdfsUtils.deleteDirWithLog(jobConfigs.getRootJobName(), jobConfigs.getDirFrom());
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }

        return 0;
    }
}
