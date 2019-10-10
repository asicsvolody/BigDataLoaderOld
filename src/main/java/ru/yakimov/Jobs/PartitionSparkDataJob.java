/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.exceptions.DirArrayException;
import ru.yakimov.logDb.Log;
import ru.yakimov.utils.HdfsUtils;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;

@Component
public class PartitionSparkDataJob extends Job {

    @Override
    public Integer call() throws SQLException, IOException, XMLStreamException {

            if(jobConfig.getDirFrom().size()!= 1){
                Log.write(jobConfig, "Wrong dir from array size", Log.Level.ERROR);
                return 1;
            }

            Log.write(jobConfig, "Checking for a directory ");

            HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirTo());

            Log.write(jobConfig, "Spark read and rewrite to partition data");

            Assets.getInstance().getSpark()
                    .read()
                    .parquet(jobConfig.getDirFrom().get(0) + Assets.SEPARATOR + "*.parquet")
                    .write()
                    .partitionBy(jobConfig.getPartitions())
                    .parquet(jobConfig.getDirTo());

            Log.write(jobConfig, "Delete tmp directory");

            HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirFrom().get(0));

        return 0;
    }
}
