/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.utils.SparkUtils;
import ru.yakimov.utils.SqoopIE;

@Component
public class JobOraToPartitionDir extends Job {


    @Override
    public void run() {
        int interimRes = 0;

        try {
            interimRes += SqoopIE.importDbToTmp(getJobConfig(), Assets.IMPORT_MYSQL);
        } catch (Exception e) {
            interimRes++;
            e.printStackTrace();
        }

        if(interimRes != 0){
            setJobResult(interimRes);
            return;
        }

        try {
            SparkUtils.parsistData(getJobConfig());
        } catch (Exception e) {
            interimRes++;
            e.printStackTrace();
        }

        setJobResult(interimRes);

    }
}
