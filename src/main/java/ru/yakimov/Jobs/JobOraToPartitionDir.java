/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;

@Component
public class JobOraToPartitionDir extends SubJob {
    @Override
    public Integer call() throws Exception {
        return 0;
    }


//    @Override
//    public void run() {
//        int interimRes = 0;
//
//        try {
//            interimRes += SqoopIE.importDbToTmp(getJobConfig(), Assets.IMPORT_MYSQL);
//        } catch (Exception e) {
//            interimRes++;
//            e.printStackTrace();
//        }
//
//        if(interimRes != 0){
//            setJobResult(interimRes);
//            return;
//        }
//
//        try {
//            SparkUtils.parsistData(getJobConfig());
//        } catch (Exception e) {
//            interimRes++;
//            e.printStackTrace();
//        }
//
//        setJobResult(interimRes);
//
//    }
}
