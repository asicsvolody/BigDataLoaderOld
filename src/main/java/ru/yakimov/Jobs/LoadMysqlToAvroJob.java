package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.SqoopDataLoader;

import java.io.IOException;

@Component
public class LoadMysqlToAvroJob extends Job {

//    @Override
//    public void run() {
//        int interimRes = 0;
//
//        if(interimRes == 0){
//            try {
//                interimRes+= new SqoopDataLoader().importToHdfsInAvroFiles(getJobConfig(), "loadDataFrom");
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        setJobResult(interimRes);
//
//    }

    @Override
    public Object call() throws Exception {

        int interimRes = 0;

        if(interimRes == 0){
            try {
                interimRes+= new SqoopDataLoader().importToHdfsInAvroFiles(getJobConfig(), "loadDataFrom");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return interimRes;
    }
}
