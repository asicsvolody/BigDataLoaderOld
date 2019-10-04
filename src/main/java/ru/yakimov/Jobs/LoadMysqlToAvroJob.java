package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.utils.SqoopDataLoader;

import java.io.IOException;

@Component
public class LoadMysqlToAvroJob extends Job {

    @Override
    public void run() {
        int interimRes = 0;

        try {
            interimRes+= new SqoopDataLoader().importToHdfsInAvroFiles(getJobConfig(), "loadDataFrom");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(interimRes != 0){
            setJobResult(interimRes);
            return;
        }


        setJobResult(interimRes);



    }
}
