package ru.yakimov.Jobs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.SqoopDataLoader;

import java.io.IOException;
import java.util.List;

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
        }

        if(interimRes != 0){
            setJobResult(interimRes);
            return;
        }


        setJobResult(interimRes);



    }
}
