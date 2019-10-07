/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.db;

import ru.yakimov.Assets;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class LogsFileWriter {
    
    public static void write (String jobIdentifier) throws Exception {

        Log.write(jobIdentifier, "Read logs from mysql");

        String sql = String.format("SELECT SYSTEM_LOG_MSG FROM SYSTEM_LOG WHERE SYSTEM_LOG_JOB_IDENTIFIER = '%s'", jobIdentifier);
        List<String> logsArr = MySqlDb.getSqlResults(sql);

        File dirTo = new File(Assets.getInstance().getConf().getLogsDir().toString());
        if(dirTo.mkdirs()){
            Log.write(jobIdentifier, "Dir to have created");
        }

        Log.write(jobIdentifier, "Write log file");
        try(FileWriter output = new FileWriter(new File(
                dirTo.toString()+Assets.SEPARATOR+jobIdentifier+".log"))){
            for (String s : logsArr) {
                output.write(s +"\n");
            }

        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
