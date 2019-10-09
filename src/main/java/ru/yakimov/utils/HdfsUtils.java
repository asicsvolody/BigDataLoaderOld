/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.yakimov.Assets;
import ru.yakimov.db.Log;

public class HdfsUtils {

    public static boolean deleteFromHadoop(String path) throws Exception {
        FileSystem fs = Assets.getInstance().getFs();
        Path file = new Path(path);
        if(fs.exists(file)){
            fs.delete(file, true);
            return true;
        }
        return false;
    }

    public static void deleteDirWithLog(String jobIdentifier, String hdfsDir) throws Exception {
        if(deleteFromHadoop(hdfsDir)){
            Log.write(jobIdentifier, "Delete "+ hdfsDir );
        }
    }

}
