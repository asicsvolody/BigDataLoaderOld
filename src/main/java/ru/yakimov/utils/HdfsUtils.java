/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.yakimov.Assets;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.logDb.Log;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;

public class HdfsUtils {

    public static synchronized boolean deleteFromHadoop(String path) throws XMLStreamException, IOException, SQLException {
        FileSystem fs = Assets.getInstance().getFs();
        Path file = new Path(path);
        if(fs.exists(file)){
            fs.delete(file, true);
            return true;
        }
        return false;
    }

    public static synchronized void deleteDirWithLog(JobConfiguration config,  String hdfsDir) throws SQLException, IOException, XMLStreamException {
        if(deleteFromHadoop(hdfsDir)){
            Log.write(config, "Delete "+ hdfsDir );
        }
    }

}
