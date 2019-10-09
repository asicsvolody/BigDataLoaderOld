/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import ru.yakimov.Jobs.RootJob;
import ru.yakimov.config.JobXmlLoader;

import java.io.File;
import java.util.ArrayList;

public class RootJobsCreator {

    public  static RootJob[] getRootJobsFromDir(String jobsDir){

        ArrayList <RootJob> resRootJobs = new ArrayList<>();

        File[] rootJobsFiles = new File(jobsDir).listFiles();

        if(rootJobsFiles == null)
            return null;

        for (File file : rootJobsFiles) {
            if(file.getPath().endsWith(".jxml")){
                RootJob rootJob = getRootJobsFromFile(file);
                resRootJobs.add(rootJob);
            }
        }

        return resRootJobs.toArray(new RootJob[0]);
    }

    private static RootJob getRootJobsFromFile(File file) {
        return new RootJob(JobXmlLoader.readConfJob(file));
    }

}
