/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov;

import ru.yakimov.Jobs.RootJob;
import ru.yakimov.Jobs.SubJob;
import ru.yakimov.db.LogsFileWriter;
import ru.yakimov.db.MySqlDb;
import ru.yakimov.utils.RootJobsCreator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class BootProcessMain {

    public BootProcessMain() {
        try {

            RootJob[] rootJobs = RootJobsCreator.getRootJobsFromDir(Assets.getInstance().getConf().getJobsDir());


            if (rootJobs == null) {
                System.out.println("Jobs not found");
                return;
            }

            ExecutorService service = Executors.newCachedThreadPool();

            Map<String, Future<Integer>> futureMap = new HashMap<>();

            for (RootJob rootJob : rootJobs) {

                futureMap.put(rootJob.getRootJobName(), service.submit(rootJob));

            }

            service.shutdown();

            service.awaitTermination(10, TimeUnit.HOURS);


            for (String rootJobName : futureMap.keySet()) {
                printFutureResults(rootJobName, futureMap.get(rootJobName));
            }

            System.out.println("BootProsesMain has finished.");

            System.out.println("Write logs files");
            for (RootJob rootJob : rootJobs) {
                LogsFileWriter.write(rootJob.getRootJobName());
            }

        } catch (Exception e){
            e.printStackTrace();
        }
        finally {

        Assets.closeResources();

        }

    }

    private static void printFutureResults(String rootJobName, Future<Integer> future) throws ExecutionException, InterruptedException {
        StringBuilder str = new StringBuilder(rootJobName);
        switch (future.get()){
            case 0:
                str.append(" completed successfully.");
                break;
            case 1:
                str.append(" completed with error");
                break;
        }

        System.out.println(str.toString());
    }

    public static void main(String[] args) {
        new BootProcessMain();
    }
}
