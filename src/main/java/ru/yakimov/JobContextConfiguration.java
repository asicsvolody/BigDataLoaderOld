/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import ru.yakimov.Jobs.JobJoinTablesInPartition;
import ru.yakimov.Jobs.JobOraToPartitionDir;
import ru.yakimov.Jobs.JobUpdatePartitionData;
import ru.yakimov.Jobs.JobWriteNewDataInTable;

@Configuration
public class JobContextConfiguration {

    @Bean
    @Scope("prototype")
    public JobOraToPartitionDir loadJobOraToPartitionDir(){
        return new JobOraToPartitionDir();
    }

    @Bean
    @Scope("prototype")
    public JobJoinTablesInPartition loadJobJoinTablesInPartition(){
        return new JobJoinTablesInPartition();
    }

    @Bean
    @Scope("prototype")
    public JobUpdatePartitionData loadJobUpdataPartitionData(){
        return new JobUpdatePartitionData();
    }

    @Bean
    @Scope("prototype")
    public JobWriteNewDataInTable loadJobWriteNewDataInTable(){
        return new JobWriteNewDataInTable();
    }
}
