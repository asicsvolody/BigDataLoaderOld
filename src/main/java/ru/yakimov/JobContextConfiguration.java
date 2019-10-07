/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import ru.yakimov.Jobs.JobOraToPartitionDir;

@Configuration
public class JobContextConfiguration {

    @Bean
    @Scope("prototype")
    public JobOraToPartitionDir loadOraToParsDir(){
        return new JobOraToPartitionDir();
    }
}
