package ru.yakimov;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yakimov.Jobs.LoadMysqlToAvroJob;

@Configuration
public class JobContextConfiguration {

    @Bean
    public LoadMysqlToAvroJob loadMysqlToAvroJob(){
        return new LoadMysqlToAvroJob();
    }
}
