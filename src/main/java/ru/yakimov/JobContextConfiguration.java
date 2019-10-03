package ru.yakimov;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import ru.yakimov.Jobs.LoadMysqlToAvroJob;

@Configuration
public class JobContextConfiguration {

    @Bean
    @Scope("prototype")
    public LoadMysqlToAvroJob loadMysqlToAvroJob(){
        return new LoadMysqlToAvroJob();
    }
}
