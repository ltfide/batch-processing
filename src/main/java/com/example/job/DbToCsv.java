package com.example.job;

import com.example.model.Example;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Convert Db into a CSV
 * @author Lutfi
 * @since 27-01-2024
 */
@Configuration
@RequiredArgsConstructor
public class DbToCsv {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private static final String SQL_QUERY = "SELECT * FROM EXAMPLES";

    public JdbcCursorItemReader<Example> dbToCsvReader() {
        return new JdbcCursorItemReaderBuilder<Example>()
                .name("DbToCsvReader")
                .dataSource(dataSource)
                .sql(SQL_QUERY)
                .rowMapper(new BeanPropertyRowMapper<>(Example.class))
                .build();
    }

    public FlatFileItemWriter<Example> dbToCsvWriter() {
        return new FlatFileItemWriterBuilder<Example>()
                .name("DbToCsvWriter")
                .resource(new FileSystemResource("src/main/resources/examples_from_db.csv"))
                .delimited()
                .delimiter(",")
                .names(new String[] {"no", "uuid", "status"})
                .shouldDeleteIfExists(true)
                .build();
    }

    public Step dbToCsvStep() {
        return new StepBuilder("DbToCsvStep", jobRepository)
                .<Example, Example> chunk(1000, transactionManager)
                .reader(dbToCsvReader())
                .writer(dbToCsvWriter())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Job dbToCsvJob() {
        return new JobBuilder("DbToCsv", jobRepository)
                .start(dbToCsvStep())
                .incrementer(new RunIdIncrementer())
                .build();
    }
}
