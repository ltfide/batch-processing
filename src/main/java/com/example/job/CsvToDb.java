package com.example.job;

import com.example.model.Example;
import com.example.repository.ExampleRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Generate CSV into a DB
 * @author Lutfi
 * @since 27-01-2024
 */
@Configuration
@RequiredArgsConstructor
public class CsvToDb {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final ExampleRepository exampleRepository;

    public FlatFileItemReader<Example> csvToDbReader() {
        return new FlatFileItemReaderBuilder<Example>()
                .name("ExampleCsvReader")
                .resource(new FileSystemResource("src/main/resources/examples.csv"))
                .delimited()
                .delimiter(",")
                .names("no", "uuid", "status")
                .fieldSetMapper(fieldSetMapper())
                .build();
    }

    private FieldSetMapper<Example> fieldSetMapper() {
        return fieldSet -> {
            Example example = new Example();
            example.setNo(fieldSet.readInt("no"));
            example.setUuid(fieldSet.readString("uuid"));
            example.setStatus(fieldSet.readString("status"));
            return example;
        };
    }

    public ItemProcessor<Example, Example> csvToDbProcessor() {
        return example -> {
            // if total data 10_000 will be skipped in no. 999
            // so the total became 9999
            if (example.getNo() == 999) {
                throw new ValidationException("Testing");
            }
            return example;
        };
    }

    public ItemWriter<Example> csvToDbWriter() {
        return exampleRepository::saveAll;
    }

    public Step csvToDbStep() {
        return new StepBuilder("CsvToDbStep", jobRepository)
                .<Example, Example> chunk(100, transactionManager)
                .reader(csvToDbReader())
                .processor(csvToDbProcessor())
                .writer(csvToDbWriter())
                .faultTolerant()
                .skipLimit(1)
                .skip(ValidationException.class)
                .build();
    }

    @Bean
    public Job csvToDbJob() {
        return new JobBuilder("CsvToDb", jobRepository)
                .start(csvToDbStep())
                .incrementer(new RunIdIncrementer())
                .build();
    }
}
