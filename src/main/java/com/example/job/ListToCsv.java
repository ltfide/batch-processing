package com.example.job;

import com.example.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.WritableResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Convert List into a CSV
 * @author Lutfi
 * @since 27-01-2024
 */
@Configuration
public class ListToCsv {

    private static final String FIRSTNAME = "firstname";
    private static final String LASTNAME = "lastname";
    private static final int AGE = 10;
    private static final WritableResource CSV_OUTPUT =  new FileSystemResource("src/main/resources/person.csv");

    public ItemReader<Person> personReader() {
        List<Person> persons = new ArrayList<>();
        persons.add(new Person(FIRSTNAME, LASTNAME, AGE));
        persons.add(new Person(FIRSTNAME, LASTNAME, AGE));
        persons.add(new Person(FIRSTNAME, LASTNAME, AGE));
        return new ListItemReader<>(persons);
    }

    public ItemProcessor<Person, Person> personProcessor() {
        AtomicInteger counter = new AtomicInteger(0);
        return person -> {
            int c = counter.incrementAndGet();
            person.setFirstname(person.getFirstname() + c);
            person.setLastname(person.getLastname() + c);
            person.setAge(person.getAge() + c);
            return person;
        };
    }

    public FlatFileHeaderCallback personHeader() {
        return writer -> writer.write("firstname,lastname,age");
    }

    public FlatFileItemWriter<Person> personWriter() {
        return new FlatFileItemWriterBuilder<Person>()
                .name("PersonWriterCSV")
                .resource(CSV_OUTPUT)
                .delimited()
                .delimiter(",")
                .names(new String[] {"firstname", "lastname", "age"})
                .headerCallback(personHeader())
                .shouldDeleteIfExists(true)
                .build();
    }

    public Step csvStep(JobRepository jobRepository,
                        PlatformTransactionManager transactionManager) {
        return new StepBuilder("CsvStep", jobRepository)
                .<Person, Person> chunk(10, transactionManager)
                .reader(personReader())
                .processor(personProcessor())
                .writer(personWriter())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Job csvJob(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager) {
        return new JobBuilder("CsvJob", jobRepository)
                .start(csvStep(jobRepository, transactionManager))
                .incrementer(new RunIdIncrementer())
                .build();
    }
}
