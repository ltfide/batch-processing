package com.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchService {

    private final JobLauncher jobLauncher;
    private final Job csvToDbJob;

    private JobParameters getJobParameters() {
        return new JobParametersBuilder()
            .addLong("startsAt", System.currentTimeMillis())
            .toJobParameters();
    }

    public void runCsvToDb() {
        try {
            jobLauncher.run(csvToDbJob, getJobParameters());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
