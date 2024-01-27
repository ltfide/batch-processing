package com.example.runner;

import com.example.service.BatchService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class BatchRunner implements CommandLineRunner {

    private final BatchService service;

    @Override
    public void run(String... args) throws Exception {
        service.runAllJob();
    }
}
