package com.example.repository;

import com.example.model.Example;
import org.springframework.data.repository.CrudRepository;

public interface ExampleRepository extends CrudRepository<Example, Integer> {
}
