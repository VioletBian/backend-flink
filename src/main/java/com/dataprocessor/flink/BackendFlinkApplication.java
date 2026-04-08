package com.dataprocessor.flink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class BackendFlinkApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendFlinkApplication.class, args);
    }
}
