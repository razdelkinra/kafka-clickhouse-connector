package com.project.clickhousekafkaconnector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaClickhouseConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaClickhouseConnectorApplication.class, args);
    }

}
