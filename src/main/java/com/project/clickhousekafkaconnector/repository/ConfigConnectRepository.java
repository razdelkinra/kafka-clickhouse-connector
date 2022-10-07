package com.project.clickhousekafkaconnector.repository;

import com.project.clickhousekafkaconnector.entitty.ConnectConfig;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface ConfigConnectRepository extends ReactiveCrudRepository<ConnectConfig, Long> {
}
