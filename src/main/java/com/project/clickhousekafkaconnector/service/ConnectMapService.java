package com.project.clickhousekafkaconnector.service;

import com.project.clickhousekafkaconnector.dto.ConnectDto;
import com.project.clickhousekafkaconnector.dto.ConnectMapDto;
import com.project.clickhousekafkaconnector.service.clickhouse.ClickHouseService;
import com.project.clickhousekafkaconnector.service.kafka.KafkaService;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


@Service
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@AllArgsConstructor
@Slf4j
public class ConnectMapService {

    ClickHouseService clickHouseService;
    KafkaService kafkaService;
    ConfigConnectService configConnectService;

    /***
     * Возвращает данные с названием всех топиков из Kafka и таблиц из ClickHouse
     */
    public Mono<ConnectMapDto> getConnectionData() {
        return clickHouseService.getTables()
                .zipWith(kafkaService.getListTopic(), (tables, topics) -> new ConnectMapDto(topics, tables))
                .doOnError(throwable -> log.error("Error get connection data with cause {}", throwable.getCause().getMessage()));
    }

    /***
     * Создает канал передачи данных из Kafka в ClickHouse c сохранением в БД с конфигурациями
     */
    public Flux<String> createChannel(ConnectDto connect) {
        return configConnectService.saveConfig(connect)
                .flatMapMany(connectConfig -> connectToChannel(connect))
                .doOnError(throwable -> log.error("Error create channel for topic {} with cause {}", connect.getTopic(), throwable.getCause().getMessage()));
    }

    /***
     * Создает канал передачи данных из Kafka в ClickHouse для существующих каналов в БД
     */
    public Mono<String> connectToChannel(ConnectDto connect) {
        return kafkaService.subscribe(connect.getTopic(), connect.getTable())
                .doOnError(throwable -> log.error("Error connect to channel for topic {} with cause {}", connect.getTopic(), throwable.getCause().getMessage()));

    }
}
