package com.project.clickhousekafkaconnector.service;

import com.project.clickhousekafkaconnector.dto.ConnectDto;
import com.project.clickhousekafkaconnector.entitty.ConnectConfig;
import com.project.clickhousekafkaconnector.repository.ConfigConnectRepository;
import com.project.clickhousekafkaconnector.service.redis.KeyService;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@AllArgsConstructor
@Slf4j
public class InitConfigurationService {

    ConfigConnectRepository configConnectRepository;
    ConnectMapService connectMapService;
    KeyService keyService;


    // TODO: Добавить consul для проверки состояния на других репликах

    /***
     * Считывает конфигурацию для существующих каналов передачи данных их Kafka в CLickHouse из БД
     */
    @EventListener(ApplicationReadyEvent.class)
    public void getConfiguration() {
        configConnectRepository.findAll()
                .filter(ConnectConfig::getIsEnable)
                .flatMap(connectConfig -> connectMapService.connectToChannel(new ConnectDto(null, connectConfig.getTopic(), connectConfig.getTable())))
                .collectList()
                .doOnSuccess(s -> log.info("Configuration has been loaded. {} consumers is working", s.size()))
                .doOnError(throwable -> log.info("Error load configuration. Cause : {}", throwable.getMessage()))
                .subscribe();
    }

    @Scheduled(initialDelay = 60000, fixedDelay = 60000)
    public void checkNewChannel() {
        configConnectRepository.findAll()
                .filter(ConnectConfig::getIsEnable)
                .filter(connectConfig -> !keyService.getActiveTable().contains(connectConfig.getTable()))
                .flatMap(connectConfig -> connectMapService.connectToChannel(new ConnectDto(null, connectConfig.getTopic(), connectConfig.getTable())))
                .collectList()
                .doOnSuccess(s -> log.info("List consumers has been updated. {} consumers is working", s.size()))
                .doOnError(throwable -> log.info("Error update list consumers. Cause : {}", throwable.getMessage()))
                .subscribe();
    }
}
