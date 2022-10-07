package com.project.clickhousekafkaconnector.service;

import com.project.clickhousekafkaconnector.dto.ConnectDto;
import com.project.clickhousekafkaconnector.entitty.ConnectConfig;
import com.project.clickhousekafkaconnector.repository.ConfigConnectRepository;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@AllArgsConstructor
public class ConfigConnectService {

    ConfigConnectRepository configConnectRepository;

    /***
     * Сохраняет канал в БД с проверкой на существование текущего
     */
    public Mono<ConnectConfig> saveConfig(ConnectDto connect) {
        return isRequiredFieldNotEmpty(connect)
                .flatMapMany(isValid -> configConnectRepository.findAll())
                .any(config -> config.getTable().equals(connect.getTable()) && config.getTopic().equals(connect.getTopic()))
                .flatMap(isExist -> isExist ?
                        Mono.error(new Exception("Consumer for topic" + connect.getTopic() + "already is started")) :
                        configConnectRepository.save(new ConnectConfig(null, connect.getTopic(), connect.getTable(), true))
                )
                .doOnError(throwable -> log.error("Error create config with cause {}", throwable.getCause().getMessage()))
                .switchIfEmpty(Mono.just(new ConnectConfig()))
                .doOnSuccess(connectConfig -> log.info(connectConfig.toString()));
    }

    /***
     * Сохраняет канал в БД как неактивный
     */
    public Mono<ConnectConfig> disconnectConfig(ConnectDto connect) {
        return isRequiredFieldNotEmpty(connect)
                .flatMap(isValid -> configConnectRepository.findById(connect.getId()))
                .map(connectConfig -> {
                    connectConfig.setIsEnable(false);
                    return connectConfig;
                })
                .flatMap(configConnectRepository::save);
    }

    private Mono<Boolean> isRequiredFieldNotEmpty(ConnectDto connect) {
        return StringUtils.isAnyBlank(connect.getTable(), connect.getTopic()) ?
                Mono.error(new Exception("topic and table field should be not null")) : Mono.just(true);
    }
}
