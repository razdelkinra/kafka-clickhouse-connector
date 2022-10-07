package com.project.clickhousekafkaconnector.service.clickhouse;

import com.project.clickhousekafkaconnector.service.redis.KeyService;
import com.project.clickhousekafkaconnector.service.redis.RedisService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Service
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Component
@RequiredArgsConstructor
@Slf4j
public class ClickHouseDataLoader {

    ClickHouseService clickHouseService;
    RedisService redisService;
    KeyService keyService;

    @Scheduled(initialDelay = 20000, fixedRate = 10000)
    public void load() {
        loadDataFromRedisToClickHouse()
                .doOnError(throwable -> log.error("Error save data to Clickhouse with cause {}", throwable.getMessage()))
                .subscribe();
    }

    public Flux<Long> loadDataFromRedisToClickHouse() {
        return redisService.getKeys()
                .subscribeOn(Schedulers.parallel())
                .flatMap(key -> redisService.findAll(key)
                        .buffer()
                        .flatMap(records -> clickHouseService.save(records, keyService.getTable(key)))
                        .doOnNext(count -> log.info("Records has been saved to ClickHouse from topic {} to table {} count = {}", key, keyService.getTable(key), count))
                        .flatMap(integer -> redisService.updateKey(key).flatMap(result -> redisService.delete(key)))
                        .doOnError(throwable -> log.error("Error save data to Clickhouse with cause {}", throwable.getMessage()))
                )
                .onErrorReturn(0L);
    }

}
